-- Query for performing migration #3
-- This creates the fetch_reminders function
--
-- fmt.Sprintf arguments:
-- 1. Name of the "fetch_reminders" function
-- 2. Name of the "reminders" table
-- 3. Name of the "hosts" table
-- 4. Name of the "hosts_actor_types" table
-- 5. Name of the "actors" table

CREATE FUNCTION %[1]s(
  fetch_ahead_interval interval,
  lease_duration interval,
  actor_hosts uuid[],
  actor_types text[],
  health_check_interval interval,
  max_reminders integer
)
RETURNS SETOF uuid
AS $func$

DECLARE
  r RECORD;
  a_host_id uuid;
BEGIN
  -- Create a temporary table for storing capacity information
  -- We will need to reference this data in more than one place, and also update it
  CREATE TEMPORARY TABLE temp_capacities (
    host_id UUID NOT NULL,
    actor_type TEXT NOT NULL,
    capacity INTEGER NOT NULL
  ) ON COMMIT DROP;
  CREATE INDEX ON temp_capacities (host_id);
  CREATE INDEX ON temp_capacities (actor_type);

  -- Create another temporary table for the hosts that need to be created
  CREATE TEMPORARY TABLE temp_allocate_actors (
    reminder_id uuid NOT NULL,
    actor_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    reminder_delay integer NOT NULL,
    host_id uuid
  ) ON COMMIT DROP;

  -- Start by loading the initial capacity based on how many reminders are currently being executed
  FOR r IN
    SELECT
      hat.host_id,
      hat.actor_type,
      (
        SELECT COUNT(rr.reminder_id)
        FROM %[2]s AS rr
        LEFT JOIN %[5]s
          USING (actor_id, actor_type)
        WHERE
          %[5]s.host_id = hat.host_id
          AND rr.actor_type = hat.actor_type
          AND rr.reminder_lease_time >= CURRENT_TIMESTAMP - lease_duration
      ) AS count,
      hat.actor_concurrent_reminders AS max
    FROM %[4]s AS hat
    WHERE 
      hat.host_id = ANY(actor_hosts)
  LOOP
    IF (r.max <= 0 OR r.count < r.max) THEN
      INSERT INTO temp_capacities VALUES (
        r.host_id,
        r.actor_type,
        CASE WHEN r.max <= 0 THEN 2147483647 ELSE r.max - r.count END
      );
    END IF;
  END LOOP;

  -- Load all upcoming reminders for all actors that are active on hosts in the capacities table (all of which have some capacity)
  -- This also loads reminders for actors that are not active, but which can be executed on hosts currently connected
  FOR r IN
    SELECT
      rr.reminder_id,
      rr.actor_type, rr.actor_id, %[5]s.host_id,
      EXTRACT(EPOCH FROM rr.reminder_execution_time - CURRENT_TIMESTAMP)::int AS reminder_delay,
      row_number() OVER (
        PARTITION BY %[5]s.host_id, rr.actor_type ORDER BY rr.reminder_execution_time ASC
      ) AS row_number,
      capacity
    FROM %[2]s AS rr
    LEFT JOIN %[5]s
      USING (actor_type, actor_id)
    LEFT JOIN %[3]s
      ON %[5]s.host_id = %[3]s.host_id AND %[3]s.host_last_healthcheck >= CURRENT_TIMESTAMP - health_check_interval
    LEFT JOIN temp_capacities
      ON %[3]s.host_id = temp_capacities.host_id AND rr.actor_type = temp_capacities.actor_type
    WHERE 
      rr.reminder_execution_time < CURRENT_TIMESTAMP + fetch_ahead_interval
      AND (
        rr.reminder_lease_id IS NULL
        OR rr.reminder_lease_time IS NULL
        OR rr.reminder_lease_time < CURRENT_TIMESTAMP - lease_duration
      )
      AND (
        (
            %[3]s.host_id IS NULL
            AND rr.actor_type = ANY(actor_types)
        )
        OR capacity > 0
      )
    ORDER BY reminder_delay ASC
    LIMIT max_reminders
  LOOP
    -- RAISE NOTICE 'record: %', r;
    -- For the reminders that have an active actor, filter based on the capacity
    IF (
      r.host_id IS NOT NULL
      AND r.row_number <= r.capacity
    ) THEN
      -- Decrease the capacity
      UPDATE temp_capacities
        SET capacity = capacity -1 
        WHERE temp_capacities.host_id = r.host_id AND temp_capacities.actor_type = r.actor_type;
      -- RAISE NOTICE 'NOT NULL host_id: %', r;

      -- Return the row
      RETURN NEXT (r.reminder_id);
    ELSIF r.host_id IS NULL THEN
      -- For reminders that don't have an active actor, we need to activate an actor
      -- Because multiple reminders could be waiting on the same un-allocated actor, we first need to collect them
      INSERT INTO temp_allocate_actors
          (reminder_id, actor_type, actor_id, reminder_delay)
        VALUES
          (r.reminder_id, r.actor_type, r.actor_id, r.reminder_delay);

      -- RAISE NOTICE 'NULL host_id: %', r;
    END IF;
  END LOOP;

  -- Now, let's allocate an actor for all reminders who are to be execute on a currently-unallocated actor
  -- We perform a SELECT DISTINCT query here
  FOR r IN
    SELECT DISTINCT t.actor_type, t.actor_id, t.reminder_delay
    FROM temp_allocate_actors AS t
  LOOP
    -- RAISE NOTICE 'Need allocation: %', r;

    -- First, we pick a host that has capacity to execute this reminder
    BEGIN
      SELECT t.host_id INTO a_host_id
        FROM temp_capacities AS t
        WHERE t.actor_type = r.actor_type AND capacity > 0
        ORDER BY random() LIMIT 1;
    EXCEPTION
      WHEN no_data_found THEN
        -- If we're here, there was no host with capacity
        CONTINUE;
    END;

    -- Update the temp_capacities table
    UPDATE temp_capacities AS t
      SET capacity = capacity - 1
      WHERE t.host_id = a_host_id AND t.actor_type = r.actor_type;

    -- Create the actor now
    -- Here we can do an upsert because we know that, if the row is present, it means the actor was active on a host that is dead but not GC'd yet
    -- We set the activation to the current timestamp + the delay
    INSERT INTO %[5]s
      (actor_type, actor_id, host_id, actor_activation, actor_idle_timeout)
    SELECT 
      r.actor_type, r.actor_id, a_host_id, (CURRENT_TIMESTAMP + GREATEST(r.reminder_delay, 0) * interval '1 second'),
      (
        SELECT hat.actor_idle_timeout
        FROM %[4]s AS hat
        WHERE hat.actor_type = r.actor_type AND hat.host_id = a_host_id
      )
      ON CONFLICT (actor_type, actor_id) DO UPDATE
      SET
        host_id = EXCLUDED.host_id, actor_activation = EXCLUDED.actor_activation, actor_idle_timeout = EXCLUDED.actor_idle_timeout;

    -- Update the temp_allocate_actors table
    -- Note this can update more than one row
    UPDATE temp_allocate_actors AS t
    SET host_id = a_host_id
    WHERE t.actor_type = r.actor_type AND t.actor_id = r.actor_id;
  END LOOP;

  -- Finally, let's return also the reminders for actors that have just been allocated
  -- We need to filter host_id NULL values because some actors may not have been allocated sucessfully
  RETURN QUERY
    SELECT t.reminder_id
    FROM temp_allocate_actors AS t
    WHERE t.host_id IS NOT NULL;

  RETURN;
END;
$func$ LANGUAGE plpgsql;
