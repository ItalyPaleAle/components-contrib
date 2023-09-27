/*
Copyright 2023 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package postgresql

// Query for performing migration #1
//
// fmt.Sprintf arguments:
// 1. Name of the "hosts" table
// 2. Name of the "hosts_actor_types" table
// 3. Name of the "actors" table
const migration1Query = `CREATE TABLE %[1]s (
  host_id uuid PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
  host_address text NOT NULL,
  host_app_id text NOT NULL,
  host_actors_api_level integer NOT NULL,
  host_last_healthcheck timestamp with time zone NOT NULL
);

CREATE UNIQUE INDEX ON %[1]s (host_address);
CREATE INDEX ON %[1]s (host_last_healthcheck);

CREATE TABLE %[2]s (
  host_id uuid NOT NULL,
  actor_type text NOT NULL,
  actor_idle_timeout integer NOT NULL,
  PRIMARY KEY (host_id, actor_type),
  FOREIGN KEY (host_id) REFERENCES %[1]s (host_id) ON DELETE CASCADE
);

CREATE INDEX ON %[2]s (actor_type);

CREATE TABLE %[3]s (
  actor_type text NOT NULL,
  actor_id text NOT NULL,
  host_id uuid NOT NULL,
  actor_idle_timeout integer NOT NULL,
  actor_activation timestamp with time zone NOT NULL,
  PRIMARY KEY (actor_type, actor_id),
  FOREIGN KEY (host_id) REFERENCES %[1]s (host_id) ON DELETE CASCADE
);`

// Query for performing migration #2
//
// fmt.Sprintf arguments:
// 1. Name of the "reminders" table
const migration2Query = `CREATE TABLE %[1]s (
  reminder_id uuid PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(), 
  actor_type text NOT NULL,
  actor_id text NOT NULL,
  reminder_name text NOT NULL,
  reminder_execution_time timestamp with time zone NOT NULL,
  reminder_period text,
  reminder_ttl timestamp,
  reminder_data bytea,
  reminder_lease_time timestamp with time zone,
  reminder_lease_pid text
);

CREATE UNIQUE INDEX ON %[1]s (actor_type, actor_id, reminder_name);
CREATE INDEX ON %[1]s (reminder_execution_time);
CREATE INDEX ON %[1]s (reminder_lease_time);
`

// Query for looking up an actor, or creating it ex novo.
//
// The purpose of this query is to perform an atomic "load or set". Given an actor ID and type, it will:
// - If there's already a row in the table with the same actor ID and type, AND the last healthcheck hasn't expired, returns the row
// - If there's no row in the table with the same actor ID and type, OR if there's a row but the last healthcheck has expired, inserts a new row (performing an "upsert" if the row already exists)
// In both cases, the query lookups up the actor host's ID, and then returns the actor host's address and app ID, and the idle timeout configured for the actor type
//
// Note that in case of 2 requests at the same time when the row doesn't exist, this may fail with a race condition.
// You will get a unique constraint violation. The query can be retried in that case.
//
// Query arguments:
// 1. Actor type, as `string`
// 2. Actor ID, as `string`
// 3. Health check interval, as `time.Duration`
//
// fmt.Sprintf arguments:
// 1. Name of the "hosts" table
// 2. Name of the "hosts_actor_types" table
// 3. Name of the "actors" table
//
// Inspired by: https://stackoverflow.com/a/72033548/192024
const lookupActorQuery = `WITH new_row AS (
  INSERT INTO %[3]s (actor_type, actor_id, host_id, actor_idle_timeout, actor_activation)
    SELECT $1, $2, %[2]s.host_id, %[2]s.actor_idle_timeout, CURRENT_TIMESTAMP
      FROM %[2]s, %[1]s
      WHERE
        %[2]s.actor_type = $1
        AND %[1]s.host_id = %[2]s.host_id
        AND %[1]s.host_last_healthcheck >= CURRENT_TIMESTAMP - $3::interval
        AND NOT EXISTS (
          SELECT %[3]s.host_id
            FROM %[3]s, %[1]s
            WHERE
              %[3]s.actor_type = $1
              AND %[3]s.actor_id = $2
              AND %[3]s.host_id = %[1]s.host_id
              AND %[1]s.host_last_healthcheck >= CURRENT_TIMESTAMP - $3::interval
        )
      ORDER BY random() LIMIT 1
    ON CONFLICT (actor_type, actor_id) DO UPDATE
      SET
        host_id = EXCLUDED.host_id, actor_idle_timeout = EXCLUDED.actor_idle_timeout, actor_activation = EXCLUDED.actor_activation
    RETURNING host_id, actor_idle_timeout
)
(
  SELECT %[1]s.host_app_id, %[1]s.host_address, %[3]s.actor_idle_timeout
    FROM %[3]s, %[1]s
    WHERE
      %[3]s.actor_type = $1
      AND %[3]s.actor_id = $2
      AND %[3]s.host_id = %[1]s.host_id
      AND %[1]s.host_last_healthcheck >= CURRENT_TIMESTAMP - $3::interval
  UNION ALL
  SELECT %[1]s.host_app_id, %[1]s.host_address, new_row.actor_idle_timeout
    FROM new_row, %[1]s
    WHERE
      new_row.host_id = %[1]s.host_id
      AND %[1]s.host_last_healthcheck >= CURRENT_TIMESTAMP - $3::interval
) LIMIT 1;`

// Query for fetching the upcoming reminders.
//
// This query retrieves a batch of upcoming reminders, and at the same time it updates the retrieved rows to "lock" them.
// Reminders are retrieved if they are to be executed within the next "fetch ahead" interval and if they don't already have a lease.
// This only fetches reminders for actors that are either not active on any host, or active on the list of hosts that have a connection with the current instance of the actors service.
//
// Query arguments:
// 1. Fetch ahead interval, as a `time.Duration`
// 2. Lease duration, as a `time.Duration`
// 3. IDs of actor hosts that have an active connection to the current instance of the actors service, as a `string[]`
// 4. Maximum batch size, as `int`
//
// fmt.Sprintf arguments:
// 1. Name of the "reminders" table
// 2. Name of the "actors" table
const remindersFetchQuery = `UPDATE %[1]s
SET reminder_lease_time = CURRENT_TIMESTAMP
WHERE reminder_id IN (
    SELECT reminder_id
    FROM %[1]s
    LEFT JOIN %[2]s
        ON %[2]s.actor_type = %[1]s.actor_type AND %[2]s.actor_id = %[1]s.actor_id
    WHERE 
        %[1]s.reminder_execution_time < CURRENT_TIMESTAMP + $1::interval
        AND (
            %[1]s.reminder_lease_time IS NULL
            OR %[1]s.reminder_lease_time < CURRENT_TIMESTAMP - $2::interval
        )
        AND (
            %[2]s.host_id IS NULL
            OR %[2]s.host_id = ANY($3)
        )
    ORDER BY %[1]s.reminder_execution_time ASC
    LIMIT $4
)
RETURNING
    actor_type, actor_id, reminder_name,
    EXTRACT(EPOCH FROM reminder_execution_time - CURRENT_TIMESTAMP)::int,
    reminder_data, reminder_lease_time;`
