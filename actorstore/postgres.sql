CREATE TABLE hosts (
  host_id uuid PRIMARY KEY NOT NULL,
  host_address text NOT NULL,
  host_app_id text NOT NULL,
  host_actors_api_level integer NOT NULL,
  host_last_healthcheck timestamp NOT NULL
);

CREATE UNIQUE INDEX ON hosts (host_address);
CREATE INDEX ON hosts (host_last_healthcheck);

CREATE TABLE hosts_actor_types (
  host_id uuid NOT NULL,
  actor_type text NOT NULL,
  actor_idle_timeout integer NOT NULL,
  PRIMARY KEY (host_id, actor_type),
  FOREIGN KEY (host_id) REFERENCES hosts (host_id) ON DELETE CASCADE
);

CREATE INDEX ON hosts_actor_types (actor_type);

CREATE TABLE actors (
  actor_type text NOT NULL,
  actor_id text NOT NULL,
  host_id uuid NOT NULL,
  host_app_id text NOT NULL,
  host_address text NOT NULL,
  actor_idle_timeout integer NOT NULL,
  actor_activation timestamp NOT NULL,
  PRIMARY KEY (actor_type, actor_id),
  FOREIGN KEY (host_id) REFERENCES hosts (host_id) ON DELETE CASCADE
);


---

-- https://stackoverflow.com/a/72033548/192024
-- Note that in case of 2 requests at the same time when the row doesn't exist, this may fail with a race condition
-- You will get a unique constraint violation. The query can be retried in that case.

WITH new_row AS (
  INSERT INTO actors (actor_type, actor_id, host_id, actor_idle_timeout, actor_activation)
    SELECT 'type1', 'myactor1', 'c58c342c-42eb-474c-b7a1-0f3cc59a2320', 100, CURRENT_TIMESTAMP
      WHERE NOT EXISTS (
        SELECT * FROM actors WHERE actor_type = 'type1' AND actor_id = 'myactor1'
      )
    RETURNING *
)
(
  SELECT actors.host_id, actors.actor_idle_timeout, actors.actor_activation FROM actors WHERE actor_type = 'type1' AND actor_id = 'myactor1'
  UNION ALL
  SELECT * FROM new_row
) LIMIT 1;

---

WITH new_row AS (
  INSERT INTO actors (actor_type, actor_id, host_id, host_app_id, host_address, actor_idle_timeout, actor_activation)
    SELECT 'type2', 'myactor4', hosts_actor_types.host_id, hosts.host_app_id, hosts.host_address, hosts_actor_types.actor_idle_timeout, CURRENT_TIMESTAMP
      FROM hosts_actor_types, hosts
      WHERE
        hosts_actor_types.actor_type = 'type2' AND
        hosts.host_id = hosts_actor_types.host_id AND
        NOT EXISTS (
          SELECT * FROM actors WHERE actor_type = 'type2' AND actor_id = 'myactor4'
        )
      ORDER BY random() LIMIT 1
    RETURNING host_app_id, host_address, actor_idle_timeout
)
(
  SELECT host_app_id, host_address, actor_idle_timeout
    FROM actors
    WHERE actor_type = 'type2' AND actor_id = 'myactor4'
  UNION ALL
  SELECT * FROM new_row
) LIMIT 1;


---

PREPARE createactor (text, text) AS

    WITH new_row AS (
      INSERT INTO actors (actor_type, actor_id, host_id, host_app_id, host_address, actor_idle_timeout, actor_activation)
        SELECT $1, $2, hosts_actor_types.host_id, hosts.host_app_id, hosts.host_address, hosts_actor_types.actor_idle_timeout, CURRENT_TIMESTAMP
          FROM hosts_actor_types, hosts
          WHERE
            hosts_actor_types.actor_type = $1 AND
            hosts.host_id = hosts_actor_types.host_id AND
            NOT EXISTS (
              SELECT * FROM actors WHERE actor_type = $1 AND actor_id = $2
            )
          ORDER BY random() LIMIT 1
        RETURNING host_app_id, host_address, actor_idle_timeout
    )
    (
      SELECT host_app_id, host_address, actor_idle_timeout
        FROM actors
        WHERE actor_type = $1 AND actor_id = $2
      UNION ALL
      SELECT * FROM new_row
    ) LIMIT 1;


EXECUTE createactor('type1', 'actor1.1');