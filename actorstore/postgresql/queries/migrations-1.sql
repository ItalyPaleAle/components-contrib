-- Query for performing migration #1
-- This creates the tables for actor state
--
-- fmt.Sprintf arguments:
-- 1. Name of the "hosts" table
-- 2. Name of the "hosts_actor_types" table
-- 3. Name of the "actors" table

CREATE TABLE %[1]s (
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
  actor_concurrent_reminders integer NOT NULL DEFAULT 0,
  PRIMARY KEY (host_id, actor_type),
  FOREIGN KEY (host_id) REFERENCES %[1]s (host_id) ON DELETE CASCADE
);

CREATE INDEX ON %[2]s (actor_type);

CREATE TABLE %[3]s (
  actor_type text NOT NULL,
  actor_id text NOT NULL,
  host_id uuid NOT NULL,
  actor_idle_timeout integer NOT NULL,
  actor_activation timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (actor_type, actor_id),
  FOREIGN KEY (host_id) REFERENCES %[1]s (host_id) ON DELETE CASCADE
);
