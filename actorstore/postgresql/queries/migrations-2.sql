-- Query for performing migration #2
-- This creates tables for reminders
--
-- fmt.Sprintf arguments:
-- 1. Name of the "reminders" table
CREATE TABLE %[1]s (
  reminder_id uuid PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(), 
  actor_type text NOT NULL,
  actor_id text NOT NULL,
  reminder_name text NOT NULL,
  reminder_execution_time timestamp with time zone NOT NULL,
  reminder_period text,
  reminder_ttl timestamp with time zone,
  reminder_data bytea,
  reminder_lease_id uuid,
  reminder_lease_time timestamp with time zone,
  reminder_lease_pid text
);

CREATE UNIQUE INDEX ON %[1]s (actor_type, actor_id, reminder_name);
CREATE INDEX ON %[1]s (reminder_execution_time);
CREATE INDEX ON %[1]s (reminder_lease_pid);
