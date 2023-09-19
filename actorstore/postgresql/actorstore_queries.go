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
  host_last_healthcheck timestamp NOT NULL
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
  host_app_id text NOT NULL,
  host_address text NOT NULL,
  actor_idle_timeout integer NOT NULL,
  actor_activation timestamp NOT NULL,
  PRIMARY KEY (actor_type, actor_id),
  FOREIGN KEY (host_id) REFERENCES %[1]s (host_id) ON DELETE CASCADE
);`

// Query for looking up an actor, or creating it ex novo.
//
// Note that in case of 2 requests at the same time when the row doesn't exist, this may fail with a race condition
// You will get a unique constraint violation. The query can be retried in that case.
//
// Query arguments:
// 1. Actor type
// 2. Actor ID
//
// fmt.Sprintf arguments:
// 1. Name of the "hosts" table
// 2. Name of the "hosts_actor_types" table
// 3. Name of the "actors" table
//
// Inspired by: https://stackoverflow.com/a/72033548/192024
const lookupActorQuery = `WITH new_row AS (
  INSERT INTO %[3]s (actor_type, actor_id, host_id, host_app_id, host_address, actor_idle_timeout, actor_activation)
    SELECT $1, $2, %[2]s.host_id, %[3]s.host_app_id, %[3]s.host_address, %[2]s.actor_idle_timeout, CURRENT_TIMESTAMP
      FROM %[2]s, %[3]s
      WHERE
        %[2]s.actor_type = $1 AND
        %[3]s.host_id = %[2]s.host_id AND
        NOT EXISTS (
          SELECT * FROM %[3]s WHERE actor_type = $1 AND actor_id = $2
        )
      ORDER BY random() LIMIT 1
    RETURNING host_app_id, host_address, actor_idle_timeout
)
(
  SELECT host_app_id, host_address, actor_idle_timeout
    FROM %[3]s
    WHERE actor_type = $1 AND actor_id = $2
  UNION ALL
  SELECT * FROM new_row
) LIMIT 1;`
