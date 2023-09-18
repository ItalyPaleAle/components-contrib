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
// Arguents are:
// - Name of the "hosts" table
// - Name of the "hosts_actor_types" table
// - Name of the "actors" table
const migration1Query = `CREATE TABLE %[1]s (
  host_id uuid PRIMARY KEY NOT NULL,
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
