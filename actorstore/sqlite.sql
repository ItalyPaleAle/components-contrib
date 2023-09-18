CREATE TABLE hosts (
  host_id INTEGER PRIMARY KEY,
  host_address TEXT NOT NULL,
  host_app_id TEXT NOT NULL,
  host_actors_api_level INTEGER NOT NULL,
  host_last_healthcheck INTEGER NOT NULL
);

CREATE UNIQUE INDEX hosts_host_address_idx ON hosts (host_address);
CREATE INDEX hosts_host_last_healthcheck_idx ON hosts (host_last_healthcheck);

CREATE TABLE hosts_actor_types (
  host_id INTEGER NOT NULL,
  actor_type TEXT NOT NULL,
  actor_idle_timeout INTEGER NOT NULL,
  PRIMARY KEY (host_id, actor_type),
  FOREIGN KEY (host_id) REFERENCES hosts (host_id) ON DELETE CASCADE
);

CREATE INDEX actor_type_hosts_actor_types_idx ON hosts_actor_types (actor_type);

CREATE TABLE actors (
  actor_type TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  host_id INTEGER NOT NULL,
  host_app_id text NOT NULL,
  host_address text NOT NULL,
  actor_idle_timeout INTEGER NOT NULL,
  actor_activation timestamp NOT NULL,
  PRIMARY KEY (actor_type, actor_id),
  FOREIGN KEY (host_id) REFERENCES hosts (host_id) ON DELETE CASCADE
);

CREATE INDEX actors_host_id_idx ON actors (host_id);
