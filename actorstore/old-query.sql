INSERT INTO hosts
	(host_address, host_app_id, host_actors_api_level, host_last_healthcheck)
VALUES
	($1, $2, $3, CURRENT_TIMESTAMP)
ON CONFLICT (host_address) DO UPDATE
    SET host_actors_api_level = EXCLUDED.host_actors_api_level,
        host_last_healthcheck = CURRENT_TIMESTAMP
    WHERE
        (hosts.host_address = EXCLUDED.host_address AND hosts.host_app_id = EXCLUDED.host_app_id)
        OR hosts.host_last_healthcheck < CURRENT_TIMESTAMP - $4
RETURNING host_id;
