CREATE KEYSPACE IF NOT EXISTS netflow WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE netflow;

CREATE TABLE IF NOT EXISTS connections (
	local_ip	int,
	local_port	smallint,
	remote_ip	int,
    connection_id int,
	primary key (local_ip, remote_ip, local_port)
);

CREATE TABLE IF NOT EXISTS netflow (
    connection_id   int,
	time_index      int,
	num_packets	    bigint,
	num_bytes	    bigint,
	start_time	    bigint,
	end_time	    bigint,
	protocol	    int,
	end_reason	    int,
	primary key (connection_id, time_index)
);
