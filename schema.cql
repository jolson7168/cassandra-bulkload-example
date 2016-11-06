CREATE KEYSPACE IF NOT EXISTS netflow WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE netflow;
CREATE TABLE IF NOT EXISTS localIP (
	local_ip	ascii,
	remote_ip	ascii,
//	local_port	int,
//	remote_port int,
    port        int,
	time_index  int,
	num_packets	bigint,
	num_bytes	bigint,
	start_time	bigint,
	end_time	bigint,
	protocol	int,
	end_reason	int,
	primary key (local_ip, start_time)
);
