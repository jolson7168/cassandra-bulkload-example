# cassandra-bulkload

SSTable generating and bulk loading code modeled on DataStax [Using Cassandra Bulk Loader, Updated](http://www.datastax.com/dev/blog/using-the-cassandra-bulk-loader-updated) blog post.

Modified to load bulk netflow data.

## Generating SSTables

Setup:
    
Modify 'args' in build.gradle to point to a text file listing the .bin files you wish to load.

Sample file ./data/allFiles.txt included as an example.

Run:

    $ ./gradlew run

This will generate SSTable(s) under the `data` directory.

## Bulk loading

First, create schema using `schema.cql` file:

    $ cqlsh -u <user> -p <password> -f schema.cql

Then, load SSTables to Cassandra using `sstableloader`:

    $ sstableloader -u <user> -pw <password> -d <ip address of the node> ./data/netflow/localIP

    Established connection to initial hosts
    Opening sstables and calculating sections to stream
    Streaming relevant part of /home/ubuntu/cassandra-bulkload-example/data/netflow/localIP/netflow-localip-ka-1-Data.db to [localhost/127.0.0.1]
    progress: [localhost/127.0.0.1]0:1/1 100% total: 100% 2.619KiB/s (avg: 2.619KiB/s)
    progress: [localhost/127.0.0.1]0:1/1 100% total: 100% 0.000KiB/s (avg: 2.479KiB/s)

    Summary statistics: 
       Connections per host    : 1         
       Total files transferred : 1         
       Total bytes transferred : 10.869KiB 
       Total duration          : 4385 ms   
       Average transfer rate   : 2.478KiB/s
       Peak transfer rate      : 2.619KiB/s


(assuming you have `cqlsh` and `sstableloader` in your `$PATH`)

## Check loaded data


    $ bin/cqlsh
    Connected to Test Cluster at 127.0.0.1:9042.
    [cqlsh 5.0.1 | Cassandra 2.1.0 | CQL spec 3.2.0 | Native protocol v3]
    Use HELP for help.
    cqlsh> USE netflow ;
    cqlsh:quote> SELECT * FROM localIP WHERE localIP = '10.1.106.65' LIMIT 3;

     local_ip    | start_time | end_reason | end_time   | num_bytes | num_packets | port | protocol | remote_ip   | time_index
    -------------+------------+------------+------------+-----------+-------------+------+----------+-------------+------------
     10.1.106.65 | 1473840840 |          5 | 1475852089 |       770 |          13 | 3544 |       17 | 65.4.81.187 |          0
     10.1.106.65 | 1473840841 |          5 | 1475587549 |       343 |           7 | 3544 |       17 | 65.4.81.187 |          0
     10.1.106.65 | 1473840842 |          5 | 1476390621 |       639 |          11 | 3544 |       17 | 89.82.140.1 |          0

    (3 rows)

Voilà!

## Know issues:

1. Cassandra bulk import does not support all Cassandra data types, including inet, smallint, and tinyint. Will verify with the mailing list. 

## To do:

 0. Lock down the clustering / partition keys!!
 1. Lock down input set
 2. Lock down data types
    a. inet / ascii for the ip addresses
    b. timestamp / bigint for the timestamps
    c. since smallint, tinyint don't seem to be able to be loaded with the bulk import, keep these as ints, or update after load??
 3. Improve logging
 4. Do some benchmarking
 
 
