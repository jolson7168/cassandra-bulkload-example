# cassandra-bulkload

SSTable generating and bulk loading code modeled on DataStax [Using Cassandra Bulk Loader, Updated](http://www.datastax.com/dev/blog/using-the-cassandra-bulk-loader-updated) blog post.
Modified to load bulk netflow data

## Generating SSTables

Setup:
    
    Modify 'args' in build.gradle to point to a text file listing the .bin files you wish to load.
    Sample file ./data/allFiles.txt included as an exmple.

Run:

    $ ./gradlew run

This will generate SSTable(s) under `data` directory.

## Bulk loading

First, create schema using `schema.cql` file:

    $ cqlsh -f schema.cql

Then, load SSTables to Cassandra using `sstableloader`:

    $ sstableloader -d <ip address of the node> data/netflow/localIP

(assuming you have `cqlsh` and `sstableloader` in your `$PATH`)

## Check loaded data


    $ bin/cqlsh
    Connected to Test Cluster at 127.0.0.1:9042.
    [cqlsh 5.0.1 | Cassandra 2.1.0 | CQL spec 3.2.0 | Native protocol v3]
    Use HELP for help.
    cqlsh> USE netflow ;
    cqlsh:quote> SELECT * FROM localIP WHERE localIP = '' LIMIT 3;



    (3 rows)

Voilà!

## Know issues:

1. Cassandra bulk import does not support all Cassandra data types, including inet, smallint, and tinyint. Will verify with the mailing list. 

## To do:

 1. Lock down input set
 2. Lock down data types
    a. inet / ascii for the ip addresses
    b. timestamp / bigint for the timestamps
    c. since smallint, tinyint don't seem to be able to be loaded with the bulk import, keep these as ints, or update after load??
 3. Improve logging
 4. Do some benchmarking
 
 
