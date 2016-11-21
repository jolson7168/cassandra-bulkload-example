/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bulkload;

//import java.io.BufferedReader;
//import java.io.File;
//import java.io.Path;
//import java.io.Files;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.io.InputStream;
import java.io.*;
import java.nio*;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.nio.ByteBuffer;
//import java.nio.ByteOrder;
//import java.nio.channels.FileChannel;



import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;


    

/**
 * Usage: java bulkload.BulkLoad
 */
public class BulkLoad
{

    /** Default output directory */
    public static final String DEFAULT_OUTPUT_DIR = "./data";

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    /** Keyspace name */
    public static final String KEYSPACE = "netflow";
    /** Table name */
    public static final String CONNECTIONSTABLE = "connections";
    public static final String NETFLOWTABLE = "netflow";

    /**
     * Schema for bulk loading table.
     * It is important not to forget adding keyspace name before table name,
     * otherwise CQLSSTableWriter throws exception.
     */
    public static final String NETFLOWSCHEMA = String.format("CREATE TABLE %s.%s ("+
                                                          "connection_id    int, " +
                                                          "time_index       int, " +
                                                          "num_packets	    bigint, " +
                                                          "num_bytes	    bigint, " +
                                                          "start_time	    bigint, " +
                                                          "end_time	        bigint, " +
                                                          "protocol	        int, " +
                                                          "end_reason	    int, " +
                                                          "PRIMARY KEY (connection_id, time_index) )" , KEYSPACE, NETFLOWTABLE);


    public static final String CONNECTIONSCHEMA = String.format("CREATE TABLE %s.%s (" +
	                                                        "local_ip	bigint," +
	                                                        "local_port	int, "+
	                                                        "remote_ip	bigint," +
                                                            "connection_id bigint," +
	                                                        "primary key ((local_ip, remote_ip, local_port)) )", KEYSPACE, CONNECTIONSTABLE);




    /**
     * INSERT statement to bulk load.
     * It is like prepared statement. You fill in place holder for each data.
     */
    public static final String INSERT_NETFLOW = String.format("INSERT INTO %s.%s (" +
                                                           "connection_id,time_index,num_packets,num_bytes,start_time,end_time,protocol,end_reason" +
                                                           ") VALUES (" +
                                                               "?, ?, ?, ?, ?, ?, ?, ?" +
                                                           ")", KEYSPACE, NETFLOWTABLE);

    public static final String INSERT_CONNECTION = String.format("INSERT INTO %s.%s (" +
                                                            "local_ip, local_port, remote_ip, connection_id" +
                                                           ") VALUES (" +
                                                               "?, ?, ?, ?" +
                                                           ")", KEYSPACE, CONNECTIONSTABLE);


    private static List<String> readFile(String fileName) {

		List<String> list = new ArrayList<>();

		try (Stream<String> stream = Files.lines(Paths.get(fileName))) {

			list = stream.map(String::toLowerCase).collect(Collectors.toList());

		} catch (IOException e) {
			e.printStackTrace();
		}

        return list;
    }


    private static String toNtoa(long raw) {
        byte[] b = new byte[] {(byte)(raw >> 24), (byte)(raw >> 16), (byte)(raw >> 8), (byte)raw};
        try {
            return InetAddress.getByAddress(b).getHostAddress();
        } catch (UnknownHostException e) {
            //No way here
            return null;
        }
    } 


    private static void readFromNIONetflow(String fileName, CQLSSTableWriter writer) throws IOException{
      System.out.format("Starting netflow file: %s\n", fileName); 
      FileInputStream fis = new FileInputStream(new File(fileName));
      FileChannel channel = fis.getChannel();
      ByteBuffer bb = ByteBuffer.allocateDirect(37);
      bb.order(ByteOrder.LITTLE_ENDIAN);
      bb.clear();
      long len = 0;
      int counter = 0;
      while ((len = channel.read(bb))!= -1){
        bb.flip();
        long numPackets = bb.getInt() & 0xffffffffL; 
        long numBytes = bb.getInt() & 0xffffffffL; 
        //No overflow here for test data. TODO: How to convert C 64bit unsigned to Java?
        long startTime = bb.getLong();
        if (startTime < 2000000000) {
            startTime = startTime * 1000;
        }
        //No overflow here for test data. TODO: How to convert C 64bit unsigned to Java?
        long endTime = bb.getLong();               
        if (endTime < 2000000000) {
            endTime = endTime * 1000;
        }
        String localIp = toNtoa(bb.getInt() & 0xffffffffL); 
        String remoteIp = toNtoa(bb.getInt() & 0xffffffffL); 
        int port = bb.getShort() & 0xffff;
        int protocol = bb.getShort() & 0xffff;
        int dirAndReason= bb.get();

        // Send to Cassandra
        // We use Java types here based on
        // http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29

        // TODO: fix this...
        int timeIndex = 0;

        try {
            System.out.format("%s, %s, %d, %d, %d, %d, %d, %d, %d, %d \n", localIp, remoteIp, port, timeIndex, numPackets, numBytes, startTime, endTime, protocol, dirAndReason); 
            writer.addRow(localIp, remoteIp, port, timeIndex, numPackets, numBytes, startTime, endTime, protocol, dirAndReason);
            counter = counter + 1;
        }
        catch (InvalidRequestException e)
        {
            e.printStackTrace();
        }

        bb.clear();
        if ((counter % 10000) == 0) {
            System.out.format("   Processed: %d records\n", counter); 
        }
        
      }
      channel.close();
      fis.close();
      System.out.format("Done! %d records read and written \n", counter); 
    }


    private static void readFromNIOPartition(String fileName, CQLSSTableWriter writer) throws IOException{
      System.out.format("Starting connection file: %s\n", fileName); 
      FileInputStream fis = new FileInputStream(new File(fileName));
      FileChannel channel = fis.getChannel();
      ByteBuffer bb = ByteBuffer.allocateDirect(14);
      bb.order(ByteOrder.LITTLE_ENDIAN);
      bb.clear();
      long len = 0;
      int counter = 0;


      Path file = Paths.get("./logs/log2.txt");


      while ((len = channel.read(bb))!= -1){
        bb.flip();
        long localIp = bb.getInt() & 0xffffffffL; 
        long remoteIp = bb.getInt() & 0xffffffffL; 
        int port = bb.getShort() & 0xffff;
        long connectionId = bb.getInt() & 0xffffffffL; 

        // Send to Cassandra
        // We use Java types here based on
        // http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29


        try {
            String theLine = String.format("%d,%d,%d,%d\n", localIp, remoteIp, port, connectionId);
            Files.write(file, theLine, Charset.forName("UTF-8"), StandardOpenOption.APPEND);
            writer.addRow(localIp, port, remoteIp, connectionId);
            counter = counter + 1;
        }
        catch (InvalidRequestException e)
        {
            e.printStackTrace();
        }

        bb.clear();
        if ((counter % 10000) == 0) {
            System.out.format("   Processed: %d records\n", counter); 
        }
        
      }
      channel.close();
      fis.close();
      System.out.format("Done! %d records read and written \n", counter); 
    }

    public static void main(String[] args)
    {
        if (args.length == 0)
        {
            System.out.println("usage: java bulkload.BulkLoad <file containing .bin path + file names>");
            return;
        }

        // magic!
        Config.setClientMode(true);

        // Create output directory that has keyspace and table name in the path
        File outputDirNetflow = new File(DEFAULT_OUTPUT_DIR + File.separator + KEYSPACE + File.separator + NETFLOWTABLE);
        File outputDirConnections = new File(DEFAULT_OUTPUT_DIR + File.separator + KEYSPACE + File.separator + CONNECTIONSTABLE);
        if (!outputDirNetflow.exists() && !outputDirNetflow.mkdirs())
        {
            throw new RuntimeException("Cannot create netflow output directory: " + outputDirNetflow);
        }
        if (!outputDirConnections.exists() && !outputDirConnections.mkdirs())
        {
            throw new RuntimeException("Cannot create connections output directory: " + outputDirConnections);
        }

        // Prepare NETFLOW SSTable writer
        CQLSSTableWriter.Builder netflow_builder = CQLSSTableWriter.builder();
        // set output directory
        netflow_builder.inDirectory(outputDirNetflow)
               // set target schema
               .forTable(NETFLOWSCHEMA)
               // set CQL statement to put data
               .using(INSERT_NETFLOW)
               // set partitioner if needed
               // default is Murmur3Partitioner so set if you use different one.
               .withPartitioner(new Murmur3Partitioner());
        CQLSSTableWriter netflow_writer = netflow_builder.build();

        // Prepare CONNECTIONS SSTable writer
        CQLSSTableWriter.Builder connections_builder = CQLSSTableWriter.builder();
        // set output directory
        connections_builder.inDirectory(outputDirConnections)
               // set target schema
               .forTable(CONNECTIONSCHEMA)
               // set CQL statement to put data
               .using(INSERT_CONNECTION)
               // set partitioner if needed
               // default is Murmur3Partitioner so set if you use different one.
               .withPartitioner(new Murmur3Partitioner());
        CQLSSTableWriter connections_writer = connections_builder.build();





        List<String> fileArray =  new ArrayList<String>();

        for (String fileName: args) {
            fileArray.addAll(readFile(fileName));
        }

        for (String fileName: fileArray) {
            try {
                if (fileName.contains("partition")) {
                    readFromNIOPartition(fileName, connections_writer);
                }
                else if (fileName.contains("connections")) {
                    readFromNIONetflow(fileName, netflow_writer);
                }

            } catch (IOException e) {
			    e.printStackTrace();
		    }
   
        }

        try
        {
            connections_writer.close();
            netflow_writer.close();
        }
        catch (IOException ignore) {}
    }
}
