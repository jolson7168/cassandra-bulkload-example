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


import java.io.*;
import java.nio.file.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.commons.io.FileUtils;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Arrays;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rabbitmq.client.*;
    

/**
 * Usage: java bulkload.BulkLoad
 */
public class BulkLoad
{
    private static final Logger logger = LogManager.getLogger("SSTableConverter");


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
                                                          "connection_id    bigint, " +
                                                          "start_time       bigint, " +
                                                          "end_time 	    bigint, " +
                                                          "protocol 	    int, " +
                                                          "dir_reason	    int, " +
                                                          "num_packets      bigint, " +
                                                          "num_bytes	    bigint, " +                                                          
                                                          "PRIMARY KEY (connection_id, start_time))" , KEYSPACE, NETFLOWTABLE);


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
                                                           "connection_id,start_time,end_time,protocol,dir_reason,num_packets,num_bytes" +
                                                           ") VALUES (" +
                                                               "?, ?, ?, ?, ?, ?, ?" +
                                                           ")", KEYSPACE, NETFLOWTABLE);

    public static final String INSERT_CONNECTION = String.format("INSERT INTO %s.%s (" +
                                                            "local_ip, local_port, remote_ip, connection_id" +
                                                           ") VALUES (" +
                                                               "?, ?, ?, ?" +
                                                           ")", KEYSPACE, CONNECTIONSTABLE);


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
      logger.info(String.format("   Starting netflow file: %s", fileName));
      FileInputStream fis = new FileInputStream(new File(fileName));
      FileChannel channel = fis.getChannel();
      ByteBuffer bb = ByteBuffer.allocateDirect(41);
      bb.order(ByteOrder.LITTLE_ENDIAN);
      bb.clear();
      long len = 0;
      int counter = 0;

      try{
          //PrintWriter fileWriter = new PrintWriter(DEFAULT_OUTPUT_DIR + File.separator + getHostname() + File.separator + KEYSPACE + File.separator +"log.txt", "UTF-8");
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
            //String localIp = toNtoa(bb.getInt() & 0xffffffffL); 
            //String remoteIp = toNtoa(bb.getInt() & 0xffffffffL); 
            long sourceIP = bb.getInt() & 0xffffffffL;
            long targetIP = bb.getInt() & 0xffffffffL;
            int port = bb.getShort() & 0xffff;
            int protocol = bb.getShort() & 0xffff;
            int dirAndReason= bb.get();
            long connectionId = bb.getInt() & 0xffffffffL; 

            // Send to Cassandra
            // We use Java types here based on
            // http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29

            try {
                //fileWriter.println(String.format("%d,%d,%d,%d,%d,%d,%d,%d", counter, connectionId, startTime, endTime, protocol, dirAndReason, numPackets, numBytes)); 
                writer.addRow(connectionId, startTime, endTime, protocol, dirAndReason, numPackets, numBytes);
                counter = counter + 1;
            }
            catch (InvalidRequestException e)
            {
                e.printStackTrace();
            }

            bb.clear();
            if ((counter % 1000000) == 0) {
                logger.info(String.format("      Processed: %d records", counter));
            }
        }
        channel.close();
        fis.close();
        //fileWriter.close();
        logger.info(String.format("   Done! %d records read and written ", counter));
    }
    catch (IOException e) {

    }
}

    private static void readFromNIOPartition(String fileName, CQLSSTableWriter writer) throws IOException{
      logger.info(String.format("   Starting connection file: %s", fileName));
      FileInputStream fis = new FileInputStream(new File(fileName));
      FileChannel channel = fis.getChannel();
      ByteBuffer bb = ByteBuffer.allocateDirect(14);
      bb.order(ByteOrder.LITTLE_ENDIAN);
      bb.clear();
      long len = 0;
      int counter = 0;


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
            String theLine = String.format("%d,%d,%d,%d", localIp, remoteIp, port, connectionId);
            List<String> lines = Arrays.asList(theLine);            
            writer.addRow(localIp, port, remoteIp, connectionId);
            counter = counter + 1;
        }
        catch (InvalidRequestException e)
        {
            e.printStackTrace();
        }

        bb.clear();
        if ((counter % 1000000) == 0) {
            logger.info(String.format("      Processed: %d records", counter));
        }
        
      }
      channel.close();
      fis.close();
      logger.info(String.format("   Done! %d records read and written", counter));
    }

    private static String getHostname() {
        
        String hostname = "Unknown";
        try
        {
                InetAddress addr;
                addr = InetAddress.getLocalHost();
                hostname = addr.getHostName();
        }
        catch (UnknownHostException ex)
        {
        
        }
        return hostname;
        
    }

    private static long getUsableDiskSpace(String root) {

        File file = new File(root);
        return file.getUsableSpace();
    }

    public static void main(String[] args) throws java.io.IOException, java.lang.InterruptedException, java.util.concurrent.TimeoutException
    {

        if (args.length != 9)
        {
            System.out.println("usage: java bulkload.BulkLoad <rabbit mq ip>, <queue name>, <login>, <password>, <instance>, <working root>, <temp root>, <target dir>, <required disk space>");
            return;
        }

        final String instance = args[4];
        final String writeRoot = args[5];
        final String tmpDir = args[6];
        final String targetRoot = args[8];
        final long neededDiskSpace = Long.parseLong(args[7]);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(args[0]);
        factory.setUsername(args[2]);
        factory.setPassword(args[3]);
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.queueDeclare(args[1], true, false, false, null);
        channel.basicQos(1);

        // magic!
        Config.setClientMode(true);
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                long diskSpace = getUsableDiskSpace(writeRoot);
                String message = new String(body, "UTF-8").trim();
                if (diskSpace < neededDiskSpace) {
                    logger.error(String.format("Out of disk space prior to processing file: %s. Exiting!", message));
                    System.exit(0);
                }
                logger.info(String.format("Processing file: %s", message));
                logger.info(String.format("   Usable disk space %d bytes out of %d bytes required", diskSpace, neededDiskSpace ));
                Path source = Paths.get(message);
                String fname = source.getFileName().toString();
                String fnameStripped = source.getFileName().toString().replace("expanded","").replace(".bin","");
                Path destination = Paths.get(tmpDir+File.separator+fname);

                try {
                    logger.info(String.format("   Copying file from %s to %s", source.toString(), destination.toString()));
                    Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);

                    //opt/mnt/1474537440000/netflow/netflow
                    File outputDirNetflow = new File(writeRoot+File.separator+fnameStripped + File.separator + KEYSPACE + File.separator + NETFLOWTABLE);
                    File outputDirConnections = new File(writeRoot+File.separator + "connections" + File.separator + KEYSPACE + File.separator + CONNECTIONSTABLE);

                    try {
                        if (fname.contains("partition")) {
                            if (!outputDirConnections.exists() && !outputDirConnections.mkdirs())
                            {
                                throw new RuntimeException("Cannot create connections output directory: " + outputDirConnections);
                            }
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
                            readFromNIOPartition(destination.toString(), connections_writer);
                            connections_writer.close();
                        }
                        else if (fname.contains("expanded")) {
                            if (!outputDirNetflow.exists() && !outputDirNetflow.mkdirs())
                            {
                                throw new RuntimeException("Cannot create netflow output directory: " + outputDirNetflow);
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
                            readFromNIONetflow(destination.toString(), netflow_writer);
                            netflow_writer.close();
                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    // here
                    logger.info(String.format("  Copying directory %s to %s", writeRoot+File.separator+fnameStripped,targetRoot+File.separator+fnameStripped));
                    File srcDir = new File(writeRoot+File.separator+fnameStripped);
                    File destDir = new File(targetRoot+File.separator+fnameStripped);
                    try {
                        //
                        // Copy source directory into destination directory
                        // including its child directories and files. When
                        // the destination directory is not exists it will
                        // be created. This copy process also preserve the
                        // date information of the file.
                        //
                        FileUtils.copyDirectory(srcDir, destDir);
                        try {
                            logger.info(String.format("  Deleting directory %s ", writeRoot+File.separator+fnameStripped));
                            FileUtils.deleteDirectory(srcDir);
                        }
                        catch (IOException e) {
                            logger.error(String.format("   Could not delete directory %s", writeRoot+File.separator+fnameStripped));
                        }
                    } catch (IOException e) {
                        logger.error(String.format("   Could not copy directory from %s to %s", writeRoot+File.separator+fnameStripped,targetRoot+File.separator+fnameStripped));
                    }
                    try {
                        logger.info(String.format("  Deleting file: %s", destination.toString()));
                        Files.delete(destination);
                    }
                    catch (NoSuchFileException e) {
                        logger.error(String.format("   Could not delete file: %s", destination.toString()));
                    }
                    logger.info(String.format("  Acking queue for file: %s", message));
                    channel.basicAck(envelope.getDeliveryTag(), false);

                }
                catch (NoSuchFileException e) {
                    logger.error(String.format("      File |%s| does not exist", source.toString()));
                }
            }
        };

        channel.basicConsume(args[1], false, consumer);
    }
}
