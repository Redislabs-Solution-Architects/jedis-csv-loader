package com.jsd.jedis;


import com.redis.streams.command.serial.SerialTopicConfig;
import com.redis.streams.command.serial.TopicManager;
import com.redis.streams.command.serial.TopicProducer;
import com.redis.streams.command.serial.SerialTopicConfig.TTLFuzzMode;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Scanner;
import java.util.Map;
import java.util.List;


import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;


public class AppStreams {

    Properties config = new Properties();
    RedisDataLoader redisDataLoader;

    public AppStreams(String configFile) throws Exception {
        this.config.load(new FileInputStream(configFile));
        redisDataLoader = new RedisDataLoader(configFile);
    }


    public void consumeStream(String consumerName) throws Exception {
        JedisPooled jedisPooled = redisDataLoader.getJedisPooled();
        Pipeline pipeline = jedisPooled.pipelined();

        String groupName = config.getProperty("streams.consumer.group.name", "group1");
        String streamName = this.config.getProperty("streams.topic.name", "topic1");
        int batchSize =  Integer.parseInt(config.getProperty("streams.consumer.batch.size","1000"));

        XReadGroupParams groupParams = new XReadGroupParams();
        groupParams.count(batchSize);
        groupParams.block(1000);

        while (true) {
            Response<Map<String, List<StreamEntry>>> response = pipeline.xreadGroupAsMap(groupName, consumerName,
                    groupParams, Map.of(streamName, StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY));

            pipeline.sync();

            Map<String, List<StreamEntry>> entries = response.get();

            if (entries != null && entries.size() != 0) {
                processStreamEntries(entries, pipeline);
            }
            else {
                break;
            }

        }
    }

    public void processStreamEntries(Map<String,List<StreamEntry>> entries, Pipeline pipeline) throws Exception {
        String groupName = config.getProperty("streams.consumer.group.name", "group1");
        String streamName = this.config.getProperty("streams.topic.name", "topic1");

        if (entries != null && !entries.isEmpty()) {
            for (Map.Entry<String, List<StreamEntry>> entry : entries.entrySet()) {
                String stream = entry.getKey(); // The stream name
                List<StreamEntry> streamEntries = entry.getValue(); // List of StreamEntry objects

                // Iterate through the list of StreamEntry
                for (StreamEntry streamEntry : streamEntries) {

                    StreamEntryID entryID = streamEntry.getID(); 
                    Map<String, String> fields = streamEntry.getFields(); 

                    
                    for (Map.Entry<String, String> field : fields.entrySet()) {
                        //Process The Value
                        //System.out.println(field.getValue());
                        pipeline.xack(streamName, groupName, entryID);
                        pipeline.xdel(streamName, entryID);
                    }
                }
            }

            pipeline.sync();
        }
    }

    public static void main(String[] args) throws Exception {
        Scanner s = new Scanner(System.in);
        String configFile = "./config.properties";

        System.out.print("\nEnter the config file path (Defaults to ./config.properties): ");
        String configFile1 = s.nextLine();

        if (!"".equals(configFile1)) {
            configFile = configFile1;
        }

        RedisStreamFactory streamFactory = new RedisStreamFactory(configFile);

        System.err.print("\nChoose Option:\n[1] Create Consumer Group\n[2] Start Producing\n[3] Start Consuming\nSelect: ");

        String option = s.nextLine();

        if("1".equals(option)) {
            streamFactory.createConsumerGroup();
            
        }
        else if("2".equals(option)) {
            streamFactory.startProducer();

        }
        else if("3".equals(option)) {
            System.err.print("\nEnter Consumer Name: ");
            String consumerName = s.nextLine();
            AppStreams appStreams = new AppStreams(configFile);
            appStreams.consumeStream(consumerName);
        }
    }

}
