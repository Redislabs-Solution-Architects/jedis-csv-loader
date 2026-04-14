package com.jsd.jedis;

import com.jsd.utils.*;
import com.redis.streams.command.serial.SerialTopicConfig;
import com.redis.streams.command.serial.TopicManager;
import com.redis.streams.command.serial.TopicProducer;
import com.redis.streams.command.serial.SerialTopicConfig.TTLFuzzMode;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Scanner;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;


public class RedisStreamFactory {

    Properties config = new Properties();
    RedisDataLoader redisDataLoader;

    public RedisStreamFactory(String configFile) throws Exception {
        this.config.load(new FileInputStream(configFile));
        redisDataLoader = new RedisDataLoader(configFile);
    }

    public void startProducer() throws Exception {
        String topicName = this.config.getProperty("streams.topic.name", "topic1");
        JedisPooled jedisPooled = redisDataLoader.getJedisPooled();

        String randomFilePath = config.getProperty("random.def.file");
        RandomDataGenerator dataGenerator = new RandomDataGenerator(randomFilePath);

        int numRecords = Math.max(Integer.parseInt(config.getProperty("data.record.limit")), 10000);
        int batchSize = 10000;
        int numThreads = Integer.parseInt(config.getProperty("streams.producer.num.threads","2"));

        CountDownLatch latch = new CountDownLatch(numThreads);

        System.out.println("[RedisStreamFactory] Producing Stream Entries:");

        long startTime = System.currentTimeMillis();

        //CREATE TOPIC
        for(int t =0; t < numThreads; t++) {
            Pipeline pipeline = jedisPooled.pipelined();
            producerThread(topicName, pipeline, t, numRecords/numThreads, batchSize, dataGenerator, latch) ;
        }

 
        latch.await();

        long endTime = System.currentTimeMillis();

        System.out.println("[RedisStreamFactory] Load Time sec: " + (endTime - startTime) / 1000);

    }

    private void producerThread(String topicName, Pipeline pipeline, int threadNum, int numRecords, int batchSize, 
                                RandomDataGenerator dataGenerator, CountDownLatch latch) throws Exception {
        int numBatch = numRecords / batchSize;
        Thread t = new Thread() {
            public void run() {
                XAddParams addParams = new XAddParams();
                addParams.maxLen(10000000);

                for (int b = 0; b < numBatch; b++) {
                    for(int r = 0; r < batchSize; r++) {
                        //producer.produce(Map.of("trade", dataGenerator.generateRecord("header").toString()));
                        pipeline.xadd(topicName, addParams, Map.of("value", dataGenerator.generateRecord("header").toString()));          
                    }
        
                    pipeline.sync();        
                }  

                latch.countDown();
            }
        };

        t.start();
    }

    public void createConsumerGroup() {
        try {
            JedisPooled jedisPooled = redisDataLoader.getJedisPooled();

            String groupName = config.getProperty("streams.consumer.group.name", "group1");
            String streamName = this.config.getProperty("streams.topic.name", "topic1");

            String status = jedisPooled.xgroupCreate(streamName, groupName, StreamEntryID.XGROUP_LAST_ENTRY, true);

            System.out.println("[RedisStreamFactory] Created Consumer Group: " + groupName + " " + status + " Stream: " + streamName);

        } catch (Exception e) {
            System.err.println("[RedisStreamFactory] " + e.getMessage());
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

        RedisStreamFactory appStreams = new RedisStreamFactory(configFile);
        appStreams.startProducer();

    }

}
