package com.jsd.jedis;

import com.jsd.utils.*;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import redis.clients.jedis.csc.Cache;

/**
 * Simple Jedis Client
 * This client runs the RedisDataLoader utility which can load data from a CSV
 * file into Redis Hash or JSON objects.
 * It uses JedisPipelining.
 * The JSON loader supports the loading on nested JSON.
 */
public class App {

    public static void main(String[] args) throws Exception {

        // set the DNS cache timeout to facilitate faster failover to replica
        java.security.Security.setProperty("networkaddress.cache.ttl", "2");

        Properties config = new Properties();
        Scanner s = new Scanner(System.in);
        String configFile = "./config.properties";

        System.out.print("\nEnter the config file path (Defaults to ./config.properties): ");
        String configFile1 = s.nextLine();

        if (!"".equals(configFile1)) {
            configFile = configFile1;
        }

        config.load(new FileInputStream(configFile));
        RedisDataLoader redisDataLoader = new RedisDataLoader(configFile);

        System.err.print("\nChoose Option:\n[1] Generate Random Data\n[2] Delete Keys\n[3] HA - Failover\n[4] Client-Side Caching\nSelect: ");

        String option = s.nextLine();

        if ("1".equalsIgnoreCase(option)) {
 
            generateRandomData(redisDataLoader, config);
           
        } else if ("2".equalsIgnoreCase(option)) {

            deleteKeys(redisDataLoader, config);

        } else if ("3".equalsIgnoreCase(option)) {

            writeFailover(configFile);

        } else if ("4".equalsIgnoreCase(option)) {

            clientSideCache(redisDataLoader, config);
        }
        else {
            //misc use cases
            generateRandomPrimitive(redisDataLoader, config);
        }

        redisDataLoader.close();
        s.close();
    }

    public static void generateRandomData(RedisDataLoader redisDataLoader, Properties config) throws Exception {
        String randomFilePath = config.getProperty("random.def.file");
        RandomDataGenerator dataGenerator = new RandomDataGenerator(randomFilePath);

        int numRecords = Math.max(Integer.parseInt(config.getProperty("data.record.limit")), 10000);
        int numBatch = numRecords / 10000;
        int batchSize = 10000;

        String recordType = config.getProperty("data.record.type", "JSON");
        String keyPrefix = config.getProperty("data.key.prefix");


        for (int b = 0; b < numBatch; b++) {
            if ("HASH".equalsIgnoreCase(recordType)) {
                redisDataLoader.loadHash(keyPrefix, dataGenerator, batchSize);
            }
            else {
                redisDataLoader.loadJSON(keyPrefix, dataGenerator, batchSize);
            }

            Thread.sleep(Long.parseLong(config.getProperty("data.batch.interval", "500")));
        }
    }

    public static void generateRandomPrimitive(RedisDataLoader redisDataLoader, Properties config) throws Exception {

        int numRecords = Math.max(Integer.parseInt(config.getProperty("data.record.limit")), 10000);
        int numBatch = numRecords / 10000;
        int batchSize = 10000;
        String keyPrefix = config.getProperty("data.key.prefix");

        Pipeline jedisPipeline = redisDataLoader.getJedisPipeline();

        
        for (int b = 0; b < numBatch; b++) {
            String sysTime = "" + System.currentTimeMillis();

            for(int r = 0; r <  batchSize; r++) {
                jedisPipeline.incrBy(keyPrefix + sysTime + "-" + r, (long)ThreadLocalRandom.current().nextInt(1, 10000));
                jedisPipeline.expire(keyPrefix + sysTime + "-" + r, 600l);
            }

            jedisPipeline.sync();

            //Thread.sleep(Long.parseLong(config.getProperty("data.batch.interval", "500")));
        }


        jedisPipeline.sync();
    
    }

    public static void clientSideCache(RedisDataLoader redisDataLoader, Properties config) {

        JedisPooled jedisPooled = redisDataLoader.getJedisPooled();
        Cache clientCache = redisDataLoader.getClientCache();
        Scanner s = new Scanner(System.in);


        //KEY PREFIX random generator appends the uts to the key prefix e.g. trades:123455667-1
        ScanParams scanParams = new ScanParams().count(100).match(config.getProperty("client.cache.key.prefix") + "*");
        String cursor = ScanParams.SCAN_POINTER_START;


        String keyPrefix0 = "";

        while (true) {
            ScanResult<String> scanResult = jedisPooled.scan(cursor, scanParams);

            for (String key : scanResult.getResult()) {
                keyPrefix0 = key;
                keyPrefix0 = keyPrefix0.substring(0, keyPrefix0.indexOf("-") + 1);
            }

            if (!"".equals(keyPrefix0)) {
                break;
            }

            cursor = scanResult.getCursor();

            if (cursor.equals("0")) {
                break;
            }
        }

        int cacheSize = Integer.parseInt(config.getProperty("client.cache.size","10000"));

        while (true) {

            System.out.print("Read Records? (y/n): ");

            String option = s.nextLine();

            if ("n".equalsIgnoreCase(option)) {
                break;
            }

            long startTime = System.currentTimeMillis();

            for (int k = 0; k < cacheSize; k++) {
                jedisPooled.jsonGet(keyPrefix0 + k);
            }

            long endTime = System.currentTimeMillis();

            System.out.println("[App] Read Time ms : " + (endTime - startTime));
            System.out.println("[App] Client Cache Size : " + clientCache.getSize());
            System.out.println("[App] Client Cache Stats : " + clientCache.getAndResetStats());
        }

        redisDataLoader.close();

        s.close();
    }

    public static void writeFailover(String configFile) throws Exception {

        Thread t = new Thread() {
            public void run() {

                Properties config = new Properties();
                RedisDataLoader redisDataLoader = null;

                Pipeline pipeline = null;

                try {

                    try {
                        redisDataLoader = new RedisDataLoader(configFile);
                        pipeline = redisDataLoader.getJedisPipeline();
                    } catch (Exception e) {
                    }

                    config.load(new FileInputStream(configFile));

                    String filePath = config.getProperty("random.def.file");

                    RandomDataGenerator dataGenerator = new RandomDataGenerator(filePath);

                    int numBatches = 100;
                    int batchSize = 1000;

                    boolean reconnect = false;

                    for (int batch = 0; batch < numBatches; batch++) {
                        try {

                            if (reconnect) {
                                // establish a new connection
                                redisDataLoader = new RedisDataLoader(configFile);
                                pipeline = redisDataLoader.getJedisPipeline();
                                reconnect = false;
                            }

                            for (int r = 0; r < batchSize; r++) {
                                pipeline.jsonSet("Batch-" + batch + ":Record-" + r,
                                        dataGenerator.generateRecord("header"));
                            }

                            pipeline.sync();
                        } catch (Exception e1) {
                            System.err.println("[App] Connection Failure; Retrying Connection: ");
                            System.err.println("[App] Re-Writing Batch : " + batch);
                            batch--;
                            reconnect = true;
                            Thread.sleep(5000l);
                        }

                        Thread.sleep(1000l);
                    }

                    System.out.println("[App] Successfully Written " + (numBatches * batchSize) + " Records");

                } catch (Exception e) {
                    System.out.println("[App] Exception in writeFailover() for\n" + e);
                }
            }
        };

        t.start();
    }

    public static void deleteKeys(RedisDataLoader redisDataLoader, Properties config) throws Exception {
        Scanner s = new Scanner(System.in);  
        System.out.print("Enter Key Prefix (e.g. trades:) ");
        String prefix = s.nextLine();

        long startTime = System.currentTimeMillis();

        int numKeys = redisDataLoader.deleteKeys(prefix);

        long endTime = System.currentTimeMillis();

        System.out.println("Deleted " + numKeys + " from the DB in " + getExecutionTime(startTime, endTime));

        s.close();
 
    }


    public static void loadFile(RedisDataLoader redisDataLoader, Properties config) throws Exception {
        // FILE DATA
        String keyPrefix = config.getProperty("data.key.prefix");
        String headerID = config.getProperty("data.header.field");
        String detailID = config.getProperty("data.detail.field");
        String detailName = config.getProperty("data.detail.attr.name");
        String filePath = config.getProperty("data.file");
        String keyType = "header"; // header or random
        CSVScanner scanner = new CSVScanner(filePath, ",", false);
        // redisDataLoader.loadHash(keyPrefix, headerID, scanner);
        redisDataLoader.loadJSON(keyPrefix, keyType, headerID, detailID, detailName, scanner, 0);
    }

    static String getExecutionTime(long startTime, long endTime) {
        long diff = endTime - startTime;

        long sec = Math.floorDiv(diff, 1000l);
        long msec = diff % 1000l;

        return "" + sec + " s : " + msec + " ms";
    }
}