package com.jsd.jedis;

import com.jsd.utils.*;

import java.io.FileInputStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import redis.clients.jedis.csc.Cache;
import redis.clients.jedis.json.Path2;

/**
 * Simple Jedis Client
 * This client runs the RedisDataLoader utility which can load data from a CSV
 * file into Redis Hash or JSON objects.
 * It uses JedisPipelining.
 * The JSON loader supports the loading on nested JSON.
 */
public class App {

    public static RedisDataLoader redisDataLoader;

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
        redisDataLoader = new RedisDataLoader(configFile);

        System.err.print(
                "\nChoose Option:\n[1] Generate Random Data\n[2] Delete Keys\n[3] HA - Failover\n[4] Client-Side Caching\n[5] Get/Set Data\n[6] Track Key\nSelect: ");

        String option = s.nextLine();

        if ("1".equalsIgnoreCase(option)) {

            generateRandomData(redisDataLoader, config);

        } else if ("2".equalsIgnoreCase(option)) {

            deleteKeys(redisDataLoader, config);

        } else if ("3".equalsIgnoreCase(option)) {

            writeFailover(configFile);

        } else if ("4".equalsIgnoreCase(option)) {
            redisDataLoader = new RedisDataLoader(configFile, true);
            clientSideCache(redisDataLoader, config);

        } else if ("5".equalsIgnoreCase(option)) {
            dataUpdates(redisDataLoader, config);
        } else if ("6".equalsIgnoreCase(option)) {
            System.out.print("Enter Key: ");
            String key = s.nextLine();

            System.out.print("Key Type (JSON/HASH): ");
            String keyType = s.nextLine();

            keyType = ("".equals(keyType)) ? "HASH" : keyType;

            System.out.print("Enter Fields (field1,field2,...): ");
            String[] fields = s.nextLine().split(",");

            System.out.print("Enter Label e.g (region=us-east): ");
            String labelField = s.nextLine();

            System.out.print("Increment By: ");
            int value = s.nextInt();

            System.out.print("Interval (msec): ");
            int interval = s.nextInt();

            System.out.print("Itterations : ");
            int itterations = s.nextInt();

            // add the label field to the list of fields to be tracked
            String[] allFields = new String[fields.length + 1];
            System.arraycopy(fields, 0, allFields, 0, fields.length);
            allFields[allFields.length - 1] = labelField.substring(0, labelField.indexOf("="));

            int trackInterval = Integer.parseInt(config.getProperty("track.key.interval", "10"));

            int randomFactor = Integer.parseInt(config.getProperty("track.random.factor", "10"));
            int randomDuration = Integer.parseInt(config.getProperty("track.random.duration", "100"));

            
            AtomicInteger itterationTracker = new AtomicInteger(0);

            trackKey(redisDataLoader, key, keyType, itterationTracker, trackInterval, itterations, allFields, true);

            updateKey(redisDataLoader, key, keyType, itterationTracker, interval, itterations, randomFactor, randomDuration, fields, value,
                    labelField);

            Thread.sleep(120000l);

        } else {
            // misc use cases
            generateRandomPrimitive(redisDataLoader, config);
        }

        redisDataLoader.close();
        s.close();
    }

    public static void generateRandomData(RedisDataLoader redisDataLoader, Properties config) throws Exception {
        String randomFilePath = config.getProperty("random.def.file");
        RandomDataGenerator dataGenerator = new RandomDataGenerator(randomFilePath);

        int numRecords = Math.max(Integer.parseInt(config.getProperty("data.record.limit")), 10000);
        int batchSize = Integer.parseInt(config.getProperty("data.batch.size", "10000"));
        int numThreads = Integer.parseInt(config.getProperty("data.num.threads", "1"));
        long batchInterval = Long.parseLong(config.getProperty("data.batch.interval", "10000"));

        String recordType = config.getProperty("data.record.type", "JSON");
        String keyPrefix = config.getProperty("data.key.prefix");

        redisDataLoader.loadData(recordType, keyPrefix, dataGenerator, numRecords, batchSize, batchInterval,
                numThreads);

    }

    public static void updateKey(RedisDataLoader redisDataLoader, String key, String keyType,
            AtomicInteger itterationTracker, int interval,
            int itterations, int randomFactor, int randomDuration, String[] fields, int value, String labelField) throws Exception {

  

        String[] labelSet = labelField.split("=");

        Pipeline p = redisDataLoader.getJedisPipeline();

        for (int s = 0; s < itterations; s++) {

            itterationTracker.incrementAndGet();

            for (String field : fields) {
                if ("HASH".equalsIgnoreCase(keyType)) {
                    p.hincrBy(key, field, value);
                } else {
                    p.jsonNumIncrBy(key, Path2.of(field), (double) value);
                }
            }

            if ("HASH".equalsIgnoreCase(keyType)) {
                p.hset(key, labelSet[0], labelSet[1]);
            }

            p.sync();

            try {

                if (s % randomFactor == 0) {
                    Thread.sleep((long)randomDuration);
                } else {
                    Thread.sleep((long) interval);
                }

            } catch (Exception e) {
            }
        }

        itterationTracker.incrementAndGet();
    }

    public static void trackKey(RedisDataLoader redisDataLoader, String key, String keyType,
            AtomicInteger itterationTracker, int trackInterval,
            int itterations, String[] fields, boolean async) throws Exception {
        JedisPooled jedisPooled = redisDataLoader.getJedisPooled();

        Thread t = new Thread() {
            public void run() {


                while (true) {
                    if ("HASH".equalsIgnoreCase(keyType)) {
                        List<String> results = jedisPooled.hmget(key, fields);
                        String displayString = "[" + System.currentTimeMillis() + "] ";
                        for (String val : results) {
                            displayString = displayString + val + " | ";
                        }

                        System.err.println(displayString);

                        if(itterationTracker.get() <= itterations) {
                            try {Thread.sleep((long) trackInterval);} catch (Exception e) {}
                        }
                        else {
                            try {Thread.sleep(500l);} catch (Exception e) {}
                        }
                    }
                }

                //System.out.println("[App] Tracking Complete after Itterations: " + (itterationTracker.get() - 1));
            }
        };

        if (async) {
            t.start();
        } else {
            t.run();
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

            for (int r = 0; r < batchSize; r++) {
                jedisPipeline.incrBy(keyPrefix + sysTime + "-" + r,
                        (long) ThreadLocalRandom.current().nextInt(1, 10000));
                // jedisPipeline.expire(keyPrefix + sysTime + "-" + r, 600l);
            }

            jedisPipeline.sync();

            // Thread.sleep(Long.parseLong(config.getProperty("data.batch.interval",
            // "500")));
        }

        jedisPipeline.sync();

    }

    public static void clientSideCache(RedisDataLoader redisDataLoader, Properties config) {

        JedisPooled jedisPooled = redisDataLoader.getJedisPooled();
        Cache clientCache = redisDataLoader.getClientCache();
        Scanner s = new Scanner(System.in);

        // KEY PREFIX random generator appends the uts to the key prefix e.g.
        // trades:123455667-1
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

        int cacheSize = Integer.parseInt(config.getProperty("client.cache.size", "10000"));

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

            System.out.println("[App] Last Key Read : " + keyPrefix0 + (cacheSize - 1));
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

        String async = config.getProperty("async.delete", "true");
        System.out.println("Deleting Keys Async: " + async);

        long startTime = System.currentTimeMillis();

        int numKeys = redisDataLoader.deleteKeys(prefix);

        long endTime = System.currentTimeMillis();

        System.out.println("Deleted " + numKeys + " from the DB in " + getExecutionTime(startTime, endTime));

        s.close();

    }

    public static void dataUpdates(RedisDataLoader redisDataLoader, Properties config) throws Exception {

        JedisPooled jedisPooled = redisDataLoader.getJedisPooled();

        Scanner s = new Scanner(System.in);
        System.out.print("Enter Key Prefix (e.g. trades:) ");
        String keyPrefix = s.nextLine();

        System.out.print("Action Type (r/w:) ");
        String action = s.nextLine();

        action = ("".equals(action)) ? "w" : action;

        System.out.print("Enter (actions/sec) ");
        int batchSize = Integer.parseInt(s.nextLine());

        int scanBatchSize = (batchSize < 1000) ? batchSize : 1000;

        System.out.print("Key Type (JSON/HASH) : ");
        String keyType = s.nextLine();

        keyType = ("".equals(keyType)) ? "HASH" : keyType;

        System.out.print("Field to Edit (e.g. $.amount, amount) : ");
        String keyAttribute = s.nextLine();

        System.out.print("Run Time (min) : ");
        int runTime = Integer.parseInt(s.nextLine());

        ScanParams scanParams = new ScanParams().count(scanBatchSize).match(keyPrefix + "*"); // Set the chunk size
        String cursor = ScanParams.SCAN_POINTER_START;

        int keyCount = 0;

        List<String> keyList = new ArrayList<String>();

        System.out.println("Scanning Keys: ");

        while (true) {
            ScanResult<String> scanResult = jedisPooled.scan(cursor, scanParams);

            keyList.addAll(scanResult.getResult());
            keyCount = keyCount + keyList.size();

            cursor = scanResult.getCursor();

            if (cursor.equals("0") || keyCount >= batchSize) {
                break; // End of scan
            }
        }

        Pipeline jedisPipeline = redisDataLoader.getJedisPipeline();

        System.out.println("Keys: " + keyList.getFirst() + " ... " + keyList.getLast());

        int numKeys = keyList.size();
        int pipeBatch = numKeys / 10;

        for (int i = 0; i < runTime * 60; i++) {

            for (int k = 0; k < numKeys; k++) {
                int delta = ThreadLocalRandom.current().nextInt(10000, 100000); // 1000–10000

                if (keyType.startsWith("H")) {
                    if ("w".equalsIgnoreCase(action)) {
                        jedisPipeline.hincrBy(keyList.get(k), keyAttribute, delta);
                    } else {
                        jedisPipeline.hget(keyList.get(k), keyAttribute);
                    }

                } else {
                    if ("w".equalsIgnoreCase(action)) {
                        jedisPipeline.jsonNumIncrBy(keyList.get(k), Path2.of(keyAttribute), delta);
                    } else {
                        jedisPipeline.jsonGet(keyList.get(k), Path2.of(keyAttribute));
                    }

                }

                if ((k + 1) % pipeBatch == 0) {
                    jedisPipeline.sync();
                    Thread.sleep(100l);
                }
            }
        }

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