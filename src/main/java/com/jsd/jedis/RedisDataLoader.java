package com.jsd.jedis;

import com.jsd.utils.*;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Properties;
import java.util.List;

import org.json.JSONObject;

import redis.clients.jedis.*;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.csc.Cache;
import redis.clients.jedis.csc.CacheConfig;
import redis.clients.jedis.csc.CacheFactory;
import redis.clients.jedis.csc.Cacheable;
import redis.clients.jedis.csc.DefaultCacheable;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

/**
 * Simple Jedis Client
 *
 */
public class RedisDataLoader {

    private Pipeline jedisPipeline;
    private JedisPooled jedisPooled;
    private Properties config;
    private Cache clientCache;

    public RedisDataLoader(String configFile) throws Exception {
        config = new Properties();
        config.load(new FileInputStream(configFile));

        if ("true".equalsIgnoreCase(config.getProperty("client.cache", "false"))) {
            // CLIENT-SIDE CACHING
            HostAndPort host = HostAndPort.from(loadProperty("redis.host") + ":" + loadProperty("redis.port"));
            JedisClientConfig clientConfig = DefaultJedisClientConfig.builder().resp3()
                    .user(loadProperty("redis.user"))
                    .password(loadProperty("redis.password"))
                    .build();

            CacheConfig cacheConfig = getCacheConfig();
            this.clientCache = CacheFactory.getCache(cacheConfig);

            this.jedisPooled = new JedisPooled(host, clientConfig, clientCache);

            System.out.println("[RedisDataLoader] Client Side Caching Enabled");

        } else {
            this.jedisPooled = new JedisPooled(loadProperty("redis.host"), Integer.parseInt(loadProperty("redis.port")),
                    loadProperty("redis.user"), loadProperty("redis.password"));
        }

        this.jedisPipeline = this.jedisPooled.pipelined();

        System.out.println("[RedisDataLoader] Connection Successful PING >> " + jedisPooled.ping());
    }

    public RedisDataLoader(String configFile, boolean enableClientCache) throws Exception {
        config = new Properties();
        config.load(new FileInputStream(configFile));

        if (enableClientCache) {
            // CLIENT-SIDE CACHING
            HostAndPort host = HostAndPort.from(loadProperty("redis.host") + ":" + loadProperty("redis.port"));
            JedisClientConfig clientConfig = DefaultJedisClientConfig.builder().resp3()
                    .user(loadProperty("redis.user"))
                    .password(loadProperty("redis.password"))
                    .build();

            CacheConfig cacheConfig = getCacheConfig();
            this.clientCache = CacheFactory.getCache(cacheConfig);

            this.jedisPooled = new JedisPooled(host, clientConfig, clientCache);

            System.out.println("[RedisDataLoader] Client Side Caching Enabled");

        } else {
            this.jedisPooled = new JedisPooled(loadProperty("redis.host"), Integer.parseInt(loadProperty("redis.port")),
                    loadProperty("redis.user"), loadProperty("redis.password"));
        }

        this.jedisPipeline = this.jedisPooled.pipelined();

        System.out.println("[RedisDataLoader] Connection Successful PING >> " + jedisPooled.ping());
    }

    public String loadProperty(String property) {
        String prop = null;

        String propEnv = System.getenv(config.getProperty(property));

        if (propEnv != null) {
            prop = propEnv;
        } else if ("redis.host".equalsIgnoreCase(property)) {
            prop = config.getProperty(property, "localhost");

        } else if ("redis.port".equalsIgnoreCase(property)) {
            prop = config.getProperty(property, "6379");
        } else if ("redis.user".equalsIgnoreCase(property)) {
            prop = config.getProperty(property, "default");
        } else {
            prop = config.getProperty(property);
        }

        return prop;
    }

    private CacheConfig getCacheConfig() {

        Cacheable cacheable = new DefaultCacheable() {
            @Override
            public boolean isCacheable(ProtocolCommand command, List<Object> keys) {
                boolean doCache = false;

                for (Object key : keys) {
                    if (key.toString().startsWith(config.getProperty("client.cache.key.prefix", "NA"))) {
                        doCache = true;
                    }
                }

                return (doCache && isDefaultCacheableCommand(command));
            }
        };

        // Create a cache with a maximum size of 10000 entries
        return CacheConfig.builder()
                .maxSize(Integer.parseInt(config.getProperty("client.cache.size", "10000")))
                .cacheable(cacheable).build();
    }

    public boolean testPool() {

        boolean isValid = true;

        try {
            this.jedisPooled.ping();

        } catch (Exception e) {

            System.out.println("[RedisDataLoader] Test Failed:\n" + e.toString());
            System.out.println("[RedisDataLoader] Pool Status: Created: " + this.jedisPooled.getPool().getCreatedCount()
                    + " Active: " +
                    this.jedisPooled.getPool().getNumActive() + " Idle: " + this.jedisPooled.getPool().getNumIdle());

            isValid = false;

            this.jedisPooled.close();

            this.jedisPooled = new JedisPooled(config.getProperty("redis.host", "localhost"),
                    Integer.parseInt(config.getProperty("redis.port", "6379")),
                    config.getProperty("redis.user", "default"), config.getProperty("redis.password"));

            this.jedisPipeline = this.jedisPooled.pipelined();

            this.jedisPooled.ping();

            System.out.println("[RedisDataLoader] New Pool Created: " + this.jedisPooled.getPool().getCreatedCount()
                    + " Active: " +
                    this.jedisPooled.getPool().getNumActive() + " Idle: " + this.jedisPooled.getPool().getNumIdle());
        }

        return isValid;
    }

    public Pipeline getJedisPipeline() {
        return this.jedisPipeline;
    }

    public JedisPooled getJedisPooled() {
        return this.jedisPooled;
    }

    public Cache getClientCache() {
        return this.clientCache;
    }

    public void close() {
        this.jedisPooled.close();
    }

    public void loadGEO(String key, CSVScanner csvScanner, String geoCol, String longCol, String latCol,
            String entity) {

        try {
            int record = 0;
            while (csvScanner.hasMoreRecords()) {
                jedisPipeline.geoadd(key, Double.parseDouble(csvScanner.getColumnValue(longCol)),
                        Double.parseDouble(csvScanner.getColumnValue(latCol)),
                        csvScanner.getColumnValue(entity));

                if (record % 1000 == 0) {
                    jedisPipeline.sync();
                }

                record++;
            }

            jedisPipeline.sync();
            System.out.println("[loadGEO] Successfully Loaded " + --record + " Records into GEO Index");

        } catch (Exception e) {

        }
        return;
    }

    /*
     * Loads a CSV file into Hash objects.
     * The dataset must have a unique identified to serve as the key
     */

    public void loadHash(String keyPrefix, String idCol, CSVScanner csvScanner) throws Exception {

        try {

            int record = 0;
            int numCols = csvScanner.getNumColumns();
            String[] columnNames = csvScanner.getColumnNames();

            while (csvScanner.hasMoreRecords()) {

                // load the record into redis

                try {
                    // load the record in a Map
                    HashMap<String, String> recordMap = new HashMap<String, String>();

                    // set the id column
                    recordMap.put("_id", csvScanner.getColumnValue(idCol));

                    // add the other columns
                    for (int c = 0; c < numCols; c++) {
                        if (!columnNames[c].equals(idCol)) {
                            recordMap.put(columnNames[c], csvScanner.getColumnValue(columnNames[c]));
                        }
                    }

                    jedisPipeline.hset(keyPrefix + csvScanner.getColumnValue(idCol), recordMap);
                } catch (Exception e) {
                    jedisPipeline.del(keyPrefix + csvScanner.getColumnValue(idCol));
                }

                record++;
            }

            jedisPipeline.sync();

            System.out.println("Successfully Loaded " + --record + " Records into Hashes");

        } catch (Exception e) {
            System.err.println(e);
        }
    }

    public void loadHash(String keyPrefix, RandomDataGenerator dataGenerator, int numRows) throws Exception {
        // System.out.println("[RedisDataLoader] Loading Random Data " + keyPrefix);

        String sysTime = "" + System.currentTimeMillis();

        int r = 0;

        for (r = 0; r < numRows; r++) {
            jedisPipeline.hset(keyPrefix + sysTime + "-" + r, dataGenerator.generateHashRecord("header"));
        }

        jedisPipeline.sync();

        // System.out.println("[RedisDataLoader] Loaded " + r + " record objects");

    }

    /*
     * Loads a CSV file into JSON objects, with upto 1 level of nesting.
     * It assumes the dataset is sorted by the headerID and all columns on and after
     * the detailID are line items of the header
     */

    public void loadJSON(String keyPrefix, String keyType, String headerID, String detailID, String detailName,
            CSVScanner csvScanner, int numRows) throws Exception {

        System.out.println("[RedisDataLoader] Loading Keys " + keyPrefix);

        int numCols = csvScanner.getNumColumns();
        String[] columnNames = csvScanner.getColumnNames();
        int record = 1;

        String currentHeader = "NA";
        String prevHeader = "NA";

        JSONObject headerObj = new JSONObject();
        JSONObject detailObj = new JSONObject();
        String keyID = "";

        String sysTime = "" + System.currentTimeMillis();

        while (csvScanner.hasMoreRecords() && (record < numRows || numRows < 1)) {

            currentHeader = csvScanner.getColumnValue(headerID);

            // new header record
            if (!currentHeader.equals(prevHeader) && !"NA".equals(prevHeader)) {

                keyID = ("header".equalsIgnoreCase(keyType)) ? prevHeader : "" + record + "-" + sysTime;

                // write the previous record
                jedisPipeline.jsonSet(keyPrefix + keyID, headerObj);

                // start new header record
                headerObj = new JSONObject();

                record++;
            }

            detailObj = new JSONObject();

            boolean loadingHeader = true;

            // add the other columns
            for (int c = 0; c < numCols; c++) {

                if (detailID.equals(columnNames[c])) {
                    loadingHeader = false;
                }

                if (loadingHeader) {
                    setValue(headerObj, columnNames[c], csvScanner.getColumnValue(c));
                    // headerObj.put(columnNames[c], csvScanner.getColumnValue(c));
                } else {
                    setValue(detailObj, columnNames[c], csvScanner.getColumnValue(c));
                    // detailObj.put(columnNames[c], csvScanner.getColumnValue(c));
                }
            }

            // load details as an array, if the dataset has a 1 - Many relationship
            if (!loadingHeader) {
                headerObj.append(detailName, detailObj);
            }

            prevHeader = currentHeader;
        }

        // write the last final record
        keyID = ("header".equalsIgnoreCase(keyType)) ? currentHeader : "" + record + "-" + sysTime;
        jedisPipeline.jsonSet(keyPrefix + keyID, headerObj);

        // flush the pipeline
        jedisPipeline.sync();

        System.out.println("[RedisDataLoader] Loaded " + record + " record objects");

    }

    public void loadJSON(String keyPrefix, RandomDataGenerator dataGenerator, int numRows) throws Exception {
        // System.out.println("[RedisDataLoader] Loading Random Data " + keyPrefix);

        String sysTime = "" + System.currentTimeMillis();

        int r = 0;

        for (r = 0; r < numRows; r++) {
            jedisPipeline.jsonSet(keyPrefix + sysTime + "-" + r, dataGenerator.generateRecord("header"));
        }

        jedisPipeline.sync();

        // System.out.println("[RedisDataLoader] Loaded " + r + " record objects");

    }

    public int deleteKeys(String keyPrefix) {


        ScanParams scanParams = new ScanParams().count(10000).match(keyPrefix + "*"); // Set the chunk size
        String cursor = ScanParams.SCAN_POINTER_START;

        int keyCount = 0;

        while (true) {
            ScanResult<String> scanResult = jedisPooled.scan(cursor, scanParams);

            for (String key : scanResult.getResult()) {
                keyCount++;
                jedisPipeline.del(key);
                //jedisPipeline.unlink(key);
            }

            jedisPipeline.sync();

            cursor = scanResult.getCursor();
            if (cursor.equals("0")) {
                break; // End of scan
            }
        }

        
        return keyCount;
    }


    private void setValue(JSONObject jobj, String key, String stringValue) {
        try {
            int i = Integer.parseInt(stringValue);
            jobj.put(key, i);
            return;
        } catch (Exception e) {
        }

        try {
            double d = Double.parseDouble(stringValue);
            jobj.put(key, d);
            return;
        } catch (Exception e) {
        }

        jobj.put(key, stringValue);

    }

    public static void main(String[] args) throws Exception {
       
       
    }
}
