package com.jsd.jedis;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Scanner;

import redis.clients.jedis.Jedis;

import com.jsd.utils.*;

/**
 * Redis Search Client using Jedis
 *
 */
public class AppAutoScale {

    private Properties config = new Properties();
    private RedisDataLoader redisDataLoader;
    private Jedis jedis;

    public AppAutoScale(String configFile) throws Exception {
        this.config = new Properties();
        this.config.load(new FileInputStream(configFile));


        jedis = new Jedis(config.getProperty("redis.host"), Integer.parseInt(config.getProperty("redis.port")));
        jedis.auth(config.getProperty("redis.password"));

        redisDataLoader = new RedisDataLoader(configFile);
        
    }

    public static void main(String[] args) throws Exception {

        Scanner s = new Scanner(System.in);

        // set the config file
        String configFile = "./config-auto-scale.properties";

        System.out.print("\nEnter the config file path (Defaults to ./config-auto-scale.properties): ");
        String configFile1 = s.nextLine();

        if (!"".equals(configFile1)) {
            configFile = configFile1;
        }



        AppAutoScale autoScale = new AppAutoScale(configFile);
       

        autoScale.startMonitoring();
        autoScale.loadData();


    }

    private void startMonitoring() throws Exception {
        RedisCloudAPI api = new RedisCloudAPI();

        double threshold = Double.parseDouble(config.getProperty("db.pct.threshold"));
        double factor = Double.parseDouble(config.getProperty("db.scale.factor"));

        String subscriptionID = config.getProperty("cloud.subscription");
        String dbID = config.getProperty("cloud.db");

        
        Thread t = new Thread()  {

            public void run()  {

                int scaleDB = 0;

                while(true) {
                    try {
                        String usedMemoryStr = jedis.info("memory");
                        usedMemoryStr = jedis.info("memory").split("\n")[1].split(":")[1].trim();
                        double usedMemory = Double.parseDouble(usedMemoryStr);
                        double dsize = api.getDatasetSize(subscriptionID, dbID);
                        double pct = (usedMemory / (dsize * 1000000000.0));
                        System.out.println("PCT Consumed: " + Math.round(pct * 100) + "%");

                        if(pct < threshold) {
                            scaleDB = 0;
                        }
                        else if(pct >= threshold && scaleDB == 0) {
                            scaleDB = 1;
                            System.err.println("[Monitor] Scaling Up to : " + (dsize * factor) + "GB");
                            String jsonPayload = "{ \"dryRun\": false,  \"datasetSizeInGb\": " + (dsize * factor) + "}";
                            api.updateDatabase(subscriptionID, dbID, jsonPayload);
                        }


                        Thread.sleep(1000);
                    }
                    catch(Exception e) {}
                }
            }
        };

        t.start();

    }

    private void loadData() throws Exception {

        Thread t = new Thread() {
            public void run() {
        
                try {

                    System.out.println("[AppAutoScale] Started Load ");

                    String filePath = config.getProperty("random.def.file");
                    String keyPrefix = config.getProperty("data.key.prefix");
                    int numRecords = Integer.parseInt(config.getProperty("data.record.limit"));

                    RandomDataGenerator dataGenerator = new RandomDataGenerator(filePath);

                    redisDataLoader.loadJSON(keyPrefix, dataGenerator, numRecords);
                    
                    redisDataLoader.close();

                } catch (Exception e) {
                    System.out.println("[AppAutoScale] Exception in loadData() " + "\n" + e);
                }
            }
        };

        t.start();
    }

}
