package com.jsd.jedis;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.function.Consumer;

import com.jsd.utils.RandomDataGenerator;

import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;

import redis.clients.jedis.MultiClusterClientConfig;
import redis.clients.jedis.MultiClusterClientConfig.ClusterConfig;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.providers.MultiClusterPooledConnectionProvider;

public class AACusterWatchDog {

    UnifiedJedis jedis;
    AbstractPipeline pipeline;
    MultiClusterPooledConnectionProvider connectionProvider;

    public AACusterWatchDog(String[] configFiles) throws Exception {
        int numClusters = configFiles.length;

        JedisClientConfig config = null;
        ClusterConfig[] clientConfigs = new ClusterConfig[numClusters];

        Properties configFile = new Properties();

        for (int c = 0; c < numClusters; c++) {
            configFile.load(new FileInputStream(configFiles[c]));

            if (c == 0) {
                config = DefaultJedisClientConfig.builder().user(configFile.getProperty("redis.user"))
                        .password(configFile.getProperty("redis.password")).build();
            }

            clientConfigs[c] = new ClusterConfig(new HostAndPort(configFile.getProperty("redis.host"),
                    Integer.parseInt(configFile.getProperty("redis.port"))), config);
        }

        MultiClusterClientConfig.Builder builder = new MultiClusterClientConfig.Builder(clientConfigs);
        builder.circuitBreakerSlidingWindowSize(2);
        builder.circuitBreakerSlidingWindowMinCalls(2);
        builder.circuitBreakerFailureRateThreshold(50.0f);

        this.connectionProvider = new MultiClusterPooledConnectionProvider(builder.build());

        this.jedis = new UnifiedJedis(connectionProvider);

        this.pipeline = this.jedis.pipelined();

        System.out.println("[AAClusterWatchDog] Test Connection: " + this.jedis.ping());
    }

    public boolean testClusterConnection(int clusterNum) {
        return this.connectionProvider.getConnection(clusterNum).isConnected();
    }

    public void addListener(Consumer<String> redisClient) {
        this.connectionProvider.setClusterFailoverPostProcessor(redisClient);
    }


    public void writeFailover(String configFile1, String configFile2) throws Exception {
        AbstractPipeline pipeline = this.pipeline;
        Thread t = new Thread() {
            public void run() {

                Properties config1 = new Properties();
                Properties config2 = new Properties();
        
                try {
                    config1.load(new FileInputStream(configFile1));
                    config2.load(new FileInputStream(configFile2));

                    System.out.println("[AppActiveActive] Started Load " + config1.getProperty("cluster.name"));

                    String filePath = config1.getProperty("random.def.file");
                    
                    RandomDataGenerator dataGenerator = new RandomDataGenerator(filePath);
                    
                    int numBatches = 100;
                    int batchSize = 1000;

                    
                    for (int batch = 0; batch < numBatches; batch++) {
                        
                        try {
                            for (int r = 0; r < batchSize; r++) {
                                pipeline.jsonSet("Batch-" + batch + ":Record-" + r, dataGenerator.generateRecord("header"));
                            }
                            pipeline.sync();
                        }
                        catch(Exception e1) {
                            System.err.println("[AppActiveActive] Cluster Connection Failure: " + config1.getProperty("cluster.name"));
                            System.err.println("[AppActiveActive] Failover to Cluster2: " + config2.getProperty("cluster.name"));
                            System.err.println("[AppActiveActive] Re-Writing Batch : " + batch);
                            batch--;
                            //redisDataLoader = new RedisDataLoader(configFile2);
                            //pipeline = redisDataLoader.getJedisPipeline();
                        }

                        Thread.sleep(1000l);
                    }

                    System.out.println("[AppActiveActive] Successfully Written " + (numBatches * batchSize) + " Records");

                } catch (Exception e) {
                    System.out.println("[AppActiveActive] Exception in writeFailover() for "
                            + config1.getProperty("cluster.name") + "\n" + e);
                }
            }
        };

        t.start();
    }



    public static void main(String[] args) throws Exception {
        AACusterWatchDog cwd = new AACusterWatchDog(
                new String[] { "./config-aa1.properties", "./config-aa2.properties" });
        cwd.writeFailover("./config-aa1.properties", "./config-aa2.properties");
    }
}
