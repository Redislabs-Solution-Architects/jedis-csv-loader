package com.jsd.jedis;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Scanner;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

import org.redisson.Redisson;
import org.redisson.config.Config;

import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.json.Path2;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

public class AppLock {

    RedissonClient redisson;
    RedisDataLoader dataLoader;
    Properties config = new Properties();

    public AppLock(String configFile) throws Exception {
        this.config.load(new FileInputStream(configFile));

        Config redConfig = new Config();
        redConfig.useReplicatedServers()
                .addNodeAddress(getLockHosts(config));
        redisson = Redisson.create(redConfig);

        dataLoader = new RedisDataLoader(configFile, false);

    }

    public void numericTest(Scanner s) throws Exception {
        JedisPooled jedisPooled = dataLoader.getJedisPooled();

        System.out.print("Enter Key: ");
        String key = s.nextLine();

        String pathNumeric = config.getProperty("numeric.path");

        String pathID = config.getProperty("lock.name.path");

        String lockHoldTime = config.getProperty("hold.time.ms","1000");


        for (int i = 0; i < 500; i++) {
            if (getLock(key)) {

                int decrValue = Integer.parseInt(config.getProperty("change.value"));

                String oldValue = jedisPooled.jsonGet(key, Path2.of(pathNumeric)).toString();
                oldValue = oldValue.substring(1, oldValue.length() - 1);
                int oldVal = Integer.parseInt(oldValue);

                if ((oldVal - decrValue) < 0) {

                    System.out.println("Insufficient Funds !! Balance Remaining: " + oldVal);

                } else {
                    jedisPooled.jsonSet(key, Path2.of(pathID), "\"" + config.getProperty("lock.name") + "\"");
                    String newValue = jedisPooled.jsonNumIncrBy(key, Path2.of(pathNumeric), - decrValue).toString();
                    newValue = newValue.substring(1, newValue.length() - 1);
                    int newVal = Integer.parseInt(newValue);

                    boolean valueBalanced = (oldVal == (newVal + decrValue));

                    System.out.println("Old Value: " + oldVal + " Change " + -decrValue + " New Value " + newVal + " Reconciliation: " + valueBalanced);
                }

                Thread.sleep(Long.parseLong(lockHoldTime));

                unlock(key);

            } else {
                System.err.println("[AppLock] " + key + " Locked by another process");
            }

            Thread.sleep(1000);
        }

        System.out.println("[AppLock] Client Exit");
    }

    public boolean getLock(String key) {
        boolean locked = false;
        RLock lock = redisson.getLock(key);

        long leaseTime = Long.parseLong(config.getProperty("lease.time.ms", "5000"));
        long waitTime = Long.parseLong(config.getProperty("wait.time.ms", "5000"));

        try {
            // ACQUIRE LOCK
            if (lock.tryLock(waitTime, leaseTime, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                locked = true;

            }
        } catch (Exception e) {
            System.err.println("[AppLock] Error Acquiring Lock\n" + e.toString());
        }

        return locked;
    }

    public boolean unlock(String key) {
        boolean unlocked = true;

        try {
            RLock lock = redisson.getLock(key);
            lock.unlock();
        } catch (Exception e) {}

        return unlocked;
    }

    public void testLock() {
        RLock lock = redisson.getLock("test-lock");

        try {
            // ACQUIRE LOCK
            if (lock.tryLock(500, 20000, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                System.out.println("[AppLock] Lock acquired");

                Thread.sleep(3000);

            } else {
                System.out.println("[AppLock] Could NOT acquire lock.");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (lock.isHeldByCurrentThread()) {
                // RELEASELOCK
                lock.unlock();
                System.out.println("[AppLock] Lock released.");
            }
        }
    }

    public String[] getLockHosts(Properties config) {
        Map<String, Map<String, String>> lockMap = new HashMap<>();

        for (String key : config.stringPropertyNames()) {
            if (key.matches("lock\\d+\\.redis\\.(host|port|user|password)")) {
                String[] parts = key.split("\\.");
                String lockId = parts[0];
                String property = parts[2];

                lockMap.computeIfAbsent(lockId, k -> new HashMap<>())
                        .put(property, config.getProperty(key));
            }
        }

        List<String> urls = new ArrayList<>();
        for (Map<String, String> props : lockMap.values()) {
            String user = props.getOrDefault("user", "");
            String passwordKey = props.getOrDefault("password", "");
            String password = System.getenv(passwordKey);
            if (password == null) {
                password = passwordKey;
            }

            String host = props.getOrDefault("host", "localhost");
            String port = props.getOrDefault("port", "6379");

            String url = String.format("redis://%s:%s@%s:%s", user, password, host, port);
            urls.add(url);
        }

        return urls.toArray(new String[0]);
    }

    public static void main(String[] args) throws Exception {

        Properties config = new Properties();
        Scanner s = new Scanner(System.in);

        String configFile = "./config-lock.properties";

        System.out.print("\nEnter the config file path (Defaults to ./config-lock.properties): ");
        String configFile1 = s.nextLine();

        if (!"".equals(configFile1)) {
            configFile = configFile1;
        }

        config.load(new FileInputStream(configFile));

        AppLock app = new AppLock(configFile);

        System.out.println("[AppLock] Client Name: " + config.getProperty("lock.name"));

        System.err.print("\nChoose Option:\n[1] Incr/Decr Numeric Value\n[2] Lock Test\nSelect: ");

        String option = s.nextLine();

        if ("1".equals(option)) {
            app.numericTest(s);
        }
        else {
            app.testLock();
        }
    }
}