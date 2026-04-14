package com.jsd.jedis;

import java.util.HashSet;
import java.util.Scanner;

import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.JedisPubSub;

public class AppPubSub {

    HashSet<String> keys = new HashSet<>();

    String keyPrefix;
    String lastKey;

    int eventCount = 0;

    JedisPooled jedisPooled;
    JedisPubSub jedisPubSub;

    boolean printKeyEvent = false;

    public AppPubSub(String configFile) throws Exception {
        RedisDataLoader dataLoader = new RedisDataLoader(configFile);
        this.jedisPooled = dataLoader.getJedisPooled();

        // Subscriber definition
        this.jedisPubSub = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                System.out.println("Channel: " + channel + " | Message: " + message);
            }

            @Override
            public void onPMessage(String pattern, String channel, String message) {
                logEvent(channel);

                if (printKeyEvent) {
                    System.out.println("Pattern: " + pattern + " | Channel: " + channel + " | Message: " + message);
                }

            }
        };
    }

    private void logEvent(String event) {
        // System.out.println("[KeySpaceListener] Event " + event);

        keys.add(event);
        eventCount++;
        lastKey = event.substring(15);
    }

    public void trackKeys(int targetKeys, boolean continuousTracking) throws Exception {
        System.out.println("[KeySpaceListener] Tracking Keys");
        Thread t = new Thread() {
            public void run() {
                int marker = targetKeys / 10;
                while (true) {
                    if (keys.size() == targetKeys) {
                        System.out.println("[KeySpaceListener] Detected " + keys.size() + " Keys in Redis");
                        System.out.println("[KeySpaceListener] Last Key Added: " + lastKey);
                        break;
                    } else if (continuousTracking && (keys.size() >= marker)) {
                        marker = marker + (targetKeys / 10);
                        System.out.println("[KeySpaceListener] Detected " + keys.size() + " Keys in Redis");
                        System.out.println("[KeySpaceListener] Last Key Added: " + lastKey);
                    }

                    try {
                        Thread.sleep(1000l);
                    } catch (Exception e) {
                    }
                }
            }
        };

        t.start();
    }

    public void setPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public void subscribeChannel(String channelName) {

        // Subscribe a specific channel
        new Thread(() -> jedisPooled.subscribe(jedisPubSub, channelName)).start();
    }

    public void listenKeySpace(String pattern) {

        // Subscribe to keyspace changes on all keys
        new Thread(() -> jedisPooled.psubscribe(jedisPubSub, "__keyspace@0__:" + pattern)).start();
    }

    public static void main(String[] args) throws Exception {

        Scanner s = new Scanner(System.in);
        String configFile = "./config.properties";

        System.out.print("\nEnter the config file path (Defaults to ./config.properties): ");
        String configFile1 = s.nextLine();

        if (!"".equals(configFile1)) {
            configFile = configFile1;
        }

        AppPubSub ps = new AppPubSub(configFile);

        System.err.print("\nChoose Option:\n[1] Subscribe to Channel\n[2] Listen to Key Space\nSelect: ");

        String option = s.nextLine();

        if ("1".equalsIgnoreCase(option)) {
            System.err.print("Channel Name: ");
            String channelName = s.nextLine();
            ps.subscribeChannel(channelName);

        } else if ("2".equalsIgnoreCase(option)) {
            System.err.print("Key Pattern (e.g. trades:*) :  ");
            String pattern = s.nextLine();
            ps.listenKeySpace(pattern);
            ps.printKeyEvent = true;
        }

        ps.setPrefix("NA");
    }
}
