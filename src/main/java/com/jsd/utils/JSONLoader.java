package com.jsd.utils;

import java.io.FileInputStream;

import com.jsd.jedis.RedisDataLoader;

import redis.clients.jedis.Pipeline;


import org.json.JSONObject;
import org.json.JSONTokener;

public class JSONLoader {

    public static void main(String[] args) throws Exception {
        String templateFile = "./dtcc1.json";
        int numRecords = 100000;

        JSONTokener tokener = new JSONTokener(new FileInputStream(templateFile));
        JSONObject templateObj = new JSONObject(tokener);

        String uid = templateObj.getString("productId");

        RedisDataLoader dataLoader = new RedisDataLoader("./config.properties");

        Pipeline pipeline = dataLoader.getJedisPipeline();

        for(int i = 0; i < numRecords; i++) {
            pipeline.jsonSet(uid + ":F2:Q7299904535533-" +  i, templateObj);
        }

        pipeline.sync();
    }
}
