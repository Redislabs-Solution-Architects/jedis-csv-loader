package com.jsd.utils;

import java.io.FileInputStream;
import java.util.concurrent.ThreadLocalRandom;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;



public class RandomDataGenerator {

    private JSONObject templateObj;
    private int counter = 1;

    public RandomDataGenerator(String templateFile) throws Exception {

        JSONTokener tokener = new JSONTokener(new FileInputStream(templateFile));
        templateObj = new JSONObject(tokener);
    } 

    public JSONObject generateRecord(String objName) {
        JSONObject recordObj = new JSONObject();

        JSONObject jobj = templateObj.getJSONObject(objName);

        JSONArray schema = jobj.getJSONArray("schema");

        for(int f = 0; f < schema.length(); f++) {
            JSONObject fieldObj = schema.getJSONObject(f);

            if("TEXT".equalsIgnoreCase(fieldObj.getString("type"))) {
                recordObj.put(fieldObj.getString("field"), getString(fieldObj.getJSONArray("options")));
            }
            else if("ID".equalsIgnoreCase(fieldObj.getString("type"))) {
                recordObj.put(fieldObj.getString("field"), fieldObj.getString("prefix") + counter++);
            }
            else if("NUM".equalsIgnoreCase(fieldObj.getString("type"))) {
                recordObj.put(fieldObj.getString("field"), getInt(fieldObj.getJSONArray("options")));
            }
            else if("OBJ".equalsIgnoreCase(fieldObj.getString("type"))) {
                recordObj.put(fieldObj.getString("field"), getObject(fieldObj.getJSONArray("options")));
            }
            else if("ARR".equalsIgnoreCase(fieldObj.getString("type"))) {
                String refObj = fieldObj.getString("obj");
                recordObj.put(fieldObj.getString("field"), getArray(refObj));
            }
        }

        return recordObj;
    }

    private JSONObject getObject(JSONArray arr) {
        return arr.getJSONObject(ThreadLocalRandom.current().nextInt(0, arr.length()));
    }

    private String getString(JSONArray arr) {
        return arr.getString(ThreadLocalRandom.current().nextInt(0, arr.length()));
    }

    private int getInt(JSONArray arr) {
        return ThreadLocalRandom.current().nextInt(arr.getInt(0), arr.getInt(1));
    }

    private JSONArray getArray(String objName) {
        int randLength = ThreadLocalRandom.current().nextInt(1, 10);
        JSONArray returnArr = new JSONArray();

        for(int i = 0; i < randLength; i++) {
            returnArr.put(generateRecord(objName));
        }

        return returnArr;
    }


    public static void main(String[] args) throws Exception {
        RandomDataGenerator gen = new RandomDataGenerator("./data-template-orders.json");
        for(int i = 0; i < 2; i++) {
            System.out.println(gen.generateRecord("header").toString(4));
        }
    }


}