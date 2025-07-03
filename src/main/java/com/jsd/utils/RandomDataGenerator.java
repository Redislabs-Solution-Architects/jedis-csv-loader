package com.jsd.utils;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;



public class RandomDataGenerator {

    private JSONObject templateObj;
    private JSONObject uidCounters = new JSONObject();

    public RandomDataGenerator(String templateFile) throws Exception {

        JSONTokener tokener = new JSONTokener(new FileInputStream(templateFile));
        templateObj = new JSONObject(tokener);
    } 

      

    public HashMap<String, String> generateHashRecord(String objName) {
 
        HashMap<String, String> recordObj = new HashMap<String, String>();

        JSONObject jobj = templateObj.getJSONObject(objName);

        JSONArray schema = jobj.getJSONArray("schema");

        for(int f = 0; f < schema.length(); f++) {
            JSONObject fieldObj = schema.getJSONObject(f);
            
            if("TEXT".equalsIgnoreCase(fieldObj.getString("type"))) {
                recordObj.put(fieldObj.getString("field"), getString(fieldObj.getJSONArray("options")));
            }
            else if("AUTO-TEXT".equalsIgnoreCase(fieldObj.getString("type"))) {
                recordObj.put(fieldObj.getString("field"), fieldObj.getString("prefix") + getInt(fieldObj.getJSONArray("options")));
            }
            else if("UID".equalsIgnoreCase(fieldObj.getString("type"))) {
                recordObj.put(fieldObj.getString("field"), fieldObj.getString("prefix") + getUID(objName + "-" + fieldObj.getString("field")));
            }
            else if("NUM".equalsIgnoreCase(fieldObj.getString("type"))) {
                recordObj.put(fieldObj.getString("field"),  "" + getInt(fieldObj.getJSONArray("options")));
            }
        }

        return recordObj;
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
            else if("AUTO-TEXT".equalsIgnoreCase(fieldObj.getString("type"))) {
                recordObj.put(fieldObj.getString("field"), fieldObj.getString("prefix") + getInt(fieldObj.getJSONArray("options")));
            }
            else if("UID".equalsIgnoreCase(fieldObj.getString("type"))) {
                recordObj.put(fieldObj.getString("field"), fieldObj.getString("prefix") + getUID(objName + "-" + fieldObj.getString("field")));
            }
            else if("NUM".equalsIgnoreCase(fieldObj.getString("type"))) {
                recordObj.put(fieldObj.getString("field"), getInt(fieldObj.getJSONArray("options")));
            }
            else if("OBJ".equalsIgnoreCase(fieldObj.getString("type"))) {
                recordObj.put(fieldObj.getString("field"), getObject(fieldObj.getJSONArray("options")));
            }
            else if("ARR".equalsIgnoreCase(fieldObj.getString("type"))) {
                String refObj = fieldObj.getString("obj");

                JSONArray arrRange = new JSONArray();
                arrRange.put(1); arrRange.put(10);

                try {arrRange = fieldObj.getJSONArray("range");} catch(Exception ex) {}

                recordObj.put(fieldObj.getString("field"), getArray(refObj, arrRange));
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

    private int getUID(String key) {
        int counter = 1;

        try {
            counter = this.uidCounters.getInt(key) + 1;
        }
        catch(Exception e) {}

        this.uidCounters.put(key, counter);
        
        return counter;
    }

    private JSONArray getArray(String objName, JSONArray range) {
        int low = range.getInt(0);
        int high = range.getInt(1);
        //NUM OF ARRAY ELEMENTS
        int randLength = ThreadLocalRandom.current().nextInt(low , high);
        JSONArray returnArr = new JSONArray();

        for(int i = 0; i < randLength; i++) {
            returnArr.put(generateRecord(objName));
        }

        return returnArr;
    }


    public static void main(String[] args) throws Exception {
        RandomDataGenerator gen = new RandomDataGenerator("./data-template-stock-trades.json");
        for(int i = 0; i < 2; i++) {
            System.out.println(gen.generateRecord("header").toString());
        }
    }


}
