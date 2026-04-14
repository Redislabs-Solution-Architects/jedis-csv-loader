package com.jsd.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

public class JSONUtils {

    public void saveJSONObject(JSONObject obj, String fileName) throws Exception {
        FileWriter fw = new FileWriter(fileName);
        fw.write(obj.toString(4));
        fw.flush();
        fw.close();
    }

    public JSONObject createDataTemplate(String tableSchemaFile) throws Exception {
        // load the data template file
        JSONTokener tokener = new JSONTokener(new FileInputStream("./data-template.json"));
        JSONObject dataObject = new JSONObject(tokener);

        JSONObject headerObject = dataObject.getJSONObject("header");
        JSONArray dataSchema = headerObject.getJSONArray("schema");

        JSONArray stringOptions = new JSONArray("[1,10000]");
        JSONArray dateOptions = new JSONArray("[1563753600,1698883200]");
        JSONArray numOptions = new JSONArray("[100000,1000000]");
        JSONArray booleanOptions = new JSONArray("[\"true\",\"false\"]");

        BufferedReader br = new BufferedReader(new FileReader(tableSchemaFile));
        String line;
        while ((line = br.readLine()) != null) {

            String[] colArray = line.split(",");
            String fieldName = colArray[0];
            String fieldType = colArray[1];

            JSONObject schemaObject = new JSONObject();
            schemaObject.put("field", fieldName);

            if (fieldType.toUpperCase().indexOf("CHAR") > -1 || "text".equalsIgnoreCase(fieldType)
                    || "string".equalsIgnoreCase(fieldType)) {
                schemaObject.put("type", "AUTO-TEXT");
                schemaObject.put("prefix", fieldName + "-");
                schemaObject.put("options", stringOptions);
            } else if (fieldType.toUpperCase().indexOf("UID") > -1) {
                schemaObject.put("type", "UID");
                schemaObject.put("prefix", fieldName + "-");
            } else if (fieldType.toUpperCase().indexOf("DATE") > -1 || fieldType.toUpperCase().indexOf("TIME") > -1) {
                schemaObject.put("type", "NUM");
                schemaObject.put("options", dateOptions);
            } else if (fieldType.toUpperCase().indexOf("NUM") > -1 || fieldType.toUpperCase().indexOf("INT") > -1
                    || fieldType.toUpperCase().indexOf("FLOAT") > -1) {
                schemaObject.put("type", "NUM");
                schemaObject.put("options", numOptions);
            } else if (fieldType.toUpperCase().indexOf("BOOLEAN") > -1) {
                schemaObject.put("type", "TEXT");
                schemaObject.put("options", booleanOptions);
            }
            else {
                throw new Exception("createDataTemplate() Invalid Field Type: " + fieldType);
            }

            dataSchema.put(schemaObject);
        }

        br.close();

        return dataObject;
    }

    public JSONObject createIndexTemplate(JSONObject dataObj, String keyPrefix, String keyType, int numFields)
            throws Exception {
        // load the index template file
        JSONTokener tokener = new JSONTokener(new FileInputStream("./index-def-template.json"));
        JSONObject indexObj = new JSONObject(tokener);

        indexObj.put("prefix", keyPrefix);
        indexObj.put("type", keyType);

        JSONArray indexSchema = indexObj.getJSONArray("schema");

        JSONObject dataHeader = dataObj.getJSONObject("header");
        JSONArray schemaArray = dataHeader.getJSONArray("schema");

        Set<String> uniqueFields = new HashSet<String>();

        if (numFields < 1) {
            numFields = schemaArray.length();
        }

        for (int i = 0; i < numFields; i++) {
            JSONObject fieldObj = schemaArray.getJSONObject(i);
            JSONObject indexFieldObj = new JSONObject();

            String dataType = fieldObj.getString("type");
            String fieldName = fieldObj.getString("field");

            // skip duplicate records
            if (uniqueFields.contains(fieldName)) {
                continue;
            }

            if ("JSON".equalsIgnoreCase(keyType)) {
                indexFieldObj.put("field", "$." + fieldName);
            } else {
                indexFieldObj.put("field", fieldName);
            }

            indexFieldObj.put("alias", fieldName);
            indexFieldObj.put("sortable", true);

            if ("NUM".equalsIgnoreCase(dataType)) {

                indexFieldObj.put("type", "NUMERIC");
                indexSchema.put(indexFieldObj);

            } else if(dataType.toUpperCase().indexOf("TEXT") > -1) {

                indexFieldObj.put("type", "TAG");
                indexSchema.put(indexFieldObj);
            }

            
            uniqueFields.add(fieldName);
        }

        return indexObj;

    }

    public static void main(String[] args) throws Exception {

        JSONUtils utils = new JSONUtils();

        Properties config = new Properties();

        Scanner s = new Scanner(System.in);

        // set the config file
        String configFile = "./config.properties";

        System.out.print("\nEnter the config file path (Defaults to ./config.properties): ");
        String configFile1 = s.nextLine();

        if (!"".equals(configFile1)) {
            configFile = configFile1;
        }

        config.load(new FileInputStream(configFile));

        String dataFileName = config.getProperty("random.def.file");
        String indexFileName = config.getProperty("index.def.file");

        String keyPrefix = config.getProperty("data.key.prefix");
        String keyType = config.getProperty("data.record.type", "JSON");

        //NUMBER OF FIELDS TO INDEX
        int numIndexFields = 10;

        System.err.print("\nChoose Option:\n[1] Generate Data File\n[2] Generate Index File\nSelect: ");

        String option = s.nextLine();

        if ("1".equalsIgnoreCase(option)) {
            System.err.print("\nTable Schema File: ");
            String tableSchemaFile = s.nextLine();
            utils.saveJSONObject(utils.createDataTemplate(tableSchemaFile), dataFileName);
        } else {
            JSONTokener tokener = new JSONTokener(new FileInputStream(dataFileName));
            JSONObject dataObj = new JSONObject(tokener);

            utils.saveJSONObject(utils.createIndexTemplate(dataObj, keyPrefix, keyType, numIndexFields), indexFileName);
        }
    }
}
