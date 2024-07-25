package com.jsd.jedis;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.json.JSONArray;
import org.json.JSONObject;

import com.jsd.utils.*;

import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.SearchResult;
import redis.clients.jedis.search.Document;

/**
 * Redis Search Client using Jedis
 *
 */
public class AppSearch {

    private RedisDataLoader redisDataLoader;
    private Pipeline jedisPipeline;
    private JedisPooled jedisPooled;

    private RedisIndexFactory indexFactory;
    private Properties config;

    private String indexName;
    private String indexDefFile;

    private Scanner s;

    public AppSearch(String configFile) throws Exception {

        this.config = new Properties();
        this.config.load(new FileInputStream(configFile));

        this.redisDataLoader = new RedisDataLoader(configFile);
        this.jedisPooled = redisDataLoader.getJedisPooled();
        this.jedisPipeline = redisDataLoader.getJedisPipeline();

        this.indexFactory = new RedisIndexFactory(configFile);

        this.indexName = config.getProperty("index.name");
        this.indexDefFile = config.getProperty("index.def.file");

        this.s = new Scanner(System.in);

    }

    public void start() throws Exception {


        System.out.println("\nWould you like to Reload Data for: " + indexName + " ? (y/n)");

        String loadData = s.nextLine();

        if ("y".equalsIgnoreCase(loadData)) {

            String prefix = config.getProperty("data.key.prefix");

            // DROP the INDEX
            indexFactory.dropIndex(indexName);

            // DELETE existing KEYS
            System.out.println("[AppSearch] Deleting Existing Keys");
            System.out.println("[AppSearch] Deleted " + redisDataLoader.deleteKeys(prefix) + " Keys");

            // LOAD DATA as JSON
            CSVScanner scanner = new CSVScanner(config.getProperty("data.file"), ",", false);
            redisDataLoader.loadJSON(prefix, "header", config.getProperty("data.header.field"),
                    config.getProperty("data.detail.field"),
                    config.getProperty("data.detail.attr.name"),
                    scanner, Integer.parseInt(config.getProperty("data.record.limit")));
            

            // CREATE the INDEX
            indexFactory.createIndex(indexName, indexDefFile);
            indexFactory.monitorIndex(indexName, this);

            //startQueryPrompt();

        } else {
            startQueryPrompt();
        }



    }

    public void startQueryPrompt() throws Exception {

        // PRINT INDEX SCHEMA FOR REF
        System.out.println("\n" + printIndexSchema(indexFactory.getIndexObj(indexDefFile), indexName) + "\n");

        while (true) {

            System.out.println("\n[----------------------------------------------------------------------------]");
            System.out.println("Enter Search String :");

            String queryStr = s.nextLine();

            if ("bye|quit".indexOf(queryStr) > -1) {
                break;
            }

            int queryDialect = Integer.parseInt(config.getProperty("query.dialect", "1"));

            Query q = new Query(queryStr);
            q.dialect(queryDialect);
            q.limit(0, Integer.parseInt(config.getProperty("query.record.limit", "10")));

            try {

                // EXECUTE QUERY
                Response<SearchResult> res0 = jedisPipeline.ftSearch(indexName, q);
                jedisPipeline.sync();
                SearchResult searchResult = res0.get();

                // dialect 4 optimized to stop execution once return row count is reached
                if (queryDialect != 4) {
                    System.out.println("Number of results: " + searchResult.getTotalResults());
                }

                List<Document> docs = searchResult.getDocuments();

                // loop through the results
                for (Document doc : docs) {

                    JSONObject obj = null;

                    if (queryDialect == 4) {
                        JSONArray arrObj = new JSONArray((String) doc.get("$"));
                        obj = arrObj.getJSONObject(0);
                        obj.isEmpty();

                        //process object

                    } else {
                        obj = new JSONObject((String) doc.get("$"));
                        obj.isEmpty();

                        //process object 
                    }

                    // print the keys for each result object
                    System.out.print(doc.getId() + " |");
                }

            } catch (Exception e) {
                System.out.println("[AppSearch] ERROR in Query Execution, Please Try Again :");
                System.out.println(e.toString());
                continue;
            }
        }

        
        System.out.print("BATCH MODE: Enter number of queries : ");

        this.jedisPipeline.sync();
        int numQueries = Integer.parseInt(s.nextLine());
        runSprint(redisDataLoader, numQueries, indexName, indexFactory.getIndexObj(indexDefFile));
        s.close();
    }




    public void runSprint(RedisDataLoader redisDataLoader, int numQueries, String indexName, JsonArray indexSchema) {

        System.out.println("[AppSearch] Start Sprint ");

        if(!redisDataLoader.testPool()) {
            this.jedisPipeline = redisDataLoader.getJedisPipeline();
            this.jedisPooled = redisDataLoader.getJedisPooled();
        }
        
        ArrayList<String> tagFieldList = new ArrayList<String>();
        HashMap<String, ArrayList<String>> tagValueMap = new HashMap<String, ArrayList<String>>();

        // LOOP THROUGH THE SCHEMA AND EXTRACT ALL TAGS FIELDS AND THEIR DISTINCT VALUES
        for (int i = 0; i < indexSchema.size(); i++) {
            JsonObject fieldObj = indexSchema.getJsonObject(i);

            if ("TAG".equalsIgnoreCase(fieldObj.getString("type"))) {
                String fieldName = fieldObj.getString("alias");
                tagFieldList.add(fieldName);
                tagValueMap.put(fieldName, toArrayList(jedisPooled.ftTagVals(indexName, fieldName)));      
            }
        }

        System.out.println("[AppSearch] Executing " + numQueries + " Queries");
        long startTime = System.currentTimeMillis();

        ArrayList<String> queryList = new ArrayList<String>();
        ArrayList<Response<SearchResult>> resultList = new ArrayList<Response<SearchResult>>();

        // EXECUTE QUERIES

        for (int q = 0; q < numQueries; q++) {

            String queryStr = "";

            ArrayList<String> fieldList = pickRandom(tagFieldList);

            // BUILD QUERY STRING
            for (int f = 0; f < fieldList.size(); f++) {
                queryStr = queryStr + getFilterClause(fieldList.get(f), pickRandom(tagValueMap.get(fieldList.get(f))))
                        + " ";
            }

            queryList.add(queryStr);

            Query q1 = new Query(queryStr);
            q1.limit(0, 100);
            q1.dialect(4);
            q1.setNoContent();

            resultList.add(jedisPipeline.ftSearch(indexName, q1));

        }

        jedisPipeline.sync();

        long endTime = System.currentTimeMillis();

        // PRINT RESULTS

        for (int r = 0; r < resultList.size(); r++) {
            System.out.print("[" + (r + 1) + "] " + queryList.get(r) + "\n     [[RESULTS]] ");
            Response<SearchResult> res0 = resultList.get(r);
            SearchResult searchResult = res0.get();

            List<Document> docs = searchResult.getDocuments();

            int counter = 0;
            for (Document doc : docs) {
                System.out.print(doc.getId() + "|");

                if (counter++ == 2) {
                    break;
                }
            }

            System.out.println(" ...");
        }

        System.out.println("===================================================================================");
        System.out.println("[AppSearch] Query Execution Completed in " + getExecutionTime(startTime, endTime));

    }


    private static String printIndexSchema(JsonArray indexArray, String indexName) {

        String schemaString = "SCHEMA: " + indexName + "\n";
        HashMap<String, ArrayList<String>> fieldMap = new HashMap<String, ArrayList<String>>();

        for (int i = 0; i < indexArray.size(); i++) {
            JsonObject fieldObj = (JsonObject) indexArray.get(i);

            String fieldType = fieldObj.getString("type");
            String fieldName = fieldObj.getString("alias");

            if (fieldMap.get(fieldType) == null) {
                ArrayList<String> al = new ArrayList<String>();
                al.add(fieldName);
                fieldMap.put(fieldType, al);
            } else {
                fieldMap.get(fieldType).add(fieldName);
            }
        }

        for (String type : new String[] { "TEXT", "TAG", "NUMERIC" }) {
            ArrayList<String> fl = fieldMap.get(type);

            schemaString = schemaString + type + ": ";

            for (String field : fl) {
                schemaString = schemaString + field + " | ";
            }

            schemaString = schemaString + "\n";
        }

        return schemaString;
    }

    private static ArrayList<String> pickRandom(ArrayList<String> superList) {
        ArrayList<String> randomList = new ArrayList<String>();

        int rand = ThreadLocalRandom.current().nextInt(0, superList.size());

        int toggle = ThreadLocalRandom.current().nextInt(0, 3);

        if (toggle == 0) {
            randomList.add(superList.get(rand));
        } else if (toggle == 1) {
            for (int i = 0; i <= rand; i++) {
                randomList.add(superList.get(i));
            }
        } else {
            for (int i = rand; i < superList.size(); i++) {
                randomList.add(superList.get(i));
            }
        }

        return randomList;
    }

    private static ArrayList<String> toArrayList(Set<String> valueSet) {

        ArrayList<String> returnList = new ArrayList<String>();

        for (String val : valueSet) {
            returnList.add(val);
        }

        return returnList;

    }

    private static String getFilterClause(String fieldName, ArrayList<String> valueList) {
        String filter = "";

        filter = filter + "@" + fieldName + ":{";

        String valueStr = "";

        for (int v = 0; v < valueList.size(); v++) {
            valueStr = valueStr + valueList.get(v) + ((v < (valueList.size() - 1)) ? "|" : "");
        }

        filter = filter + valueStr + "}";

        return filter;
    }

    private static String getExecutionTime(long startTime, long endTime) {
        long diff = endTime - startTime;

        long sec = Math.floorDiv(diff, 1000l);
        long msec = diff % 1000l;

        return "" + sec + " s : " + msec + " ms";
    }

    
    public static void main(String[] args) throws Exception {

        // set the config file
        String configFile = "./config.properties";

        AppSearch appSearch = new AppSearch(configFile);
        appSearch.start();

        appSearch.redisDataLoader.close();

    }

}
