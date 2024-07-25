package com.jsd.jedis.cigna;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.json.JSONArray;
import org.json.JSONObject;

import com.jsd.jedis.RedisDataLoader;
import com.jsd.utils.*;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.search.IndexDefinition;
import redis.clients.jedis.search.IndexOptions;

import redis.clients.jedis.search.Schema;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.SearchResult;
import redis.clients.jedis.search.Document;

/**
 * Redis Search Client using Jedis
 *
 */
public class RedisSearch {

    public static void main(String[] args) throws Exception {


        Scanner s = new Scanner(System.in);

        System.out.println("Would you like to Reload Data ? (y/n)");

        String loadData = s.nextLine();

        RedisDataLoader redisDataLoader = new RedisDataLoader("./config.properties");
        Pipeline jedisPipeline = redisDataLoader.getJedisPipeline();

        if ("y".equalsIgnoreCase(loadData)) {

            CSVScanner subscriptions = new CSVScanner(
                    "C:/Users/Jay Datsur/OneDrive/Tech/Redis/DataSets/subscriptions.csv", ",", false);

            //drop the existing index
            try { 
                jedisPipeline.ftDropIndex("idx:healthcare");
                System.out.println("[RedisSearch] Dropped Index");
            }
            catch(Exception e){}
            
;
            //delete existing keys
            System.out.println("Deleting Existing Keys");
            System.out.println("Deleted " + redisDataLoader.deleteKeys("healthcare:subscriptions:") + " Keys");

            // load into JSON
            redisDataLoader.loadJSON("healthcare:subscriptions:", "header", "SubscriberUniqueID", "DependantUniqueID",
                    "dependants", subscriptions, 0);

            // Creating the index
            try {

                // define the schema
                Schema schema = new Schema()
                        .addTagField("$.SubscriberUniqueID").as("SubscriberUniqueID")
                        .addTagField("$.SubscriberSSN").as("SubscriberSSN")
                        .addTagField("$.dependants[*].DependantUniqueID").as("DependantUniqueID")
                        .addTagField("$.dependants[*].DependantSSN").as("DependantSSN");

                // set the options
                IndexDefinition def = new IndexDefinition(IndexDefinition.Type.JSON)
                        .setPrefixes(new String[] { "healthcare:subscriptions:" });

                // create the Index
                jedisPipeline.ftCreate("idx:healthcare", IndexOptions.defaultOptions().setDefinition(def), schema);
                jedisPipeline.sync();

                System.out.println("[RedisSearch] Created Index");

            } catch (Exception e) {
            }

        }

        // search for a patient

        while (true) {

            System.out.println("=======================================================");
            System.out.print("Enter Patient Identifier:");

            String patientID = s.nextLine();

            // escape special characters
            String patientID2 = patientID.replace("-", "\\-");

            if ("bye|quit".indexOf(patientID) > -1) {
                break;
            }

            String queryStr = "(@SubscriberUniqueID:{" + patientID2 + "})|(@SubscriberSSN:{" + patientID2
                    + "})|(@DependantSSN:{" + patientID2 + "})|(@DependantUniqueID:{" + patientID2 + "})";

            Query q = new Query(queryStr);
            q.dialect(2);
            q.limit(0, 10);

            Response<SearchResult> res0 = jedisPipeline.ftSearch("idx:healthcare", q);
            jedisPipeline.sync();
            SearchResult res = res0.get();

            System.out.println("Number of results: " + res.getTotalResults());
            List<Document> docs = res.getDocuments();

            // loop through the results
            for (Document doc : docs) {
                JSONObject obj = new JSONObject((String) doc.get("$"));

                System.out.println("Subscription: " + doc.getId() + " " + obj.getString("SubName"));

                // ************ */
                formatResult(patientID, obj);

            }
        }

        System.out.println("Run a Work Load of 500 queries ? y/n: ");

        if("y".equalsIgnoreCase(s.nextLine())) {
            runSprint(jedisPipeline);
        }



        s.close();
        redisDataLoader.close();

    }


    public static void  runSprint(Pipeline jedisPipeline) {
        System.out.println("Running a Work Load of 500 Queries");
        ArrayList<Response<SearchResult>> resultList = new ArrayList<Response<SearchResult>>();

        for (int i = 100; i < 500; i++) {
            String id = "DEP00" + i;

            String queryStr1 = "(@SubscriberUniqueID:{" + id + "})|(@SubscriberSSN:{" + id
                    + "})|(@DependantSSN:{" + id + "})|(@DependantUniqueID:{" + id + "})";

            Query q1 = new Query(queryStr1);
            q1.limit(0, 10);

            resultList.add(jedisPipeline.ftSearch("idx:healthcare", q1));
        }

        jedisPipeline.sync();

        System.out.println("Query Execution Complete");

        for( Response<SearchResult> res0 : resultList) {
            Document doc = res0.get().getDocuments().get(0);

            JSONObject obj = new JSONObject((String) doc.get("$"));
            System.out.print(obj.getString("SubName") + " " + obj.getString("SubscriberSSN") + "|");
           
        }
    }

    public static void formatResult(String patientID, JSONObject obj) {
        // check for sub or dependant
        if (patientID.equalsIgnoreCase(obj.getString("SubscriberSSN")) ||
                patientID.equalsIgnoreCase(obj.getString("SubscriberUniqueID"))) {
            System.out.println("This is a subscriber:");
            System.out.println(obj.getString("SubscriberUniqueID") + " | " + obj.getString("SubscriberSSN") + " | "
                    + obj.getString("SubName")
                    + " | " + obj.getString("SubscriberDOB") + " | " + obj.getString("SubscriberBalAmount1")
                    + " | " + obj.getString("SubscriberBalAmount2") + " | " + obj.getString("SubscriberBalAmount3")
                    + " | " + obj.getString("SubscriberBalAmount4"));
        } else {
            System.out.println("This is a dependant:");
            System.out.println(obj.getString("SubscriberUniqueID") + " | " + obj.getString("SubscriberSSN") + " | "
                    + obj.getString("SubName")
                    + " | " + obj.getString("SubscriberDOB") + " | " + obj.getString("SubscriberBalAmount1")
                    + " | " + obj.getString("SubscriberBalAmount2") + " | " + obj.getString("SubscriberBalAmount3")
                    + " | " + obj.getString("SubscriberBalAmount4"));

            JSONArray depArray = obj.getJSONArray("dependants");

            for (int d = 0; d < depArray.length(); d++) {
                JSONObject dep = (JSONObject) depArray.get(d);

                if (patientID.equalsIgnoreCase(dep.getString("DependantSSN"))
                        || patientID.equalsIgnoreCase(dep.getString("DependantUniqueID"))) {
                    System.out.println(dep.getString("DependantUniqueID") + " | " + dep.getString("DependantSSN")
                            + " | " + dep.getString("Dependant Name")
                            + " | " + dep.getString("DependantDOB") + " | " + dep.getString("DependantBalAmount1")
                            + " | " + dep.getString("DependantBalAmount2") + " | "
                            + dep.getString("DependantBalAmount3")
                            + " | " + dep.getString("DependantBalAmount4"));

                    break;
                }
            }
        }
    }

}
