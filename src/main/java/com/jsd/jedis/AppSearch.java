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
import redis.clients.jedis.search.aggr.AggregationBuilder;
import redis.clients.jedis.search.aggr.AggregationResult;
import redis.clients.jedis.search.aggr.Reducers;
import redis.clients.jedis.search.aggr.Row;
import redis.clients.jedis.search.aggr.SortedField;
import redis.clients.jedis.search.Document;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Redis Search Client using Jedis
 *
 */
public class AppSearch {

    public static void main(String[] args) throws Exception {

        Scanner s = new Scanner(System.in);

        // set the config file
        String configFile = "./config.properties";

        System.out.print("\nEnter the config file path (Defaults to ./config.properties): ");
        String configFile1 = s.nextLine();

        if (!"".equals(configFile1)) {
            configFile = configFile1;
        }

        Properties config = new Properties();
        config.load(new FileInputStream(configFile));

        RedisDataLoader redisDataLoader = new RedisDataLoader(configFile);
        Pipeline jedisPipeline = redisDataLoader.getJedisPipeline();

        String indexName = config.getProperty("index.name");
        String indexDefFile = config.getProperty("index.def.file");

        String searchMode = config.getProperty("search.mode", "search");
        String searchDisplay = config.getProperty("search.display", "key");

        RedisIndexFactory indexFactory = new RedisIndexFactory(configFile);

        System.out.print("\nWould you like to Reload Data for: " + indexName + " ? (y/n) ");

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

        } else {
            System.out.print("\nWould you like to Create the index: " + indexName + " ? (y/n) ");
            String createIndex = s.nextLine();

            if ("y".equalsIgnoreCase(createIndex)) {
                indexFactory.dropIndex(indexName);
                indexFactory.createIndex(indexName, indexDefFile);
            }
        }

        // PRINT INDEX SCHEMA FOR REF
        System.out.println("\n" + printIndexSchema(indexFactory.getIndexObj(indexDefFile), indexName) + "\n");

        // START SEARCHING

        String prevQueryStr = "";
        String queryStr = "";
        int resultCursor = 0;
        int recordLimit = Integer.parseInt(config.getProperty("query.record.limit", "10"));

        String returnString = "";
        String sortByString = "";
        String prevReturnString = "";
        String prevSortByString = "";

        String searchSortField = "";

        String burstMode = config.getProperty("burst.mode", "n");
        int burstCount = 0;
        int maxBurst = Integer.parseInt(config.getProperty("burst.count", "5"));
        int burstInterval = 1000;
        int burstMarker = Integer.parseInt(config.getProperty("burst.start.sec", "20"));

        if ("search".equalsIgnoreCase(searchMode)) {
            // get sort field
            System.out.print("Sort Field : ");
            searchSortField = s.nextLine();
        }

        while (true) {

            System.out.println("\n[----------------------------------------------------------------------------]");

            if (burstCount == maxBurst) {
                burstCount = 0;
            }

            if ("n".equalsIgnoreCase(burstMode) || (burstCount == 0)) {
                System.out.println("Enter Search String :");
                queryStr = s.nextLine();
            }

            if ("y".equalsIgnoreCase(burstMode)) {
                if (burstCount == 0) {
                    waitTillSec(burstMarker);
                } else {
                    burstInterval = Integer.parseInt(config.getProperty("burst.interval", "1000"));
                    Thread.sleep(burstInterval);
                }
            }

            burstCount++;

            if ("bye|quit".indexOf(queryStr) > -1) {
                break;
            }

            try {

                if ("y".equalsIgnoreCase(burstMode)) {
                    System.out.println("Time: " + getCurrentTime() + " ");
                }

                // TOP N QUERY
                if (queryStr.startsWith("aggr")) {
                    executeTopNQuery(queryStr, indexName, jedisPipeline);
                    continue;
                }

                int queryDialect = Integer.parseInt(config.getProperty("query.dialect", "1"));

                String queryStrExec = "";

                if ("next".equalsIgnoreCase(queryStr)) {
                    queryStrExec = prevQueryStr;

                    returnString = prevReturnString;
                    sortByString = prevSortByString;
                    resultCursor = resultCursor + recordLimit;

                } else {
                    queryStrExec = queryStr;
                    prevQueryStr = queryStr;
                    resultCursor = 0;
                }

                // FT.AGGREGATE QUERY
                if ("aggr".equalsIgnoreCase(searchMode)) {

                    if (!"next".equalsIgnoreCase(queryStr)) {
                        System.out.print("Return Fields (field1,field2,field3...) : ");
                        returnString = s.nextLine();

                        System.out.print("Sort By (field1,field2,flied3.. ASC|DESC): ");
                        sortByString = s.nextLine();
                    }

                    executeAggrQuery(queryStrExec, indexName, jedisPipeline, s, resultCursor, returnString,
                            sortByString);

                    prevReturnString = returnString;
                    prevSortByString = sortByString;

                    continue;
                }

                // FT.SEARCH QUERY
                Query q = new Query(queryStrExec);
                q.dialect(queryDialect);
                q.limit(resultCursor, recordLimit);

                if (!"".equals(searchSortField)) {
                    q.setSortBy(searchSortField, true);
                }

                // EXECUTE QUERY
                Response<SearchResult> res0 = jedisPipeline.ftSearch(indexName, q);
                jedisPipeline.sync();
                SearchResult searchResult = res0.get();

                // getTotalResults() not to be used for dialect 4 as it stops execution once the
                // return row count is reached (for optimization)
                if (queryDialect != 4) {
                    System.out.println("Number of results: " + searchResult.getTotalResults());
                }

                List<Document> docs = searchResult.getDocuments();

                // loop through the results
                for (Document doc : docs) {

                    // print the keys for each result object
                    if ("key".equalsIgnoreCase(searchDisplay)) {
                        System.out.print(doc.getId() + " |");
                    }

                    JSONObject obj = null;

                    // Dialect 4 Returns a different Document Structure
                    if (queryDialect == 4) {
                        JSONArray arrObj = new JSONArray((String) doc.get("$"));
                        obj = arrObj.getJSONObject(0);
                        obj.isEmpty();

                    } else {
                        obj = new JSONObject((String) doc.get("$"));

                        if ("value".equalsIgnoreCase(searchDisplay)) {
                            System.out.println(obj);
                        }

                        obj.isEmpty();
                    }

                }

            } catch (Exception e) {
                System.out.println("[AppSearch] ERROR in Query Execution, Please Try Again :");
                System.out.println(e.toString());
                continue;
            }
        }

        System.out.print("BATCH MODE: Enter number of queries : ");

        int numQueries = Integer.parseInt(s.nextLine());

        runSprint(redisDataLoader, numQueries, indexName, indexFactory.getIndexObj(indexDefFile));

        s.close();
        redisDataLoader.close();

    }

    public static void executeAggrQuery(String queryString, String indexName, Pipeline jedisPipeline, Scanner s,
            int resultCursor, String returnString, String sortByString) throws Exception {

        AggregationBuilder aggr = new AggregationBuilder(queryString);

        // RETURN FIELDS
        aggr.load(returnString.split(","));

        // SORTBY FIELDS
        if (!sortByString.endsWith("ASC") && !sortByString.endsWith("DESC")) {
            sortByString = sortByString + " ASC";
        }

        // SORT FIELDS
        String[] sortBy = sortByString.split(" ");
        String fieldsPart = sortBy[0];
        String sortOrder = sortBy[1];

        String[] fields = fieldsPart.split(",");
        SortedField[] sortedFields = new SortedField[fields.length];

        for (int i = 0; i < fields.length; i++) {
            if (sortOrder.equalsIgnoreCase("ASC")) {
                sortedFields[i] = SortedField.asc("@" + fields[i]);
            } else if (sortOrder.equalsIgnoreCase("DESC")) {
                sortedFields[i] = SortedField.desc("@" + fields[i]);
            }
        }

        aggr.sortBy(sortedFields);
        aggr.limit(resultCursor, 5);

        Response<AggregationResult> response = jedisPipeline.ftAggregate(indexName, aggr);
        jedisPipeline.sync();

        AggregationResult result = response.get();

        // System.out.println("Num Records: " + result.getTotalResults());

        List<Row> rows = result.getRows();

        for (Row row : rows) {
            System.out.println(row.toString());
        }
    }

    public static void executeTopNQuery(String queryStr, String indexName, Pipeline jedisPipeline) throws Exception {

        // e.g
        // aggr Top 5 Customer by count
        // aggr Top 3 Customer by sum Amount where @Product:{apple|orange}
        // aggr show Top 5 Agent by Avg TalkTime

        String queryStr1 = queryStr;

        String filterString = "*";

        if (queryStr1.lastIndexOf(" where ") > -1) {
            filterString = queryStr1.substring(queryStr1.lastIndexOf(" where ") + 7);
            queryStr1 = queryStr1.substring(0, queryStr1.lastIndexOf(" where "));
        }

        AggregationBuilder aggr = new AggregationBuilder(filterString);

        String regex = "\\w+\\s+(Top \\d+)\\s+(\\w+)\\s+by\\s+((avg|sum|count|)\\s*\\w*)";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(queryStr1);

        if (matcher.find()) {
            String groupCol = matcher.group(2);
            String aggrCol = matcher.group(3);

            if ("count".equalsIgnoreCase(aggrCol)) {
                aggrCol = "Count";
                aggr.groupBy("@" + groupCol, Reducers.count().as("Count"));

            } else if (aggrCol.toUpperCase().startsWith("AVG")) {

                aggrCol = aggrCol.substring(4).trim();
                aggr.groupBy("@" + groupCol, Reducers.avg("@" + aggrCol).as(aggrCol));
            } else if (aggrCol.toUpperCase().startsWith("SUM")) {

                aggrCol = aggrCol.substring(4).trim();
                aggr.groupBy("@" + groupCol, Reducers.sum("@" + aggrCol).as(aggrCol));
            } else {
                aggr.groupBy("@" + groupCol, Reducers.sum("@" + aggrCol).as(aggrCol));

            }

            aggr.sortBy(SortedField.desc("@" + aggrCol));
            aggr.limit(0, Integer.parseInt(matcher.group(1).substring(4)));

            Response<AggregationResult> response = jedisPipeline.ftAggregate(indexName, aggr);
            jedisPipeline.sync();

            AggregationResult result = response.get();

            List<Row> rows = result.getRows();

            for (Row row : rows) {
                System.out.println(row.toString());
            }

        }

    }

    public static String printIndexSchema(JsonArray indexArray, String indexName) {

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

            if (fl == null) {
                continue;
            }

            schemaString = schemaString + type + ": ";

            for (String field : fl) {
                schemaString = schemaString + field + " | ";
            }

            schemaString = schemaString + "\n";
        }

        return schemaString;
    }

    public static void runSprint(RedisDataLoader redisDataLoader, int numQueries, String indexName,
            JsonArray indexSchema) {

        JedisPooled jedisPooled = redisDataLoader.getJedisPooled();
        Pipeline jedisPipeline = redisDataLoader.getJedisPipeline();

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

    private static ArrayList<String> pickRandom(ArrayList<String> superList) {
        ArrayList<String> randomList = new ArrayList<String>();

        int rand = ThreadLocalRandom.current().nextInt(0, superList.size());

        int toggle = ThreadLocalRandom.current().nextInt(0, 3);

        int maxSize = 5;
        int listSize = 0;

        if (toggle == 0) {
            randomList.add(superList.get(rand));
        } else if (toggle == 1) {
            for (int i = 0; i <= rand; i++) {
                randomList.add(superList.get(i));

                if (++listSize > maxSize) {
                    break;
                }
            }
        } else {
            for (int i = rand; i < superList.size(); i++) {
                randomList.add(superList.get(i));

                if (++listSize > maxSize) {
                    break;
                }
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

    static String getExecutionTime(long startTime, long endTime) {
        long diff = endTime - startTime;

        long sec = Math.floorDiv(diff, 1000l);
        long msec = diff % 1000l;

        return "" + sec + " s : " + msec + " ms";
    }

    static String getCurrentTime() {
        // Get the current time
        LocalTime now = LocalTime.now();

        // Format the time to mm:ss.SSS
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("mm:ss.SSS");
        String formattedTime = now.format(formatter);

        // Print the formatted time
        return formattedTime;
    }

    static void waitTillSec(int sec) {
        while (true) {
            LocalTime now = LocalTime.now();
            if (now.getSecond() % sec == 0) {
                break;
            }
        }

        return;
    }
}
