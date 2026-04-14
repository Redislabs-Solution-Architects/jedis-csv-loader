package com.jsd.utils;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;

import org.json.JSONArray;
import org.json.JSONObject;

import com.jsd.jedis.AppPubSub;

public class DBLoader {

    private String configFile;
    private Properties config;

    private Connection conn;

    private RandomDataGenerator gen;

    public DBLoader(String configFile) throws Exception {
        this.configFile = configFile;
        config = new Properties();
        config.load(new FileInputStream(configFile));
        gen = new RandomDataGenerator(config.getProperty("data.template.file"));
        createConnection();
    }

    private void createConnection() throws Exception {
        Class.forName(config.getProperty("db.driver.name"));
        String url = config.getProperty("db.url");
        String user = config.getProperty("db.user");
        String pwd = config.getProperty("db.password");
        conn = DriverManager.getConnection(url, user, pwd);
        conn.setAutoCommit(false);
    }

    public void createTables() {

        System.out.println("[DBLoader] Creating Tables:");
        String[] tables = { "header.table", "detail.table" };

        try {

            Statement stmt = conn.createStatement();

            for (String t : tables) {
                String tableSQL = config.getProperty(t, "NA");

                try {
                    if (!"NA".equalsIgnoreCase(tableSQL)) {
                        stmt.executeUpdate(tableSQL);
                        System.out.println("[DBLoader] Created " + t + " Table:");
                    }

                } catch (SQLException se) {
                    if (955 == se.getErrorCode()) {
                        System.err.println("[DBLoader] " + t + " Already Exists");
                    }
                }
            }

        } catch (SQLException se) {
            System.err.println("[DBLoader] Error Creating Tables:\n" + se);
        }

    }

    public void truncateTables() {
        System.out.println("[DBLoader] Truncating Tables:");
        String[] tables = { "detail.table.trunc", "header.table.trunc"  };

        try {

            Statement stmt = conn.createStatement();

            for (String t : tables) {
                String tableSQL = config.getProperty(t, "NA");

                try {
                    if (!"NA".equalsIgnoreCase(tableSQL)) {
                        stmt.executeUpdate(tableSQL);
                        System.out.println("[DBLoader] Truncated " + t + " Table:");
                        conn.commit();
                    }

                } catch (SQLException se) {

                }
            }

        } catch (SQLException se) {
            System.err.println("[DBLoader] Error Truncating Tables:\n" + se);
        }
    }

    private void insertRow(JSONObject record, String colNamesStr, String colTypesStr, PreparedStatement stmt)
            throws Exception {
        String[] colTypes = colTypesStr.split(",");
        String[] colNames = colNamesStr.split(",");

        for (int c = 0; c < colTypes.length; c++) {
            if ("i".equalsIgnoreCase(colTypes[c])) {
                stmt.setInt(c + 1, record.getInt(colNames[c]));
            } else if ("f".equalsIgnoreCase(colTypes[c])) {
                stmt.setFloat(c + 1, record.getFloat(colNames[c]));
            } else {
                stmt.setString(c + 1, record.getString(colNames[c]));
            }
        }

        stmt.executeUpdate();
    }

    public void loadData() throws Exception {

        System.out.println("[DBLoader] Loading Data:");

        JSONObject record = null;

        String headerUID = config.getProperty("header.json.uid");
        String detailArr = config.getProperty("detail.json.arr", "NA");

        String headerCols = config.getProperty("header.json.col.names");
        String detailCols = headerUID + "," + config.getProperty("detail.json.col.names", "NA");

        String headerColTypes = config.getProperty("header.json.col.types");
        String detailColTypes = headerColTypes.substring(0, 1) + ","
                + config.getProperty("detail.json.col.types", "NA");

        PreparedStatement headerInsertStmt = conn.prepareStatement(config.getProperty("header.insert"));
        PreparedStatement detailInsertStmt = null;

        if (!"NA".equalsIgnoreCase(config.getProperty("detail.insert", "NA"))) {
            detailInsertStmt = conn.prepareStatement(config.getProperty("detail.insert", "NA"));
        }

        int batchSize = Integer.parseInt(config.getProperty("batch.size", "500"));

        //KEYSPACE NOTIFICATIONS via PUB-SUB
        if("true".equalsIgnoreCase(config.getProperty("track.keys", "false"))) {
            AppPubSub keyTracker = new AppPubSub(this.configFile);
            keyTracker.listenKeySpace(config.getProperty("key.prefix", "") + "*");
            keyTracker.trackKeys(batchSize, Boolean.parseBoolean(config.getProperty("track.continuous", "false")));
        }


        int headerCount = 0;
        int detailCount = 0;

        for (int i = 0; i < batchSize; i++) {
            record = gen.generateRecord("header");
            headerCount++;

            // insert the header record first
            insertRow(record, headerCols, headerColTypes, headerInsertStmt);

            // insert the detail records
            if (!"NA".equalsIgnoreCase(detailArr)) {
                JSONArray detailRecords = record.getJSONArray(detailArr);

                for (int d = 0; d < detailRecords.length(); d++) {
                    JSONObject detailRecord = detailRecords.getJSONObject(d);

                    // add the pk of the header table
                    detailRecord.put(headerUID, record.getString(headerUID));

                    insertRow(detailRecord, detailCols, detailColTypes, detailInsertStmt);
                    detailCount++;
                }
            }

            if (i % 5 == 0) {
                conn.commit();
            }

        }

        conn.commit();
        conn.close();

        System.out.println("[DBLoder] Inserted " + headerCount + " Header Rows");
        System.out.println("[DBLoder] Inserted " + detailCount + " Detail Rows");
    }

    public static void main(String[] args) throws Exception {
        DBLoader dbLoader = new DBLoader("./jdbc-loader-config-ora.properties");

        Scanner scanner = new Scanner(System.in);

        System.err.print("\nChoose Option:\n[1] Delete Tables\n[2] Load Data\nSelect: ");

        String option = scanner.nextLine();

        if ("1".equals(option)) {
            dbLoader.truncateTables();
        }
        else {
            dbLoader.createTables();
            dbLoader.truncateTables();
            dbLoader.loadData();
        }

        scanner.close();

    }
}
