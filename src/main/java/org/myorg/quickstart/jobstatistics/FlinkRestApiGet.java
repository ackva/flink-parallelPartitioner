package org.myorg.quickstart.jobstatistics;

import java.io.*;
import java.net.*;

/**
 * Inspired by StackOverflow
 */

public class FlinkRestApiGet {
    private String flinkUrl;

    // Constructor with default localhost URL
    public FlinkRestApiGet() {
        this.flinkUrl = "http://localhost:8081";
    }

    // Constructor with custom Flink URL
    public FlinkRestApiGet(String url) {
        this.flinkUrl = url;
    }

    // very generic - @param: entire URL required
    public String getHTML(String urlToRead) throws Exception {
        StringBuilder result = new StringBuilder();
        URL url = new URL(urlToRead);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        rd.close();
        return result.toString();
    }

    // very generic - @param: entire URL required
    public String getJobHTML(String jobId) throws Exception {
        StringBuilder result = new StringBuilder();
        URL url = new URL(flinkUrl + "/jobs/" + jobId);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        rd.close();
        return result.toString();
    }

    public String getJobConfigHTML(String jobId) throws Exception {
        StringBuilder result = new StringBuilder();
        URL url = new URL(flinkUrl + "/jobs/" + jobId + "/config");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        rd.close();
        return result.toString();
    }

    public String getVertexHTML(String jobId, String jobVertex) throws Exception {
        StringBuilder result = new StringBuilder();
        URL url = new URL(flinkUrl + "/jobs/" + jobId + "/vertices/" + jobVertex);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        rd.close();
        return result.toString();
    }



}