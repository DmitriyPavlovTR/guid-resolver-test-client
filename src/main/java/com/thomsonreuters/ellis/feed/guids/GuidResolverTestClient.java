package com.thomsonreuters.ellis.feed.guids;

import com.google.common.base.Stopwatch;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

class GuidResolverTestClient {

  public static final String HOST = "http://localhost:8030/";

  public static void main(String[] args) throws IOException {
    final String path = "resolve";

    Map<String,Object> params = new LinkedHashMap<>();
    params.put("context", "eu/doc/legislation/binary/f74bc3e6-4ae3-11e9-a8ed-01aa75ed71a1:BUL:0");

    final Stopwatch started = Stopwatch.createStarted();
    final int cnt = 1000;
    for (int i = 0; i < cnt; i++) {
      sendPostRequest(path, params);

      if (i % 4 == 0) {
        printSpeed(started, i);
      }
    }

    printSpeed(started, cnt);
  }

  private static void printSpeed(Stopwatch started, int cnt) {
    final long msElapsed = started.elapsed().toMillis();
    final double tps = cnt * 1000.0 / msElapsed;
    System.out.println("N=" + cnt + ", Required " + msElapsed + " millis: " +
        String.format("%.2f", tps) + " tps");
  }

  private static StringBuilder sendPostRequest(String path, Map<String, Object> params) throws IOException {
    URL url = new URL(HOST +  path);
    StringBuilder postData = new StringBuilder();
    for (Map.Entry<String,Object> param : params.entrySet()) {
      if (postData.length() != 0) postData.append('&');
      postData.append(URLEncoder.encode(param.getKey(), "UTF-8"));
      postData.append('=');
      postData.append(URLEncoder.encode(String.valueOf(param.getValue()), "UTF-8"));
    }
    byte[] postDataBytes = postData.toString().getBytes(StandardCharsets.UTF_8);

    HttpURLConnection conn = (HttpURLConnection)url.openConnection();

    conn.setRequestProperty("User-Agent", "GuidResolver stress test client");

    // For POST only - START
    conn.setDoOutput(true);
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    conn.setRequestProperty("Content-Length", String.valueOf(postDataBytes.length));

    conn.setDoOutput(true);
    final OutputStream os = conn.getOutputStream();
    os.write(postDataBytes);
    os.flush();
    os.close();
    // For POST only - END

    int responseCode = conn.getResponseCode();
    System.out.println("POST Response Code :: " + responseCode);

    if (responseCode == HttpURLConnection.HTTP_OK) { //success
      BufferedReader in = new BufferedReader(new InputStreamReader(
          conn.getInputStream()));
      String inputLine;
      StringBuilder response = new StringBuilder();

      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();

      // print result
      return response;
    } else {
      System.err.println("POST request not worked");
    }
    return null;
  }
}