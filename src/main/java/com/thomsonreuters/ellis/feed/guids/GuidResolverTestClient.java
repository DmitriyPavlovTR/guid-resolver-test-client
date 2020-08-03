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

  public static final String HOST =
      //"http://localhost:8030/";
      //"http://ellis-dev.int.thomsonreuters.com:8030/";
      "http://solt-preprod.int.thomsonreuters.com:8030/";

  public static void main(String[] args) throws IOException {
    final String path = "resolveBatch";

    byte[] content="[\"eu/doc/legislation/binary/f74bc3e6-4ae3-11e9-a8ed-01aa75ed71a1:BUL:0\", \"eu/doc/legislation/binary/9797a8dc-efdf-4d2e-a25b-9d07a8b629bb:POR:0\",\"eu/doc/legislation/binary/6b3e4af4-9e53-11ea-9d2d-01aa75ed71a1:FRA:0\"]\n".getBytes();
    final Stopwatch started = Stopwatch.createStarted();
    final int cnt = 1000;
    for (int i = 0; i < cnt; i++) {
      sendPostRequest(path, content, "application/json;charset=UTF-8");

      if (i % 4 == 0) {
        printSpeed(started, i + 1);
      }
    }

    printSpeed(started, cnt);
  }

  public static void mainResolve(String[] args) throws IOException {
    final String path = "resolve";

    Map<String,Object> params = new LinkedHashMap<>();
    params.put("context", "eu/doc/legislation/binary/f74bc3e6-4ae3-11e9-a8ed-01aa75ed71a1:BUL:0");

    final Stopwatch started = Stopwatch.createStarted();
    final int cnt = 1000;
    for (int i = 0; i < cnt; i++) {
      sendPostRequest(path, params);

      if (i % 4 == 0) {
        printSpeed(started, i + 1);
      }
    }

    printSpeed(started, cnt);
  }

  private static void printSpeed(Stopwatch started, int cnt) {
    final long msElapsed = started.elapsed().toMillis();
    final double tps = cnt * 1000.0 / msElapsed;
    System.out.println("N=" + cnt + ", Required " + msElapsed + " millis: " +
        String.format("%.2f", tps) + " tps at " + HOST);
  }

  private static StringBuilder sendPostRequest(String path, Map<String, Object> params) throws IOException {

    StringBuilder postData = new StringBuilder();
    for (Map.Entry<String,Object> param : params.entrySet()) {
      if (postData.length() != 0) postData.append('&');
      postData.append(URLEncoder.encode(param.getKey(), "UTF-8"));
      postData.append('=');
      postData.append(URLEncoder.encode(String.valueOf(param.getValue()), "UTF-8"));
    }
    byte[] postDataBytes = postData.toString().getBytes(StandardCharsets.UTF_8);

    return sendPostRequest(path, postDataBytes, "application/x-www-form-urlencoded");
  }

  private static StringBuilder sendPostRequest(String path, byte[] postDataBytes,
                                               String contentType) throws IOException {
    URL url = new URL(HOST +  path);
    HttpURLConnection conn = (HttpURLConnection)url.openConnection();

    conn.setRequestProperty("User-Agent", "GuidResolver stress test client");

    // For POST only - START
    conn.setDoOutput(true);
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", contentType);
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