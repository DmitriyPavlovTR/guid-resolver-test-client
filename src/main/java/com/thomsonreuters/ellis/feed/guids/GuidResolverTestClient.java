package com.thomsonreuters.ellis.feed.guids;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

class GuidResolverTestClient {

  public static final String HOST =
      "http://localhost:8030/";
      //"http://ellis-dev.int.thomsonreuters.com:8030/";
      //"http://solt-preprod.int.thomsonreuters.com:8030/";

  public static void main(String[] args) throws IOException, InterruptedException {
    final String path = "findGuids";

    final List<String> contexts =
        Arrays.asList(
            "eu/doc/legislation/fulltext/cellar/3b729ddf-f1f7-11e3-8cd4-01aa75ed71a1%art1" ,
            "eu/docfamily/legislation/cellar/83aea4a3-6bff-11e3-9afb-01aa75ed71a1%art20",
            "eu/docfamily/legislation/cellar/83aea4a3-6bff-11e3-9afb-01aa75ed71a1%art21",
            "eu/docfamily/legislation/cellar/50721be0-fa8f-4ae9-83b3-645e8e35f738%art15",
            "eu/doc/legislation/fulltext/cellar/3b729ddf-f1f7-11e3-8cd4-01aa75ed71a1%art55 ");
    final String content = new Gson().toJson(contexts);

    // ["eu/doc/legislation/fulltext/cellar/3b729ddf-f1f7-11e3-8cd4-01aa75ed71a1%art1"]

    runSimilarRequestMultithreadedTest(path, contexts, content, 100);
  }
  public static void mainResolveBatch(String[] args) throws IOException, InterruptedException {
    final String path = "resolveBatch";

    final List<String> contexts =
        Arrays.asList("eu/doc/legislation/binary/f74bc3e6-4ae3-11e9-a8ed-01aa75ed71a1:BUL:0",
            "eu/doc/legislation/binary/9797a8dc-efdf-4d2e-a25b-9d07a8b629bb:POR:0",
            "eu/doc/legislation/binary/6b3e4af4-9e53-11ea-9d2d-01aa75ed71a1:FRA:0",
            "eu/docfamily/legislation/cellar/3b729ddf-f1f7-11e3-8cd4-01aa75ed71a1:pdfs:HUN",
            "eu/doc/legislation/binary/9ea0fc6c-18c2-11e4-933d-01aa75ed71a1:DEU:0",
            "eu/docfamily/legislation/cellar/238c44f4-cd1e-4a44-b389-14312dff4352:pdfs:LIT",
            "eu/doc/legislation/binary/b604bdc9-75db-44de-8258-b895f2adfff2:SWE:0",
            "eu/docfamily/legislation/cellar/83aea4a3-6bff-11e3-9afb-01aa75ed71a1:parent:@EU-ENACTING-TERMS:titleII:cII:sI:art20",
            "eu/docfamily/legislation/cellar/83aea4a3-6bff-11e3-9afb-01aa75ed71a1:parent:@EU-ENACTING-TERMS:titleII:cII:sI:art21",
            "eu/docfamily/legislation/cellar/a7c47846-2e80-11e4-8c3c-01aa75ed71a1:parent:@EU-ANNEXES:annII:annpartB:annsubpartIII",
            "eu/doc/legislation/fulltext/cellar/341c71ca-001e-4c83-8f69-d754b009792c:parent:@EU-ANNEXES:annVIII:annpartD:annsubpart1:annsubsubpartSPO_IDE_A_145:para(e)",
            "eu/doc/legislation/fulltext/cellar/fc1050ce-b0ee-11e4-b5b2-01aa75ed71a1:parent:@EU-ENACTING-TERMS",
            "eu/doc/legislation/fulltext/cellar/31948c15-64be-11e4-9cbe-01aa75ed71a1:parent:@EU-ANNEXES:annI:unp001",
            "eu/doc/legislation/binary/49b2fec9-1bde-4865-b3cb-3cac22a6160e:NLD:0",
            "eu/doc/legislation/binary/c300360f-808f-4104-af32-c65b736a3595:ENG:0",
            "eu/doc/legislation/binary/c300360f-808f-4104-af32-c65b736a3595:ITA:0",
            "eu/doc/legislation/binary/e28f07bb-5705-45ad-8a46-a1d6a3bb1b52:DEU:0",
            "eu/doc/legislation/fulltext/cellar/b496e904-1a4a-45d4-8236-9f360d0946a2:@DOCDETAILS",
            "eu/docfamily/legislation/cellar/bd598921-4785-4f80-9dcb-1c2039e5cd5c:parent:@EU-ANNEXES:annIII:annpart1:unp005");
    final String content = new Gson().toJson(contexts);

   // ["eu/doc/legislation/fulltext/cellar/3b729ddf-f1f7-11e3-8cd4-01aa75ed71a1%art1"]

    runSimilarRequestMultithreadedTest(path, contexts, content, 1000);
  }

  private static void runSimilarRequestMultithreadedTest(String path, List<String> contexts,
                                                         String content, int totalCnt)
      throws InterruptedException {
    final Stopwatch started = Stopwatch.createStarted();

    final LongAdder processedReq = new LongAdder();
    final Thread logThread = new Thread(() -> {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        printSpeed(started, processedReq.intValue(), contexts.size());
      }
    });
    logThread.setDaemon(true);
    logThread.start();

    final ExecutorService executorService = Executors.newFixedThreadPool(10);
    List<Future<?>> futures = new ArrayList<>();

    final int threads = 10;
    for (int t = 0; t < threads; t++) {
      final int cnt = totalCnt / threads;
      futures.add(executorService.submit(() -> {
        for (int i = 0; i < cnt; i++) {
          final String s = sendPostJsonRequest(path, content);
          if (i == 3) {
            System.out.println(Thread.currentThread().getName() + " Results:" + s);
          }
          processedReq.add(1);
        }
        return cnt;
      }));
    }

    futures.forEach(GuidResolverTestClient::safeGet);

    executorService.shutdown();
    executorService.awaitTermination(10, TimeUnit.SECONDS);

    logThread.interrupt();
  }

  private static <T> T safeGet(Future<T> next) {
    try {
     return next.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  private static String sendPostJsonRequest(String path, String content) throws IOException {
    return sendPostRequest(path, content.getBytes(StandardCharsets.UTF_8),
        "application/json;charset=UTF-8");
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
        printSpeed(started, i + 1, 1);
      }
    }

    printSpeed(started, cnt, 1);
  }

  private static void printSpeed(Stopwatch started, int cnt, int size) {
    final long msElapsed = started.elapsed().toMillis();
    final double tps = cnt * 1000.0 / msElapsed;
    final double ctxps = tps * size;
    System.out.println("N=" + cnt + ", Required " + msElapsed + " millis: " +
        String.format("%.2f", tps) + " req/s, " +
        String.format("%.2f", ctxps) + " ctx/s at " + HOST);
  }

  private static String sendPostRequest(String path, Map<String, Object> params) throws IOException {

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

  private static String sendPostRequest(String path, byte[] postDataBytes,
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
    //System.out.println("POST Response Code :: " + responseCode);

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
      return response.toString();
    } else {
      System.err.println("POST request not worked");
    }
    return null;
  }
}