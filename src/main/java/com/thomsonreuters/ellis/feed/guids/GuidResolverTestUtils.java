package com.thomsonreuters.ellis.feed.guids;

import com.google.common.base.Throwables;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class GuidResolverTestUtils {
  static Thread startPeriodicAction(Runnable runnable, int millis) {
    final Thread logThread = new Thread(() -> {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Thread.sleep(millis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        runnable.run();
      }
    });
    logThread.setDaemon(true);
    logThread.start();
    return logThread;
  }

  static String sendPostRequest(String host,
                                String path,
                                byte[] postDataBytes,
                                String contentType) throws IOException {
    URL url = new URL(host + path);
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
      System.err.println("POST request not worked: " + responseCode);
    }
    return null;
  }

  static <T> T getNoThrows(Future<T> next) {
    try {
     return next.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  static String sendRequestWithJsonBody(String host, String path, String content)
      throws IOException {
    return sendPostRequest(host, path,
        content.getBytes(StandardCharsets.UTF_8), "application/json;charset=UTF-8");
  }
}
