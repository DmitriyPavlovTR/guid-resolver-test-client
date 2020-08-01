import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

class Test {
  public static void main(String[] args) throws IOException {
    URL obj = new URL("http://localhost:8030/resolve?context=eu/doc/legislation/binary/f74bc3e6-4ae3-11e9-a8ed-01aa75ed71a1:BUL:0");

    HttpURLConnection con = (HttpURLConnection) obj.openConnection();
    con.setRequestMethod("POST");
    con.setRequestProperty("User-Agent", "GuidResolver stress test client");

    // For POST only - START
    con.setDoOutput(true);
    OutputStream os = con.getOutputStream();
    //todo os.write(POST_PARAMS.getBytes());
    os.flush();
    os.close();
    // For POST only - END

    int responseCode = con.getResponseCode();
    System.out.println("POST Response Code :: " + responseCode);

    if (responseCode == HttpURLConnection.HTTP_OK) { //success
      BufferedReader in = new BufferedReader(new InputStreamReader(
          con.getInputStream()));
      String inputLine;
      StringBuffer response = new StringBuffer();

      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();

      // print result
      System.out.println(response.toString());
    } else {
      System.out.println("POST request not worked");
    }
  }
}