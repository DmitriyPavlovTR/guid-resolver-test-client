package com.thomsonreuters.ellis.feed.guids;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;
import java.util.TreeSet;
import java.util.stream.Stream;

public class GuidResolverCsvConverter {
  public static void main(String[] args) throws IOException {
    final String home = "."; // System.getProperty("user.home");
    final String inputCsv = "output.txt"; //"Documents\\TR_Uk_Legislative\\preprod_ctx.csv";

    final String separator = ",";
    int placeZeroBased = 0; // only last place is supported now
    final BufferedWriter writer = new BufferedWriter(new FileWriter(".\\preprod_contexts.txt"));

    final TreeSet<String> set = new TreeSet<>();
    final Stream<String> lines = Files.lines(Path.of(home, inputCsv));
    lines.map(line -> {
          String lineResult = line;
          for (int i = 0; i < placeZeroBased; i++) {
            final int idx = lineResult.indexOf(separator);
            if (idx == -1 & lineResult.length() > idx + 1) {
              return null;
            }
            lineResult = lineResult.substring(idx + 1).trim();
          }
          return lineResult;
        })
        .filter(Objects::nonNull)
        .filter(ctx -> {
          if (ctx.startsWith("\"") // spaces in context
              || ctx.startsWith("context") // test
              || ctx.startsWith("5f2599095a331b000bc0343b,")
              || ctx.endsWith("eu/doc/legislati")
          ) {
            return false;
          }
          if (ctx.startsWith("eu/doc/legislation/binary/")
              || ctx.startsWith("eu/doc/legislation/fulltext/cellar/")
              || ctx.startsWith("eu/docfamily/legislation/cellar/")) {
            return true;
          }
          throw new IllegalStateException("Unexpected context, unable to classify: [" + ctx + "]");
        })
        .forEach(set::add);

    set.forEach(line -> {
          try {
            synchronized (writer) {
              writer.write(line);
              writer.write(String.format("%n"));
            }
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });

    writer.close();
  }
}

