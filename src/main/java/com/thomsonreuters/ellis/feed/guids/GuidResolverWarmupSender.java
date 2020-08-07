package com.thomsonreuters.ellis.feed.guids;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

public class GuidResolverWarmupSender {
  public final String HOST =
      "http://localhost:8030/";
  //"http://ellis-dev.int.thomsonreuters.com:8030/";
  //"http://solt-preprod.int.thomsonreuters.com:8030/";

  private final int batchSize = 1000;

  final ExecutorService executorService = Executors.newFixedThreadPool(10);
  final List<Future<?>> futures = new ArrayList<>();
  final LongAdder submitted = new LongAdder();

  public static void main(String[] args) throws IOException, InterruptedException {
    new GuidResolverWarmupSender()
        .parseFileAndStartTasks(".\\preprod_contexts.txt");
  }

  private void parseFileAndStartTasks(String newfileSeparatedFile)
      throws IOException, InterruptedException {
    final Stream<String> lines = Files.lines(Path.of(newfileSeparatedFile));

    final Stream<String> contexts = lines
        .filter(Objects::nonNull)
        .filter(s -> !s.isBlank());

    final Thread logThread = GuidResolverTestUtils.startPeriodicAction(
        this::printStatus, 1000);

    Iterables.partition(contexts::iterator, batchSize)
        .forEach(this::submitBatch);

    futures.forEach(GuidResolverTestUtils::getNoThrows);

    executorService.shutdown();
    executorService.awaitTermination(10, TimeUnit.SECONDS);
    logThread.interrupt();
  }

  private void printStatus() {
    final StringBuilder res = new StringBuilder();
    res.append("Total Contexts submitted ").append(submitted.longValue());
    System.out.println(res.toString());
  }


  public void submitBatch(List<String> p) {
    futures.add(executorService.submit(() -> {
      System.out.println(Thread.currentThread().getName() + " To process:" + p.size());

      return p.size();
    }));
    submitted.add(p.size());
  }
}
