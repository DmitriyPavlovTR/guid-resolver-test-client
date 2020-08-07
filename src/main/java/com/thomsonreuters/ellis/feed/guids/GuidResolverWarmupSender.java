package com.thomsonreuters.ellis.feed.guids;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

public class GuidResolverWarmupSender {
  public final String HOST =
      //"http://localhost:8030/";
      //"http://ellis-dev.int.thomsonreuters.com:8030/";
      "http://solt-preprod.int.thomsonreuters.com:8030/";

  private final int batchSize = 1000;

  final ExecutorService executorService = Executors.newFixedThreadPool(10);
  final List<Future<?>> futures = new ArrayList<>();
  final LongAdder submitted = new LongAdder();
  final LongAdder completed = new LongAdder();
  final AtomicBoolean processingStartedGuard = new AtomicBoolean();
  final Stopwatch processing = Stopwatch.createUnstarted();

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
    res.append("Total Contexts [submitted ").append(submitted.longValue()).append("; ");
    res.append("completed ").append(completed.longValue()).append("; ");
    res.append("] ");

    final long elapsed = processing.elapsed(TimeUnit.MILLISECONDS);
    if(elapsed>0) {
      double cps = (1000.0 * completed.longValue()) / elapsed;

      res.append("Speed ").append(String.format("%.2f", cps)).append(" ctx/s");
    }
    System.out.println(res.toString());
  }

  public void submitBatch(List<String> p) {
    futures.add(executorService.submit(() -> processBatch(p)));
    submitted.add(p.size());
  }

  private Integer processBatch(List<String> ctxes) {
    if(processingStartedGuard.compareAndSet(false, true)) {
      processing.start();
    }
    final int cnt = ctxes.size();
    System.out.println(Thread.currentThread().getName() + " To process:" + cnt);
    sendResolveBatch(ctxes);
    completed.add(cnt);
    return cnt;
  }

  private void sendResolveBatch(List<String> ctxes)   {
    try {
      final String content  = new Gson().toJson(ctxes);
      GuidResolverTestUtils.sendRequestWithJsonBody(HOST, "resolveBatch", content);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
