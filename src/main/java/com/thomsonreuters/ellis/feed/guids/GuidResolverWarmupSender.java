package com.thomsonreuters.ellis.feed.guids;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GuidResolverWarmupSender {
  private final String host;
  private final int batchSize = 300;
  private final int nThreads = 10;

  final ExecutorService executorService = Executors.newFixedThreadPool(nThreads);

  final List<Future<List<String>>> futures = new ArrayList<>();
  final LongAdder submitted = new LongAdder();
  final LongAdder completed = new LongAdder();
  final AtomicBoolean processingStartedGuard = new AtomicBoolean();
  final Stopwatch processing = Stopwatch.createUnstarted();

  public GuidResolverWarmupSender(String host) {
    this.host=host;
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    String envId = "solt-dev"; // ellis-dev, solt-preprod, solt-prod
    String host =
        //"http://localhost:8030/";
        "http://" + envId + ".int.thomsonreuters.com:8030/";
    new GuidResolverWarmupSender(host)
        .parseFileAndStartTasks(".\\preprod_contexts.txt",
            ".\\completed_" + envId + ".txt",
            ".\\completed_now_" + envId + ".txt");
  }

  private void parseFileAndStartTasks(String ctxesToBeSent, String ctxesCompleted, String ctxesCompletedNow)
      throws IOException, InterruptedException {

    final Stream<String> earlierProcessed = loadLines(ctxesCompleted, false);

    final Set<String> existingCtxes = earlierProcessed.collect(Collectors.toSet());

    final Stream<String> contexts = loadLines(ctxesToBeSent, true);

    final Stream<String> uniqueStream = contexts.filter(ctx -> !existingCtxes.contains(ctx));

    final Thread logThread = GuidResolverTestUtils.startPeriodicAction(this::printStatus, 1000);

    final BufferedWriter completedFile = new BufferedWriter(new FileWriter(ctxesCompleted, true));
    final BufferedWriter completedNowFile = new BufferedWriter(new FileWriter(ctxesCompletedNow));
    Iterables.partition(uniqueStream::iterator, batchSize).forEach(this::submitBatch);

    existingCtxes.clear();

    futures.stream()
        .map(GuidResolverTestUtils::getNoThrows)
        .peek(f -> writeLines(completedFile, f))
        .forEach(f -> writeLines(completedNowFile, f));

    executorService.shutdown();
    executorService.awaitTermination(10, TimeUnit.SECONDS);
    logThread.interrupt();
    completedFile.close();
    completedNowFile.close();
  }

  private synchronized void writeLines(BufferedWriter completedFile, List<String> f) {
    try {
      for (String next : f) {
        completedFile.write(next);
        completedFile.newLine();
      }
      completedFile.flush();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Stream<String> loadLines(String ctxesToBeSent, boolean shouldExist) throws IOException {
    final Path of = Path.of(ctxesToBeSent);
    if (!of.getFileName().toFile().exists()) {
      if (shouldExist) {
        throw new IllegalStateException(of.getFileName().toFile().getAbsolutePath());
      } else {
        return Stream.empty();
      }
    }

    return Files.lines(of)
          .filter(Objects::nonNull)
          .filter(s -> !s.isBlank());
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
    final int size = p.size();
    futures.add(executorService.submit(() -> processBatch(p)));
    submitted.add(size);
  }

  private List<String> processBatch(List<String> ctxes) {
    if(processingStartedGuard.compareAndSet(false, true)) {
      processing.start();
    }
    final int cnt = ctxes.size();
   // System.out.println(Thread.currentThread().getName() + " To process:" + cnt);
    final List<String> strings = sendResolveBatch(ctxes);
    completed.add(cnt);
    return strings;
  }

  private List<String> sendResolveBatch(List<String> ctxes)   {
    try {
      final String content  = new Gson().toJson(ctxes);
      //ctxes.clear(); // to free memory

      final String contentAsString =
          GuidResolverTestUtils.sendRequestWithJsonBody(host, "resolveBatch", content);
      if (contentAsString == null) {
        return Collections.emptyList();
      }

      final Type type = new TypeToken<ArrayList<GuidDtoMin>>() {
      }.getType();
      final List<GuidDtoMin> guidDtoList;
      try {
        guidDtoList = new Gson().fromJson(contentAsString, type);
      } catch (JsonSyntaxException e) {
        throw new RuntimeException(contentAsString, e);
      }
      final List<String> collect =
          guidDtoList.stream().map(GuidDtoMin::getContext).collect(Collectors.toList());
      if (collect.size() != ctxes.size()) {
        System.err.println("************ Error\n" + collect + "\n" + ctxes);
      }

      return collect;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
