package org.apache.hadoop.fs.azurebfs.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.azurebfs.utils.ResourceUtilizationUtils;

/**
 * Publishes ABFS write thread pool metrics at fixed intervals,
 * independent of whether dynamic write optimization is enabled.
 */
public final class AbfsWriteMetricsPublisher {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbfsWriteMetricsPublisher.class);

  private final ScheduledExecutorService scheduler;
  private final long publishIntervalMs;
  private final ExecutorService writeThreadPool;
  private volatile boolean started = false;
  public AbfsWriteResourceUtilizationMetrics writeResourceUtilizationMetrics;

  public AbfsWriteMetricsPublisher(long publishIntervalMs,
      ExecutorService writeThreadPool, AbfsWriteResourceUtilizationMetrics writeResourceUtilizationMetrics) {
    this.publishIntervalMs = publishIntervalMs;
    this.writeThreadPool = writeThreadPool;
    this.writeResourceUtilizationMetrics = writeResourceUtilizationMetrics;
    this.scheduler = Executors.newSingleThreadScheduledExecutor(
        r -> new Thread(r, "abfs-write-metrics-publisher"));
  }

  /** Starts periodic metric publishing. */
  public synchronized void start() {
    if (started) {
      return;
    }
    started = true;

    scheduler.scheduleAtFixedRate(() -> {
      try {
        publishMetrics();
      } catch (Throwable t) {
        LOG.warn("Error publishing ABFS write metrics", t);
      }
    }, publishIntervalMs, publishIntervalMs, TimeUnit.MILLISECONDS);

    LOG.info("Started AbfsWriteMetricsPublisher at interval {} ms",
        publishIntervalMs);
  }

  /** Stops the scheduler. */
  public synchronized void stop() {
    if (!started) {
      return;
    }
    started = false;
    scheduler.shutdownNow();
    LOG.info("Stopped AbfsWriteMetricsPublisher");
  }

  /** Gather the metrics from the thread pool. */
  private WriteThreadPoolStatsCollector getCurrentStats() {
    double jvmCpu = ResourceUtilizationUtils.getJvmCpuLoad();
    double memLoad = ResourceUtilizationUtils.getMemoryLoad();
    return WriteThreadPoolStatsCollector.getCurrentStats(
        jvmCpu,
        memLoad,
        writeThreadPool);
  }

  /** Collects and pushes the write metrics. */
  private void publishMetrics() {
    WriteThreadPoolStatsCollector stats = getCurrentStats();

    if (stats == null) {
      LOG.debug("No write metrics available yet");
      return;
    }

    // Push metrics
    writeResourceUtilizationMetrics.update(stats);

    LOG.debug("Published write metrics: {}", stats);
  }
}
