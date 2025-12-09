package org.apache.hadoop.fs.azurebfs.services;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.fs.azurebfs.utils.ResourceUtilizationUtils;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ZERO;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ZERO_D;

/**
 * Collects statistics for the ABFS write thread pool and JVM/system utilization.
 * This class is independent and not an inner class.
 */
public class WriteThreadPoolStatsCollector extends ResourceUtilizationStats {
  public WriteThreadPoolStatsCollector(
      int currentPoolSize,
      int maxPoolSize,
      int activeThreads,
      int idleThreads,
      double jvmCpuLoad,
      double systemCpuUtilization,
      double availableHeapGB,
      double committedHeapGB,
      double usedHeapGB,
      double maxHeapGB,
      double memoryLoad,
      long jvmProcessId) {

    super(currentPoolSize, maxPoolSize, activeThreads, idleThreads,
        jvmCpuLoad, systemCpuUtilization, availableHeapGB,
        committedHeapGB, usedHeapGB, maxHeapGB, memoryLoad, jvmProcessId);
  }


  public static synchronized WriteThreadPoolStatsCollector getCurrentStats(
      double jvmCpuUtilization,
      double memoryLoad,
      ExecutorService boundedThreadPool) {

    if (boundedThreadPool == null) {
      return new WriteThreadPoolStatsCollector(
          ZERO, ZERO, ZERO, ZERO,
          ZERO_D, ZERO_D,
          ZERO_D, ZERO_D, ZERO_D, ZERO_D,
          ZERO_D,
          ResourceUtilizationUtils.getJvmProcessId());
    }

    int poolSize = 0, activeThreads = 0, maxPoolSize = 0;

    if (boundedThreadPool.getClass().getName()
        .equals("org.apache.hadoop.util.BlockingThreadPoolExecutorService")) {

      try {
        java.lang.reflect.Field field = boundedThreadPool.getClass()
            .getDeclaredField("eventProcessingExecutor");
        field.setAccessible(true);
        ThreadPoolExecutor executor = (ThreadPoolExecutor) field.get(boundedThreadPool);

        poolSize = executor.getPoolSize();
        activeThreads = executor.getActiveCount();
        maxPoolSize = executor.getMaximumPoolSize();
      } catch (Exception e) {
        // fallback if reflection fails
        activeThreads = poolSize = maxPoolSize = 0;
      }
    } else if (boundedThreadPool instanceof ThreadPoolExecutor) {
      ThreadPoolExecutor exec = (ThreadPoolExecutor) boundedThreadPool;
      poolSize = exec.getPoolSize();
      activeThreads = exec.getActiveCount();
      maxPoolSize = exec.getMaximumPoolSize();
    }


    int idleThreads = poolSize - activeThreads;

    return new WriteThreadPoolStatsCollector(
        poolSize,
        maxPoolSize,
        activeThreads,
        idleThreads,
        jvmCpuUtilization,
        ResourceUtilizationUtils.getSystemCpuLoad(),
        ResourceUtilizationUtils.getAvailableHeapMemory(),
        ResourceUtilizationUtils.getCommittedHeapMemory(),
        ResourceUtilizationUtils.getUsedHeapMemory(),
        ResourceUtilizationUtils.getMaxHeapMemory(),
        memoryLoad,
        ResourceUtilizationUtils.getJvmProcessId()
    );
  }
}
