/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HIGH_CPU_THRESHOLD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.LOW_CPU_THRESHOLD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.MEDIUM_CPU_THRESHOLD;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.BYTES_PER_GIGABYTE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HIGH_MEMORY_MULTIPLIER;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HIGH_MEMORY_THRESHOLD_GB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.LOW_MEMORY_MULTIPLIER;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.LOW_MEMORY_THRESHOLD_GB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MEDIUM_MEMORY_MULTIPLIER;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MEDIUM_MEMORY_THRESHOLD_GB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.POOL_SIZE_INCREASE_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SIXTY_SECONDS;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.THIRTY_SECONDS;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.VERY_HIGH_MEMORY_MULTIPLIER;

/**
 * Manages a thread pool for writing operations, adjusting the pool size based on CPU utilization.
 */
public final class WriteThreadPoolSizeManager implements Closeable {

  /* Maximum allowed size for the thread pool. */
  private final int maxThreadPoolSize;
  /* Executor for periodically monitoring CPU usage. */
  private final ScheduledExecutorService cpuMonitorExecutor;
  /* Thread pool whose size is dynamically managed. */
  private volatile ExecutorService boundedThreadPool;
  /* Lock to ensure thread-safe updates to the thread pool. */
  private final Lock lock = new ReentrantLock();
  /* New computed max size for the thread pool after adjustment. */
  private volatile int newMaxPoolSize;
  /* Logger instance for logging events from WriteThreadPoolSizeManager. */
  private static final Logger LOG = LoggerFactory.getLogger(
      WriteThreadPoolSizeManager.class);
  /* Map to maintain a WriteThreadPoolSizeManager instance per filesystem. */
  private static final ConcurrentHashMap<String, WriteThreadPoolSizeManager>
      POOL_SIZE_MANAGER_MAP = new ConcurrentHashMap<>();
  /* Name of the filesystem associated with this manager. */
  private final String filesystemName;
  /* Initial size for the thread pool when created. */
  private final int initialPoolSize;
  /* Constant representing number of bytes in one gigabyte. */
  private static final long BYTES_PER_GIGABYTE = 1024L * 1024L * 1024L;

  /**
   * Private constructor to initialize the write thread pool and CPU monitor executor
   * based on system resources and ABFS configuration.
   *
   * @param filesystemName       Name of the ABFS filesystem.
   * @param abfsConfiguration    Configuration containing pool size parameters.
   */
  private WriteThreadPoolSizeManager(String filesystemName,
      AbfsConfiguration abfsConfiguration) {
    this.filesystemName = filesystemName;

    int availableProcessors = Runtime.getRuntime().availableProcessors();
    int computedMaxPoolSize = getComputedMaxPoolSize(availableProcessors);

    /* Get the initial pool size from config, fallback to at least 1 */
    this.initialPoolSize = Math.max(1,
        abfsConfiguration.getWriteMaxConcurrentRequestCount());

    /* Set the upper bound for the thread pool size */
    this.maxThreadPoolSize = Math.max(computedMaxPoolSize, initialPoolSize);

    /*  Initialize the bounded thread pool executor */
    this.boundedThreadPool = Executors.newFixedThreadPool(initialPoolSize);

    ThreadPoolExecutor executor = (ThreadPoolExecutor) this.boundedThreadPool;
    executor.setKeepAliveTime(
        abfsConfiguration.getWriteThreadPoolKeepAliveTime(), TimeUnit.SECONDS);
    executor.allowCoreThreadTimeOut(true);

    /* Create a scheduled executor for CPU monitoring and pool adjustment */
    this.cpuMonitorExecutor = Executors.newScheduledThreadPool(
        abfsConfiguration.getWriteCorePoolSize());
  }

  /**
   * Calculates the max thread pool size using a multiplier based on
   * memory per core. Higher memory per core results in a larger multiplier.
   *
   * @param availableProcessors Number of CPU cores.
   * @return Computed max thread pool size.
   */
  private int getComputedMaxPoolSize(final int availableProcessors) {
      Runtime runtime = Runtime.getRuntime();

      long maxMemory = runtime.maxMemory();
      long usedMemory = runtime.totalMemory() - runtime.freeMemory();
      long availableHeapBytes = maxMemory - usedMemory;
      long availableHeapGB = (availableHeapBytes + BYTES_PER_GIGABYTE - 1) / BYTES_PER_GIGABYTE;
      LOG.debug("The available heap space in GB {} ", availableHeapGB);
      LOG.debug("The number of available processors is {} ", availableProcessors);
      int maxpoolSize = getMemoryTierMaxThreads(availableHeapGB, availableProcessors);
      LOG.debug("The max thread pool size is {} ", maxpoolSize);
      return maxpoolSize;
  }

  /**
   * Returns aggressive thread count = CPU cores × multiplier based on heap tier.
   */
  private int getMemoryTierMaxThreads(long availableHeapGB, int availableProcessors) {
    int multiplier;
    if (availableHeapGB <= 4) {
      multiplier = 4;
    } else if (availableHeapGB <= 8) {
      multiplier = 8;
    } else {
      multiplier = 16;
    }
    return availableProcessors * multiplier;
  }

  /**
   * Get total system memory in bytes using OperatingSystemMXBean
   *
   * @return Total memory in bytes
   */
  private long getTotalMemoryInBytes() {
    OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
    if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
      com.sun.management.OperatingSystemMXBean sunOsBean
          = (com.sun.management.OperatingSystemMXBean) osBean;
      return sunOsBean.getTotalPhysicalMemorySize();  // This returns total memory in bytes
    }
    return 0;
  }

  /**
   * Returns the singleton instance of WriteThreadPoolSizeManager for the given filesystem.
   *
   * @param filesystemName the name of the filesystem.
   * @param abfsConfiguration the configuration for the ABFS.
   *
   * @return the singleton instance.
   */
  public static synchronized WriteThreadPoolSizeManager getInstance(
      String filesystemName, AbfsConfiguration abfsConfiguration) {
    /* Check if an instance already exists in the map for the given filesystem */
    WriteThreadPoolSizeManager existingInstance = POOL_SIZE_MANAGER_MAP.get(
        filesystemName);

    /* If an existing instance is found, return it */
    if (existingInstance != null && existingInstance.boundedThreadPool != null
        && !existingInstance.boundedThreadPool.isShutdown()) {
      return existingInstance;
    }

    /* Otherwise, create a new instance, put it in the map, and return it */
    LOG.debug(
        "Creating new WriteThreadPoolSizeManager instance for filesystem: {}",
        filesystemName);
    WriteThreadPoolSizeManager newInstance = new WriteThreadPoolSizeManager(
        filesystemName, abfsConfiguration);
    POOL_SIZE_MANAGER_MAP.put(filesystemName, newInstance);
    return newInstance;
  }

  /**
   * Adjusts the thread pool size to the specified maximum pool size.
   *
   * @param newMaxPoolSize the new maximum pool size.
   */
  private void adjustThreadPoolSize(int newMaxPoolSize) {
    synchronized (this) {
      ThreadPoolExecutor threadPoolExecutor
          = ((ThreadPoolExecutor) boundedThreadPool);
      int currentCorePoolSize = threadPoolExecutor.getCorePoolSize();
      if (newMaxPoolSize >= currentCorePoolSize) {
        threadPoolExecutor.setMaximumPoolSize(newMaxPoolSize);
        threadPoolExecutor.setCorePoolSize(newMaxPoolSize);
      } else {
        threadPoolExecutor.setCorePoolSize(newMaxPoolSize);
        threadPoolExecutor.setMaximumPoolSize(newMaxPoolSize);
      }
      LOG.debug("The thread pool size is: {} ", newMaxPoolSize);
      LOG.debug("The pool size is: {} ", threadPoolExecutor.getPoolSize());
      LOG.debug("The active thread count is: {}",
          threadPoolExecutor.getActiveCount());
    }
  }

  /**
   * Starts monitoring the CPU utilization and adjusts the thread pool size accordingly.
   */
  synchronized void startCPUMonitoring() {
    cpuMonitorExecutor.scheduleAtFixedRate(() -> {
      double cpuUtilization = getCpuUtilization();
      LOG.debug("Current CPU Utilization is this: {}", cpuUtilization);
      try {
        adjustThreadPoolSizeBasedOnCPU(cpuUtilization);
      } catch (InterruptedException e) {
        throw new RuntimeException(String.format(
            "Thread pool size adjustment interrupted for filesystem %s",
            filesystemName), e);
      }
    }, 0, SIXTY_SECONDS, TimeUnit.SECONDS);
  }

  /**
   * Gets the current CPU utilization.
   *
   * @return the CPU utilization as a percentage (0.0 to 1.0).
   */
  private double getCpuUtilization() {
    OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
    if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
      com.sun.management.OperatingSystemMXBean sunOsBean
          = (com.sun.management.OperatingSystemMXBean) osBean;
      double cpuLoad = sunOsBean.getSystemCpuLoad();
      if (cpuLoad >= 0) {
        return cpuLoad;
      }
    }
    return 0.0;
  }

  /**
   * Adjusts the thread pool size based on the current CPU utilization.
   *  <ul>
   *  <li>If CPU usage is high, the pool size is reduced by ~33%.</li>
   *  <li>If CPU usage is medium, the pool size is reduced by ~20%.</li>
   *  <li>If CPU usage is low, the pool size is increased by 50%, capped at a configured max.</li>
   *  <li>If CPU usage is moderate, the current size is retained.</li>
   *  </ul>
   *
   * @param cpuUtilization the current CPU utilization.
   *
   * @throws InterruptedException if the thread pool adjustment is interrupted.
   */
  public void adjustThreadPoolSizeBasedOnCPU(double cpuUtilization)
      throws InterruptedException {
    lock.lock();
    try {
      int currentPoolSize = ((ThreadPoolExecutor) boundedThreadPool).getMaximumPoolSize();
      if (cpuUtilization > HIGH_CPU_THRESHOLD) {
        newMaxPoolSize = Math.max(initialPoolSize,
            currentPoolSize - currentPoolSize / 3);
        LOG.debug("High CPU load detected ({}). Reducing pool size: current={}, new={}",
            cpuUtilization, currentPoolSize, newMaxPoolSize);
      } else if (cpuUtilization > MEDIUM_CPU_THRESHOLD) {
        newMaxPoolSize = Math.max(initialPoolSize,
            currentPoolSize - currentPoolSize / 5);
        LOG.debug("Medium CPU load detected ({}). Moderately reducing pool size: current={}, new={}",
            cpuUtilization, currentPoolSize, newMaxPoolSize);
      } else if (cpuUtilization < LOW_CPU_THRESHOLD) {
        newMaxPoolSize = Math.min(maxThreadPoolSize,
            (int) (currentPoolSize * POOL_SIZE_INCREASE_FACTOR));
        LOG.debug("Low CPU load detected ({}). Increasing pool size: current={}, new={}",
            cpuUtilization, currentPoolSize, newMaxPoolSize);
      } else {
        newMaxPoolSize = currentPoolSize;
        LOG.debug("CPU load within normal range ({}). No change to pool size: current={}",
            cpuUtilization, currentPoolSize);
      }
      if (newMaxPoolSize != currentPoolSize) {
        LOG.debug("Adjusting thread pool size from {} to {}", currentPoolSize, newMaxPoolSize);
        this.adjustThreadPoolSize(newMaxPoolSize);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns the executor service for the thread pool.
   *
   * @return the executor service.
   */
  public ExecutorService getExecutorService() {
    return boundedThreadPool;
  }

  public ScheduledExecutorService getCpuMonitorExecutor() {
    return cpuMonitorExecutor;
  }

  @Override
  public void close() throws IOException {
    synchronized (this) {
      try {
        // Shutdown executors
        cpuMonitorExecutor.shutdown();
        HadoopExecutors.shutdown(boundedThreadPool, LOG, THIRTY_SECONDS, TimeUnit.SECONDS);
        boundedThreadPool = null;

        // Remove from the map
        POOL_SIZE_MANAGER_MAP.remove(filesystemName);
        LOG.debug("Closed and removed instance for filesystem: {}",
            filesystemName);
      } catch (Exception e) {
        LOG.warn("Failed to properly close instance for filesystem: {}",
            filesystemName, e);
      }
    }
  }
}
