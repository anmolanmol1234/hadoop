/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

import java.util.List;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.INFINITE_LEASE_DURATION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_ID;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_LEASE_FUTURE_EXISTS;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_NO_LEASE_THREADS;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.nullness.qual.Nullable;
import org.apache.hadoop.classification.VisibleForTesting;
import static org.apache.hadoop.io.retry.RetryPolicies.retryUpToMaximumCountWithFixedSleep;

import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbfsInfiniteLease extends AbfsLease {
  private static final Logger LOG = LoggerFactory.getLogger(AbfsInfiniteLease.class);

  private final AbfsClient client;
  private final String path;
  private final TracingContext tracingContext;

  // Number of retries for acquiring lease
  static final int DEFAULT_LEASE_ACQUIRE_MAX_RETRIES = 7;
  // Retry interval for acquiring lease in secs
  static final int DEFAULT_LEASE_ACQUIRE_RETRY_INTERVAL = 10;

  //private volatile String leaseID = null;
  private volatile Throwable exception = null;
  private volatile ListenableScheduledFuture<AbfsRestOperation> future = null;

  public AbfsInfiniteLease(final AbfsClient client,
      final String path,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    this(client, path, DEFAULT_LEASE_ACQUIRE_MAX_RETRIES,
        DEFAULT_LEASE_ACQUIRE_RETRY_INTERVAL, tracingContext);
  }

  public AbfsInfiniteLease(final AbfsClient client,
      final String path,
      final int acquireMaxRetries,
      final int acquireRetryInterval,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    this.leaseFreed = false;
    this.client = client;
    this.path = path;
    this.tracingContext = tracingContext;
    if (client.getNumLeaseThreads() < 1) {
      throw new LeaseException(ERR_NO_LEASE_THREADS);
    }

    // Try to get the lease a specified number of times, else throw an error
    RetryPolicy retryPolicy = retryUpToMaximumCountWithFixedSleep(
        acquireMaxRetries, acquireRetryInterval, TimeUnit.SECONDS);
    acquireLease(retryPolicy, 0, acquireRetryInterval, 0,
        new TracingContext(tracingContext));

    while (leaseID == null && exception == null) {
      try {
        future.get();
      } catch (Exception e) {
        LOG.debug("Got exception waiting for acquire lease future. Checking if lease ID or "
            + "exception have been set", e);
      }
    }
    if (exception != null) {
      LOG.error("Failed to acquire lease on {}", path);
      throw new LeaseException(exception);
    }

    LOG.debug("Acquired lease {} on {}", leaseID, path);
  }

  private void acquireLease(RetryPolicy retryPolicy, int numRetries,
      int retryInterval, long delay, TracingContext tracingContext)
      throws LeaseException {
    LOG.debug("Attempting to acquire lease on {}, retry {}", path, numRetries);
    if (future != null && !future.isDone()) {
      throw new LeaseException(ERR_LEASE_FUTURE_EXISTS);
    }
    future = client.schedule(() -> client.acquireLease(path,
            INFINITE_LEASE_DURATION, tracingContext),
        delay, TimeUnit.SECONDS);
    client.addCallback(future, new FutureCallback<AbfsRestOperation>() {
      @Override
      public void onSuccess(@Nullable AbfsRestOperation op) {
        leaseID = op.getResult().getResponseHeader(X_MS_LEASE_ID);
        acquireNewLease = true;
        LOG.debug("Acquired lease {} on {}", leaseID, path);
      }

      @Override
      public void onFailure(Throwable throwable) {
        try {
          if (RetryPolicy.RetryAction.RetryDecision.RETRY
              == retryPolicy.shouldRetry(null, numRetries, 0, true).action) {
            LOG.debug("Failed to acquire lease on {}, retrying: {}", path, throwable);
            acquireRetryCount++;
            acquireLease(retryPolicy, numRetries + 1, retryInterval,
                retryInterval, tracingContext);
          } else {
            exception = throwable;
          }
        } catch (Exception e) {
          exception = throwable;
        }
      }
    });
  }

  /**
   * Cancel future and free the lease. If an exception occurs while releasing the lease, the error
   * will be logged. If the lease cannot be released, AzureBlobFileSystem breakLease will need to
   * be called before another client will be able to write to the file.
   */
  public void free() {
    if (leaseFreed) {
      return;
    }
    try {
      LOG.debug("Freeing lease: path {}, lease id {}", path, leaseID);
      if (future != null && !future.isDone()) {
        future.cancel(true);
      }
      TracingContext tracingContext = new TracingContext(this.tracingContext);
      tracingContext.setOperation(FSOperationType.RELEASE_LEASE);
      client.releaseLease(path, leaseID, tracingContext);
    } catch (java.io.IOException e) {
      LOG.warn("Exception when trying to release lease {} on {}. Lease will need to be broken: {}",
          leaseID, path, e.getMessage());
    } finally {
      // Even if releasing the lease fails (e.g. because the file was deleted),
      // make sure to record that we freed the lease
      leaseFreed = true;
      LOG.debug("Freed lease {} on {}", leaseID, path);
    }
  }

  public void addHeaders(AppendRequestParameters requestParameters, Boolean isClose,
      String leaseDuration, List<AbfsHttpHeader> requestHeaders) {
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, this.getLeaseID()));
  }

  public String getLeaseID() {
    return leaseID;
  }

  @VisibleForTesting
  public TracingContext getTracingContext() {
    return tracingContext;
  }
}
