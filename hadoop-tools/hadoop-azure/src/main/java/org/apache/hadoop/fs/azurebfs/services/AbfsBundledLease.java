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


import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ACQUIRE_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ACQUIRE_RELEASE_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.AUTO_RENEW_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.RELEASE_LEASE_ACTION;
import org.apache.hadoop.classification.VisibleForTesting;
import java.util.List;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_DURATION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_PROPOSED_LEASE_ID;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_NO_LEASE_THREADS;

import java.util.UUID;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbfsBundledLease extends AbfsLease {
  private static final Logger LOG = LoggerFactory.getLogger(AbfsBundledLease.class);

  public enum LeaseMode {
    AUTO_RENEW_MODE,
    ACQUIRE_MODE,
    ACQUIRE_RELEASE_MODE,
    RELEASE_MODE,
    CREATE_MODE
  }

  private final AbfsClient client;
  private final String path;
  private final TracingContext tracingContext;


  public AbfsBundledLease(final AbfsClient client,
      final String path,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    this.leaseFreed = false;
    this.isInfiniteLease = false;
    this.client = client;
    this.path = path;
    this.tracingContext = tracingContext;
    this.leaseID = UUID.randomUUID().toString();
    this.acquireNewLease = false;
    LOG.debug("Assigned lease without acquisition {} on {}.", leaseID, path);

    if (client.getNumLeaseThreads() < 1) {
      throw new LeaseException(ERR_NO_LEASE_THREADS);
    }
  }

  private LeaseMode getMode(AppendRequestParameters requestParameters, Boolean isClose) {
    if(requestParameters == null && isClose == null){
      return LeaseMode.ACQUIRE_MODE;
    }
    else if (!this.isAcquireNewLease()) {
     if ((requestParameters != null && requestParameters.getMode()
          == AppendRequestParameters.Mode.FLUSH_CLOSE_MODE) || (isClose != null && isClose)) {
        return LeaseMode.ACQUIRE_RELEASE_MODE;
      } else {
        return LeaseMode.ACQUIRE_MODE;
      }
    } else {
      if ((requestParameters != null && requestParameters.getMode()
          == AppendRequestParameters.Mode.FLUSH_CLOSE_MODE) || (isClose != null && isClose)) {
        return LeaseMode.RELEASE_MODE;
      }
    }
    return LeaseMode.AUTO_RENEW_MODE;
  }

  public void addHeaders(AppendRequestParameters requestParameters, Boolean isClose,
      String leaseDuration, List<AbfsHttpHeader> requestHeaders) {
      LeaseMode mode = getMode(requestParameters, isClose);
      switch (mode) {
      case AUTO_RENEW_MODE:
        requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, AUTO_RENEW_LEASE_ACTION));
        requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, this.getLeaseID()));
        break;
      case RELEASE_MODE:
        this.setAcquireNewLease(false);
        requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, RELEASE_LEASE_ACTION));
        requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, this.getLeaseID()));
        break;
      case ACQUIRE_MODE:
        this.setAcquireNewLease(true);
        requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, ACQUIRE_LEASE_ACTION));
        requestHeaders.add(new AbfsHttpHeader(X_MS_PROPOSED_LEASE_ID, this.getLeaseID()));
        requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_DURATION, leaseDuration));
        break;
      case ACQUIRE_RELEASE_MODE:
        this.setAcquireNewLease(true);
        requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, ACQUIRE_RELEASE_LEASE_ACTION));
        requestHeaders.add(new AbfsHttpHeader(X_MS_PROPOSED_LEASE_ID, this.getLeaseID()));
        requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_DURATION, leaseDuration));
        break;
      default:
        break;
      }
    }

  @VisibleForTesting
  public TracingContext getTracingContext() {
    return tracingContext;
  }
}

