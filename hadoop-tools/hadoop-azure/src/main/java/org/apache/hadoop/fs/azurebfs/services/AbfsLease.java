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


import java.util.List;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ACQUIRE_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ACQUIRE_RELEASE_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.AUTO_RENEW_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.RELEASE_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_ACQUIRING_LEASE;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_LEASE_ALREADY_PRESENT;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * AbfsLease manages an Azure blob lease. It acquires an infinite lease on instantiation and
 * releases the lease when free() is called. Use it to prevent writes to the blob by other
 * processes that don't have the lease.
 *
 * Creating a new Lease object blocks the caller until the Azure blob lease is acquired. It will
 * retry a fixed number of times before failing if there is a problem acquiring the lease.
 *
 * Call free() to release the Lease. If the holder process dies, AzureBlobFileSystem breakLease
 * will need to be called before another client will be able to write to the file.
 */
public class AbfsLease {
  private static final Logger LOG = LoggerFactory.getLogger(AbfsLease.class);
  protected  String leaseID = null;
  public  boolean leaseFreed;
  public  boolean acquireNewLease;
  public  int acquireRetryCount = 0;
  public  boolean isInfiniteLease = true;

  public static class LeaseException extends AzureBlobFileSystemException {
    public LeaseException(Throwable t) {
      super(ERR_ACQUIRING_LEASE + ": " + t, t);
    }
    public LeaseException(String s) {
      super(s);
    }
  }

  public static boolean isValidLease(AbfsLease lease) {
    return (lease != null && lease.getLeaseID() != null && !lease.getLeaseID().isEmpty());
  }

  public void addHeaders(AppendRequestParameters requestParameters, Boolean isClose,
      String leaseDuration, List<AbfsHttpHeader> requestHeaders) {
  }



  public boolean checkLeaseStatus(AbfsRestOperation op) {
    if ((op.getResult().getStatusCode() == HTTP_CONFLICT
        || op.getResult().getStatusCode() == HTTP_PRECON_FAILED) && ((op.getRequestHeaders().contains(
        new AbfsHttpHeader(X_MS_LEASE_ACTION, RELEASE_LEASE_ACTION)))
        || (op.getRequestHeaders().contains(new AbfsHttpHeader(X_MS_LEASE_ACTION,
        AUTO_RENEW_LEASE_ACTION)))) && !(op.getResult().getStatusDescription().equals(ERR_LEASE_ALREADY_PRESENT))) {
      return true;
    }
    return false;
  }

  public boolean isFreed() {
    return leaseFreed;
  }

  public String getLeaseID() {
    return leaseID;
  }

  public boolean isInfiniteLease() {
    return isInfiniteLease;
  }

  public boolean isAcquireNewLease() {
    return acquireNewLease;
  }

  public void setAcquireNewLease(final boolean acquireNewLease) {
    this.acquireNewLease = acquireNewLease;
    leaseFreed = !acquireNewLease;
  }

  @VisibleForTesting
  public int getAcquireRetryCount() {
    return acquireRetryCount;
  }

}
