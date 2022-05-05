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

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;

import java.net.HttpURLConnection;
import java.util.List;

/**
 * Retry policy used by AbfsClient.
 * */
public class LeaseRetryPolicy extends ExponentialRetryPolicy {
    /**
     * The maximum number of retry attempts.
     */
    private final long maxLeaseRetryTime;

    public LeaseRetryPolicy(AbfsConfiguration conf){
        this(conf.getMaxLeaseRetryTime());
    }

    /**
     * Initializes a new instance of the {@link LeaseRetryPolicy} class.
     */
    public LeaseRetryPolicy(final long maxLeaseRetryTime) {
        super(maxLeaseRetryTime);
        this.maxLeaseRetryTime = maxLeaseRetryTime;
    }

    @Override
    public boolean shouldRetry(final int retryCount, int statusCode, String statusDescription, final AbfsRestOperationType operationType,
        List<AbfsHttpHeader> requestHeaders, long operationTime) {
        if (isBundleLeaseOperation(requestHeaders)) {
            return (operationTime < this.maxLeaseRetryTime
                    && (statusCode == HttpURLConnection.HTTP_CONFLICT
                    || statusCode == HttpURLConnection.HTTP_PRECON_FAILED) && AbfsLease.checkStatusDescription(statusDescription));
        }
        return super.shouldRetry(retryCount, statusCode, statusDescription, operationType, requestHeaders, operationTime);
    }

    public boolean isBundleLeaseOperation(List<AbfsHttpHeader> requestHeaders) {
        return (requestHeaders.contains(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_LEASE_ACTION, AbfsHttpConstants.RELEASE_LEASE_ACTION))
                || requestHeaders.contains(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_LEASE_ACTION, AbfsHttpConstants.AUTO_RENEW_LEASE_ACTION)));
    }
}
