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

import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

/**
 * Manages the lifecycle of Blob Storage session credentials.
 *
 * <p>
 * The manager lazily creates a session using the Blob Create Session API
 * and caches the returned credentials for subsequent requests.
 * </p>
 *
 * <p>
 * Session refresh and invalidation are introduced in later phases.
 * </p>
 */
public class AbfsSessionManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbfsSessionManager.class);

  /**
   * Blob client used to create sessions.
   */
  private final AbfsClient abfsClient;

  /**
   * Synchronizes session creation.
   */
  private final ReentrantLock sessionLock = new ReentrantLock();

  /**
   * Cached session credentials.
   */
  private volatile SessionCredentials sessionCredentials;

  /**
   * Creates an AbfsSessionManager.
   *
   * @param abfsClient Client used to create sessions.
   */
  public AbfsSessionManager(final AbfsClient abfsClient) {
    this.abfsClient = abfsClient;
  }

  /**
   * Returns the current session credentials.
   *
   * <p>
   * If no session exists, a new session is created and cached.
   * </p>
   *
   * @param tracingContext tracing context.
   * @return current session credentials.
   * @throws AzureBlobFileSystemException if session creation fails.
   */
  public SessionCredentials getSession(
      final TracingContext tracingContext)
      throws AzureBlobFileSystemException {

    if (sessionCredentials != null) {
      return sessionCredentials;
    }

    sessionLock.lock();

    try {
      if (sessionCredentials == null) {
        LOG.debug("Creating Blob session.");

        sessionCredentials = abfsClient.createSession(tracingContext);
      }

      return sessionCredentials;

    } finally {
      sessionLock.unlock();
    }
  }

  /**
   * Returns whether a session has been created.
   *
   * @return true if session credentials are cached.
   */
  public boolean hasSession() {
    return sessionCredentials != null;
  }

  /**
   * Returns the cached session credentials.
   *
   * @return cached session credentials, or null if no session exists.
   */
  public SessionCredentials getCachedSession() {
    return sessionCredentials;
  }

  /**
   * Invalidates the cached session.
   *
   * <p>
   * This method is intended to be used when the service rejects the
   * current session credentials. A new session will be created on the
   * next call to {@link #getSession(TracingContext)}.
   * </p>
   */
  public void invalidateSession() {
    LOG.debug("Invalidating Blob session.");
    sessionCredentials = null;
  }
}