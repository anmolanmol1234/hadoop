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

import java.time.Instant;
import java.util.Objects;

/**
 * Represents the credentials returned by the Azure Blob Storage
 * Create Session API.
 *
 * <p>A session consists of a session identifier, session token,
 * session key, authentication type, and expiration time. These
 * credentials are cached and managed by {@code AbfsSessionManager}
 * and are used to authorize subsequent Blob requests.</p>
 */
public final class SessionCredentials {

  /** Unique identifier for the session. */
  private final String sessionId;

  /** Session token used in the Authorization header. */
  private final String sessionToken;

  /** Session key used to generate request signatures. */
  private final byte[] sessionKey;

  /** Authentication type associated with the session. */
  private final String authenticationType;

  /** Session expiration time. */
  private final Instant expirationTime;

  /**
   * Constructs a SessionCredentials instance.
   *
   * @param sessionId          session identifier.
   * @param sessionToken       session token.
   * @param sessionKey         session key (defensive-copied).
   * @param authenticationType authentication type.
   * @param expirationTime     session expiration time.
   */
  public SessionCredentials(
      final String sessionId,
      final String sessionToken,
      final byte[] sessionKey,
      final String authenticationType,
      final Instant expirationTime) {
    this.sessionId = Objects.requireNonNull(sessionId, "sessionId");
    this.sessionToken = Objects.requireNonNull(sessionToken, "sessionToken");
    this.sessionKey = Objects.requireNonNull(sessionKey, "sessionKey").clone();
    this.authenticationType = Objects.requireNonNull(
        authenticationType, "authenticationType");
    this.expirationTime = Objects.requireNonNull(
        expirationTime, "expirationTime");
  }

  /**
   * Returns the session identifier.
   */
  public String getSessionId() {
    return sessionId;
  }

  /**
   * Returns the session token.
   */
  public String getSessionToken() {
    return sessionToken;
  }

  /**
   * Returns a defensive copy of the session key. Callers must not retain
   * the returned array beyond the immediate signing operation.
   */
  public byte[] getSessionKey() {
    return sessionKey.clone();
  }

  /**
   * Returns the session authentication type.
   */
  public String getAuthenticationType() {
    return authenticationType;
  }

  /**
   * Returns the session expiration time.
   */
  public Instant getExpirationTime() {
    return expirationTime;
  }

  /**
   * Returns a diagnostic string that intentionally omits the session key
   * and session token to prevent secret leakage into logs.
   */
  @Override
  public String toString() {
    return "SessionCredentials{"
        + "sessionId='" + sessionId + '\''
        + ", authenticationType='" + authenticationType + '\''
        + ", expirationTime=" + expirationTime
        + '}';
  }
}