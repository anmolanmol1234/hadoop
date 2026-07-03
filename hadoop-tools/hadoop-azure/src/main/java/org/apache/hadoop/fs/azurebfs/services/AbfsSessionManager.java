/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

/**
 * Manages the lifecycle of Azure Blob Storage sessions for an
 * {@link AbfsClient}.
 *
 * <p>Sessions are container-scoped. Since each {@link AbfsClient} is bound
 * to a single container, this manager caches at most one active session
 * at a time. The manager is responsible for creating a session on demand,
 * refreshing it before expiry, invalidating it on session-authentication
 * failures, and coordinating concurrent create requests so that only one
 * Create Session call is issued at any point in time.
 *
 * <p>When Create Session fails, the manager enters a temporary
 * OAuth-fallback state during which {@link #getSessionCredentials(TracingContext)}
 * returns {@code null}, causing the caller to authenticate the request
 * using the existing OAuth flow.
 */
public class AbfsSessionManager {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsSessionManager.class);

  private final AbfsClient client;

  private final boolean enabled;

  /**
   * Interval before session expiry at which a proactive refresh is
   * attempted. Corresponds to
   * {@code fs.azure.session.refresh.threshold.seconds}.
   */
  private final Duration refreshSkew;

  /**
   * Duration for which the manager stays in OAuth-fallback mode after a
   * Create Session failure. Corresponds to
   * {@code fs.azure.session.fallback.duration.seconds}.
   */
  private final Duration fallbackDuration;

  /**
   * Currently cached session credentials, or {@code null} if none exist,
   * the session was invalidated, or it has expired.
   */
  private final AtomicReference<SessionKeyCredentials> activeSessionRef =
      new AtomicReference<>();

  /**
   * Absolute expiry time of the currently cached session.
   */
  private final AtomicReference<Instant> activeSessionExpiryRef =
      new AtomicReference<>();

  /**
   * Single-flight guard for Create Session. The thread that
   * CAS-installs a future here performs the network call; all other
   * threads join the same future.
   */
  private final AtomicReference<CompletableFuture<SessionKeyCredentials>>
      inFlightRef = new AtomicReference<>();

  /**
   * Absolute time at which the OAuth-fallback window ends. Initialized
   * to {@link Instant#EPOCH} so the manager is not in fallback at
   * startup.
   */
  private final AtomicReference<Instant> fallbackUntilRef =
      new AtomicReference<>(Instant.EPOCH);

  /**
   * Constructs a session manager for the given client.
   *
   * @param client the owning {@link AbfsClient}.
   * @param configuration the ABFS configuration used to read session
   *     authentication settings.
   */
  public AbfsSessionManager(final AbfsClient client,
      final AbfsConfiguration configuration) {
    this.client = client;
    this.enabled = configuration.isSessionAuthEnabled();
    this.refreshSkew = Duration.ofSeconds(
        configuration.getSessionRefreshThresholdSeconds());
    this.fallbackDuration = Duration.ofSeconds(
        configuration.getSessionFallbackDurationSeconds());
  }

  /**
   * @return {@code true} if session authentication is enabled by
   *     configuration.
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Returns whether the given operation is eligible for session
   * authentication.
   *
   * @param op the operation about to be signed.
   * @return {@code true} if the operation may use session authentication.
   */
  public boolean isEligible(final AbfsRestOperation op) {
    return enabled;
  }

  /**
   * Returns credentials suitable for signing an outgoing request, or
   * {@code null} if the caller should authenticate the request using
   * OAuth.
   *
   * <p>The manager returns {@code null} when session authentication is
   * disabled, when the manager is in the OAuth-fallback window, or when
   * a Create Session call fails with an unexpected runtime error. When
   * the cached session is nearing expiry, a background refresh is
   * scheduled and the cached credentials are returned to avoid blocking
   * the request path.
   *
   * @param tracingContext tracing context associated with the request.
   * @return session credentials, or {@code null} to fall back to OAuth.
   * @throws AzureBlobFileSystemException if session creation fails with
   *     a driver-level ABFS exception.
   */
  public SessionKeyCredentials getSessionCredentials(
      final TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    if (!enabled) {
      return null;
    }
    if (inFallback()) {
      LOG.debug("Session manager is in fallback window; request will use "
          + "OAuth.");
      return null;
    }
    final SessionKeyCredentials cached = activeSessionRef.get();
    final Instant expiry = activeSessionExpiryRef.get();
    final Instant now = Instant.now();

    if (cached != null && expiry != null && now.isBefore(expiry)) {
      if (needsRefresh(now, expiry)) {
        // Cached credentials are still valid. Schedule a background
        // refresh so the next request receives fresh credentials.
        triggerAsyncRefresh(tracingContext);
      }
      return cached;
    }
    return awaitSessionCreation(tracingContext);
  }

  /**
   * Invalidates the currently cached session. This method should be
   * called when the service signals that the session is no longer usable,
   * for example a 401 response containing a {@code session_expiring}
   * status in the {@code WWW-Authenticate} or {@code x-ms-auth-info}
   * response header.
   *
   * <p>Repeated invocations are idempotent.
   */
  public void invalidateCurrentSession() {
    final SessionKeyCredentials prev = activeSessionRef.getAndSet(null);
    activeSessionExpiryRef.set(null);
    if (prev != null) {
      LOG.debug("Invalidated cached session with token {}",
          prev.getSessionToken());
    }
  }

  /**
   * @return {@code true} if the OAuth-fallback window is still active.
   */
  private boolean inFallback() {
    return Instant.now().isBefore(fallbackUntilRef.get());
  }

  /**
   * Enters the OAuth-fallback window after a Create Session failure.
   *
   * @param cause the failure that triggered the fallback.
   */
  private void enterFallback(final Throwable cause) {
    final Instant until = Instant.now().plus(fallbackDuration);
    fallbackUntilRef.set(until);
    LOG.warn("Entering session authentication fallback window until {} "
        + "due to: {}", until, cause.toString());
  }

  /**
   * @param now the current time.
   * @param expiry the session expiry time.
   * @return {@code true} if a proactive refresh should be attempted.
   */
  private boolean needsRefresh(final Instant now, final Instant expiry) {
    return now.isAfter(expiry.minus(refreshSkew));
  }

  /**
   * Schedules a proactive refresh of the cached session on the
   * ForkJoin common pool. The refresh is routed through
   * {@link #startOrJoinCreation(TracingContext)} to preserve single-flight
   * semantics.
   *
   * @param tracingContext tracing context propagated to the refresh call.
   */
  private void triggerAsyncRefresh(final TracingContext tracingContext) {
    CompletableFuture.runAsync(() -> {
      try {
        startOrJoinCreation(tracingContext).join();
      } catch (Throwable t) {
        // Best-effort refresh. The cached credentials remain valid; the
        // next request will either serve them or block on a fresh create.
        LOG.debug("Background session refresh failed.", t);
      }
    });
  }

  /**
   * Blocks the caller until the single-flight Create Session call
   * completes.
   *
   * <p>An {@link AzureBlobFileSystemException} is propagated to the
   * caller. Any other failure is logged and reported as {@code null} so
   * that the request falls back to OAuth rather than failing the user.
   *
   * @param tracingContext tracing context associated with the request.
   * @return session credentials, or {@code null} if creation failed with
   *     an unexpected runtime error.
   * @throws AzureBlobFileSystemException if Create Session fails with an
   *     ABFS-level exception.
   */
  private SessionKeyCredentials awaitSessionCreation(
      final TracingContext tracingContext)
      throws AzureBlobFileSystemException {

    final CompletableFuture<SessionKeyCredentials> future =
        startOrJoinCreation(tracingContext);

    try {
      return future.join();
    } catch (CompletionException ce) {
      final Throwable cause = ce.getCause() != null ? ce.getCause() : ce;
      if (cause instanceof AzureBlobFileSystemException) {
        throw (AzureBlobFileSystemException) cause;
      }
      LOG.debug("Session creation failed with an unexpected error; "
          + "request will use OAuth.", cause);
      return null;
    }
  }

  /**
   * Ensures that only a single Create Session call is issued at a time.
   *
   * <p>If a Create Session call is already in progress, the current
   * in-flight future is returned. Otherwise, the calling thread becomes
   * the creator: it installs a fresh future, executes Create Session,
   * completes the future, and clears the guard so the next expiry cycle
   * can start a new creation.
   *
   * <p>If the CAS installation loses the race, this method retries. The
   * retry terminates because the next iteration will either observe a
   * live future (and return it) or a cleared reference (and successfully
   * install a fresh future).
   *
   * @param tracingContext tracing context associated with the request.
   * @return the in-flight Create Session future.
   */
  private CompletableFuture<SessionKeyCredentials> startOrJoinCreation(
      final TracingContext tracingContext) {

    final CompletableFuture<SessionKeyCredentials> existing =
        inFlightRef.get();
    if (existing != null && !existing.isDone()) {
      return existing;
    }

    final CompletableFuture<SessionKeyCredentials> fresh =
        new CompletableFuture<>();
    if (!inFlightRef.compareAndSet(existing, fresh)) {
      return startOrJoinCreation(tracingContext);
    }

    try {
      final SessionKeyCredentials creds = doCreateSession(tracingContext);
      fresh.complete(creds);
    } catch (Throwable t) {
      fresh.completeExceptionally(t);
    } finally {
      // Clear the guard so the next expiry cycle can start a new create.
      inFlightRef.compareAndSet(fresh, null);
    }
    return fresh;
  }

  /**
   * Issues the Create Session call, caches the returned credentials, and
   * returns a {@link SessionKeyCredentials} for request signing. On any
   * failure the OAuth-fallback window is armed and the exception is
   * rethrown.
   *
   * @param tracingContext tracing context associated with the request.
   * @return session credentials returned by the Create Session API.
   * @throws AzureBlobFileSystemException if Create Session fails.
   */
  private SessionKeyCredentials doCreateSession(
      final TracingContext tracingContext)
      throws AzureBlobFileSystemException {

    LOG.debug("Creating new Blob Storage session.");
    final SessionCredentials sessionResponse;
    try {
      sessionResponse = client.createSession(tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      enterFallback(ex);
      throw ex;
    } catch (RuntimeException ex) {
      enterFallback(ex);
      throw ex;
    }

    final SessionKeyCredentials creds = new SessionKeyCredentials(
        client.getAccountName(),
        sessionResponse.getSessionToken(),
        sessionResponse.getSessionKey());

    activeSessionRef.set(creds);
    activeSessionExpiryRef.set(sessionResponse.getExpirationTime());

    LOG.debug("Cached new session with token {} expiring at {}",
        creds.getSessionToken(), sessionResponse.getExpirationTime());

    return creds;
  }
}
