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

import java.net.URL;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
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
 * <p>Create Session is retried up to
 * {@code fs.azure.session.max.retry.count} times on recoverable failures
 * (5xx server errors, 408 request timeout, 429 rate limiting) before the
 * manager enters the OAuth-fallback window. Non-recoverable failures
 * (4xx client errors like {@code FeatureNotEnabled}) enter fallback
 * immediately.
 *
 * <p>When Create Session fails permanently, the manager enters a
 * temporary OAuth-fallback state during which
 * {@link #getSessionCredentials(TracingContext)} returns {@code null},
 * causing the caller to authenticate the request using the existing
 * OAuth flow. Background-refresh failures that occur while a still-valid
 * session remains cached do not arm the fallback window  the cache
 * continues to serve requests until it truly expires.
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
   * Maximum number of retries after the initial Create Session attempt
   * before the manager enters fallback. Corresponds to
   * {@code fs.azure.session.max.retry.count}. A value of {@code 0}
   * disables retries: exactly one attempt is made per Create Session
   * cycle.
   */
  private final int maxRetryCount;

  /**
   * Delay between Create Session retry attempts. Corresponds to
   * {@code fs.azure.session.retry.interval.seconds}.
   */
  private final Duration retryInterval;

  /**
   * Time source used for all expiry and fallback-window decisions.
   * Injectable so unit tests can control simulated time without wall-clock
   * sleeps. Production callers use {@link Clock#systemUTC()}.
   */
  private final Clock clock;

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
   * Constructs a session manager backed by the system UTC clock.
   *
   * @param client the owning {@link AbfsClient}.
   * @param configuration the ABFS configuration used to read session
   *     authentication settings.
   */
  public AbfsSessionManager(final AbfsClient client,
      final AbfsConfiguration configuration) {
    this(client, configuration, Clock.systemUTC());
  }

  /**
   * Constructs a session manager backed by the supplied clock. Reserved
   * for unit tests that need to control simulated time deterministically.
   *
   * @param client the owning {@link AbfsClient}.
   * @param configuration the ABFS configuration used to read session
   *     authentication settings.
   * @param clock time source for expiry and fallback-window decisions.
   */
  @VisibleForTesting
  AbfsSessionManager(final AbfsClient client,
      final AbfsConfiguration configuration,
      final Clock clock) {
    this.client = client;
    this.clock = clock;
    this.enabled = configuration.isSessionAuthEnabled();
    this.refreshSkew = Duration.ofSeconds(
        configuration.getSessionRefreshThresholdSeconds());
    this.fallbackDuration = Duration.ofSeconds(
        configuration.getSessionFallbackDurationSeconds());
    this.maxRetryCount = configuration.getSessionMaxRetryCount();
    this.retryInterval = Duration.ofSeconds(
        configuration.getSessionRetryIntervalSeconds());
  }

  /**
   * @return {@code true} if session authentication is enabled by
   *     configuration.
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Returns whether the given operation is eligible for session-based
   * authentication.
   *
   * <p>Session authentication is supported only for blob-level GET or
   * HEAD requests that do not include a {@code comp} query parameter.
   * Operations that do not meet these requirements must use OAuth
   * authentication.
   *
   * <p>An operation is eligible only if:
   * <ul>
   *   <li>The HTTP method is {@code GET} or {@code HEAD}.</li>
   *   <li>The request URL does not contain a {@code comp} query parameter.</li>
   *   <li>The request targets a blob
   *       ({@code /<container>/<blob>}), not a container.</li>
   * </ul>
   *
   * @param op the operation to evaluate; may be {@code null}
   * @return {@code true} if the operation supports session authentication;
   *         {@code false} otherwise
   */
  @VisibleForTesting
  static boolean supportsSession(final AbfsRestOperation op) {
    if (op == null) {
      return false;
    }
    // 1. Method must be GET or HEAD.
    final String method = op.getMethod();
    if (!AbfsHttpConstants.HTTP_METHOD_GET.equalsIgnoreCase(method)
        && !AbfsHttpConstants.HTTP_METHOD_HEAD.equalsIgnoreCase(method)) {
      return false;
    }
    final URL url = op.getUrl();
    if (url == null) {
      return false;
    }
    final String query = url.getQuery();
    if (query != null && containsCompParam(query)) {
      return false;
    }
    String path = url.getPath();
    if (path == null || path.isEmpty()) {
      return false;
    }
    if (path.charAt(0) == '/') {
      path = path.substring(1);
    }
    final int slash = path.indexOf('/');
    if (slash <= 0 || slash == path.length() - 1) {
      return false;
    }
    return true;
  }

  /**
   * Returns whether the query string contains a {@code comp} parameter.
   *
   * <p>The parameter name is matched case-insensitively and only as a
   * complete query parameter name, avoiding false positives such as
   * {@code composed=...}.
   *
   * @param query the raw query string
   * @return {@code true} if a {@code comp} parameter is present;
   *         {@code false} otherwise
   */
  private static boolean containsCompParam(final String query) {
    final String lower = query.toLowerCase(Locale.ROOT);
    int idx = 0;
    while (idx < lower.length()) {
      final int hit = lower.indexOf("comp=", idx);
      if (hit < 0) {
        return false;
      }
      if (hit == 0 || lower.charAt(hit - 1) == '&') {
        return true;
      }
      idx = hit + 1;
    }
    return false;
  }

  /**
   * Returns whether the given operation is eligible for session
   * authentication.
   *
   * @param op the operation about to be signed.
   * @return {@code true} if the operation may use session authentication.
   */
  public boolean isEligible(final AbfsRestOperation op) {
    if (!enabled) {
      return false;
    }
    return supportsSession(op);
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
    final Instant now = clock.instant();

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
    return clock.instant().isBefore(fallbackUntilRef.get());
  }

  /**
   * Considers arming the OAuth-fallback window after a Create Session
   * failure.
   *
   * <p>If a still-valid cached session remains at the time of failure,
   * the fallback window is not armed. Callers continue to serve cached
   * credentials until the session truly expires. This preserves the
   * best-effort contract of background refresh: a transient refresh
   * failure does not demote user requests to OAuth while the cache is
   * still usable.
   *
   * <p>Once the cached session actually expires, a blocking Create
   * Session attempt is made; if that also fails, this method is called
   * again with no valid cache  and the fallback window arms
   * normally. Cold-cache failures always arm the window on the first
   * failed attempt because there are no valid credentials to preserve.
   *
   * @param cause the failure that triggered the fallback consideration.
   */
  private void enterFallback(final Throwable cause) {
    final SessionKeyCredentials cached = activeSessionRef.get();
    final Instant expiry = activeSessionExpiryRef.get();
    if (cached != null && expiry != null
        && clock.instant().isBefore(expiry)) {
      LOG.debug("Create Session failed but cached session remains valid "
          + "until {}; not arming fallback window.", expiry, cause);
      return;
    }
    final Instant until = clock.instant().plus(fallbackDuration);
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
   * Issues the Create Session call with retry on recoverable failures,
   * caches the returned credentials, and returns a
   * {@link SessionKeyCredentials} for request signing.
   *
   * <p>Recoverable failures (5xx server errors, 408 request timeout,
   * 429 rate limiting) are retried up to {@link #maxRetryCount} times
   * with {@link #retryInterval} between attempts. Non-recoverable
   * failures (4xx client errors) short-circuit the retry loop.
   *
   * <p>Once retries are exhausted or a non-recoverable failure occurs,
   * the manager considers arming the OAuth-fallback window via
   * {@link #enterFallback} and rethrows the last exception.
   *
   * @param tracingContext tracing context associated with the request.
   * @return session credentials returned by the Create Session API.
   * @throws AzureBlobFileSystemException if Create Session fails after
   *     exhausting retries or on a non-recoverable failure.
   */
  private SessionKeyCredentials doCreateSession(
      final TracingContext tracingContext)
      throws AzureBlobFileSystemException {

    LOG.debug("Creating new Blob Storage session.");
    final SessionCredentials sessionResponse =
        callCreateSessionWithRetry(tracingContext);

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

  /**
   * Executes {@code client.createSession(...)} with retry on
   * recoverable failures. Mirrors the retry contract of the driver's
   * OAuth token acquisition in {@code AzureADAuthenticator.getTokenCall}.
   *
   * <p>The total number of wire attempts is bounded by
   * {@code maxRetryCount + 1}. Non-recoverable failures short-circuit
   * the loop. Runtime exceptions propagate immediately without retry.
   * Any final failure is passed to {@link #enterFallback}, which
   * decides whether to arm the fallback window based on whether a
   * still-valid cached session remains.
   *
   * @param tracingContext tracing context propagated to each attempt.
   * @return the successful Create Session response.
   * @throws AzureBlobFileSystemException if all attempts fail or a
   *     non-recoverable failure occurs.
   */
  private SessionCredentials callCreateSessionWithRetry(
      final TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final int maxAttempts = maxRetryCount + 1;
    AzureBlobFileSystemException lastFailure = null;

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return client.createSession(tracingContext);
      } catch (AbfsRestOperationException ex) {
        lastFailure = ex;
        if (!isRecoverable(ex) || attempt == maxAttempts) {
          LOG.debug("Create Session attempt {}/{} failed with status {}: "
              + "giving up.", attempt, maxAttempts, ex.getStatusCode(), ex);
          enterFallback(ex);
          throw ex;
        }
        LOG.debug("Create Session attempt {}/{} failed with status {}: "
            + "retrying.", attempt, maxAttempts, ex.getStatusCode(), ex);
        sleepBetweenRetries();
      } catch (AzureBlobFileSystemException ex) {
        // Non-REST driver exception (e.g. XML parse, network I/O wrap).
        // Treat as recoverable up to the retry budget.
        lastFailure = ex;
        if (attempt == maxAttempts) {
          LOG.debug("Create Session attempt {}/{} failed: giving up.",
              attempt, maxAttempts, ex);
          enterFallback(ex);
          throw ex;
        }
        LOG.debug("Create Session attempt {}/{} failed: retrying.",
            attempt, maxAttempts, ex);
        sleepBetweenRetries();
      } catch (RuntimeException ex) {
        // Unexpected  do not retry; consider fallback and propagate.
        enterFallback(ex);
        throw ex;
      }
    }

    // Unreachable the loop either returns on success or throws on the
    // final attempt but the compiler requires a terminal statement.
    throw lastFailure;
  }

  /**
   * Whether an {@link AbfsRestOperationException} represents a
   * recoverable failure worth retrying.
   *
   * <p>5xx server errors, 408 Request Timeout, and 429 Too Many Requests
   * are recoverable. 4xx client errors are treated as permanent for
   * this request (for example, {@code FeatureNotEnabled},
   * {@code ContainerNotFound}, {@code InvalidQueryParameterValue}).
   *
   * @param ex the exception to classify.
   * @return {@code true} if the failure is worth retrying.
   */
  private static boolean isRecoverable(final AbfsRestOperationException ex) {
    final int status = ex.getStatusCode();
    return status >= 500 || status == 408 || status == 429;
  }

  /**
   * Sleeps for {@link #retryInterval} between Create Session retry
   * attempts. Restores the interrupt flag if the sleep is interrupted.
   */
  private void sleepBetweenRetries() {
    if (retryInterval.isZero() || retryInterval.isNegative()) {
      return;
    }
    try {
      TimeUnit.MILLISECONDS.sleep(retryInterval.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}