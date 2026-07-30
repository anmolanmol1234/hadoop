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

import java.io.Closeable;
import java.net.URL;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

/**
 * Manages the lifecycle of Azure Blob Storage sessions for an
 * {@link AbfsClient}.
 * <p>
 * Sessions are container-scoped. Since each {@link AbfsClient} is bound to
 * a single container, this manager caches at most one active session at a
 * time. The manager creates a session on demand, refreshes it before
 * expiry, invalidates it on session-authentication failures, and
 * coordinates concurrent create requests so that only one Create Session
 * call is issued at any point in time.
 * <p>
 * Create Session is retried up to
 * {@code fs.azure.session.max.retry.count} times on recoverable failures
 * (5xx server errors, 408 request timeout, 429 rate limiting) before the
 * manager enters the OAuth-fallback window. Non-recoverable failures
 * (4xx client errors such as {@code FeatureNotEnabled}) enter fallback
 * immediately.
 * <p>
 * When Create Session fails permanently, the manager enters a temporary
 * OAuth-fallback state during which
 * {@link #getSessionCredentials(TracingContext)} returns {@code null},
 * causing the caller to authenticate the request using the existing OAuth
 * flow. Background-refresh failures that occur while a still-valid session
 * remains cached do not arm the fallback window: the cache continues to
 * serve requests until it truly expires. A subsequent successful Create
 * Session clears the window.
 * <p>
 * Thread safety: this class is thread-safe. The cached credentials and
 * their expiry are published together through a single immutable
 * {@link CachedSession} snapshot, so a reader can never observe new
 * credentials paired with a stale expiry. At most one Create Session call
 * is in flight at a time, and at most one background refresh is scheduled
 * at a time, so a burst of requests inside the refresh-skew window results
 * in exactly one additional Create Session call.
 * <p>
 * Instances hold a background executor and must be closed via
 * {@link #close()} when the owning {@link AbfsClient} is closed.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AbfsSessionManager implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsSessionManager.class);

  /**
   * Query parameter name that disqualifies an operation from session
   * authentication.
   */
  private static final String COMP_QUERY_PARAM = "comp";

  /**
   * Name of the background session-refresh thread.
   */
  private static final String REFRESH_THREAD_NAME = "abfs-session-refresh";

  /**
   * Owning client used to issue the Create Session call.
   */
  private final AbfsClient client;

  /**
   * Whether session authentication is enabled by configuration.
   */
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
   * cycle. Negative configured values are clamped to {@code 0}.
   */
  private final int maxRetryCount;

  /**
   * Delay between Create Session retry attempts. Corresponds to
   * {@code fs.azure.session.retry.interval.seconds}.
   */
  private final Duration retryInterval;

  /**
   * Time source used for all expiry and fallback-window decisions.
   * Injectable so that unit tests can control simulated time without
   * wall-clock sleeps. Production callers use {@link Clock#systemUTC()}.
   */
  private final Clock clock;

  /**
   * Currently cached session snapshot, or {@code null} if no session
   * exists or the session was invalidated.
   */
  private final AtomicReference<CachedSession> sessionRef =
      new AtomicReference<>();

  /**
   * Single-flight guard for Create Session. The thread that CAS-installs a
   * future here performs the network call; all other threads join the same
   * future.
   */
  private final AtomicReference<CompletableFuture<SessionKeyCredentials>>
      inFlightRef = new AtomicReference<>();

  /**
   * Guards background-refresh scheduling. Without this guard, every
   * request arriving inside the refresh-skew window would submit its own
   * refresh task, producing a burst of redundant Create Session calls once
   * the previous single-flight future completed.
   */
  private final AtomicBoolean refreshInProgress = new AtomicBoolean(false);

  /**
   * Monotonically increasing counter bumped by
   * {@link #invalidateCurrentSession()}. A Create Session call that was
   * already in flight when an invalidation occurred must not resurrect the
   * cache, so it compares the generation captured before the wire call
   * against the current value before publishing its result.
   */
  private final AtomicInteger invalidationGeneration = new AtomicInteger();

  /**
   * Absolute time at which the OAuth-fallback window ends. Initialized to
   * {@link Instant#EPOCH} so that the manager is not in fallback at
   * startup.
   */
  private final AtomicReference<Instant> fallbackUntilRef =
      new AtomicReference<>(Instant.EPOCH);

  /**
   * Dedicated executor for background refreshes. Create Session performs
   * blocking network I/O, which must not run on the ForkJoin common pool
   * where it can starve unrelated parallel work in the JVM. The single
   * worker thread is a daemon so that an unclosed manager cannot keep the
   * JVM alive.
   */
  private final ExecutorService refreshExecutor;

  /**
   * Set by {@link #close()} to stop scheduling further refreshes.
   */
  private volatile boolean closed;

  /**
   * Immutable snapshot of a cached session and its expiry. Publishing both
   * fields through a single reference removes the torn-read window that
   * exists when credentials and expiry are held in two separate atomics.
   */
  private static final class CachedSession {

    /**
     * Credentials used to sign requests.
     */
    private final SessionKeyCredentials credentials;

    /**
     * Absolute expiry time of {@link #credentials}.
     */
    private final Instant expiry;

    /**
     * Creates an immutable session snapshot.
     *
     * @param credentials credentials used to sign requests.
     * @param expiry absolute expiry time of the credentials.
     */
    CachedSession(final SessionKeyCredentials credentials,
        final Instant expiry) {
      this.credentials = credentials;
      this.expiry = expiry;
    }
  }

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
    this.maxRetryCount = Math.max(0,
        configuration.getSessionMaxRetryCount());
    this.retryInterval = Duration.ofSeconds(
        configuration.getSessionRetryIntervalSeconds());
    this.refreshExecutor = Executors.newSingleThreadExecutor(
        newDaemonThreadFactory());
  }

  /**
   * Creates the thread factory used by the background refresh executor.
   *
   * @return a thread factory producing a single named daemon thread.
   */
  private static ThreadFactory newDaemonThreadFactory() {
    return runnable -> {
      final Thread thread = new Thread(runnable, REFRESH_THREAD_NAME);
      thread.setDaemon(true);
      return thread;
    };
  }

  /**
   * Indicates whether session authentication is enabled.
   *
   * @return {@code true} if session authentication is enabled by
   *     configuration; {@code false} otherwise.
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Determines whether the given operation is eligible for session-based
   * authentication.
   * <p>
   * Session authentication is supported only for blob-level {@code GET}
   * requests that do not carry a {@code comp} query parameter.
   * {@code HEAD} is not supported and must continue to use OAuth.
   * <p>
   * An operation is eligible only if all of the following hold:
   * <ul>
   *   <li>the HTTP method is {@code GET};</li>
   *   <li>the request URL carries no {@code comp} query parameter;</li>
   *   <li>the request targets a blob ({@code /<container>/<blob>}), not a
   *   container and not a trailing-slash directory path.</li>
   * </ul>
   *
   * @param op the operation to evaluate; may be {@code null}.
   * @return {@code true} if the operation supports session
   *     authentication; {@code false} otherwise.
   */
  @VisibleForTesting
  static boolean supportsSession(final AbfsRestOperation op) {
    if (op == null) {
      return false;
    }

    // 1. Method must be GET. HEAD is intentionally excluded.
    final String method = op.getMethod();
    if (!AbfsHttpConstants.HTTP_METHOD_GET.equalsIgnoreCase(method)) {
      return false;
    }

    final URL url = op.getUrl();
    if (url == null) {
      return false;
    }

    // 2. A comp parameter denotes a sub-resource operation.
    final String query = url.getQuery();
    if (query != null && containsCompParam(query)) {
      return false;
    }

    // 3. The path must address a blob, not a container or a directory.
    String path = url.getPath();
    if (path == null || path.isEmpty()) {
      return false;
    }
    if (path.charAt(0) == '/') {
      path = path.substring(1);
    }
    if (path.isEmpty() || path.endsWith("/")) {
      return false;
    }

    final int slash = path.indexOf('/');
    return slash > 0 && slash < path.length() - 1;
  }

  /**
   * Determines whether a query string carries a {@code comp} parameter.
   * <p>
   * The parameter name is matched case-insensitively and only as a
   * complete query parameter name, which avoids false positives such as
   * {@code composed=...}. A valueless {@code comp} parameter, for example
   * {@code ?comp&x=1}, is also matched.
   *
   * @param query the raw query string; must not be {@code null}.
   * @return {@code true} if a {@code comp} parameter is present;
   *     {@code false} otherwise.
   */
  private static boolean containsCompParam(final String query) {
    final String lower = query.toLowerCase(Locale.ROOT);
    int idx = 0;
    while (idx < lower.length()) {
      int end = lower.indexOf('&', idx);
      if (end < 0) {
        end = lower.length();
      }
      final int eq = lower.indexOf('=', idx);
      final String name = (eq >= 0 && eq < end)
          ? lower.substring(idx, eq)
          : lower.substring(idx, end);
      if (COMP_QUERY_PARAM.equals(name.trim())) {
        return true;
      }
      idx = end + 1;
    }
    return false;
  }

  /**
   * Determines whether the given operation may be signed with session
   * credentials, taking configuration into account.
   *
   * @param op the operation about to be signed; may be {@code null}.
   * @return {@code true} if the operation may use session authentication;
   *     {@code false} otherwise.
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
   * <p>
   * The manager returns {@code null} when session authentication is
   * disabled, when the manager is inside the OAuth-fallback window, or
   * when a Create Session call fails with an unexpected runtime error.
   * When the cached session is nearing expiry, at most one background
   * refresh is scheduled and the cached credentials are returned
   * immediately, so that the request path is never blocked by a refresh.
   *
   * @param tracingContext tracing context associated with the request.
   * @return session credentials, or {@code null} if the caller should
   *     fall back to OAuth.
   * @throws AzureBlobFileSystemException if session creation fails with a
   *     driver-level ABFS exception.
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

    // Single load: credentials and expiry are always mutually consistent.
    final CachedSession cached = sessionRef.get();
    final Instant now = clock.instant();

    if (cached != null && now.isBefore(cached.expiry)) {
      if (needsRefresh(now, cached.expiry)) {
        // Cached credentials are still valid. Schedule a background
        // refresh so the next request receives fresh credentials.
        triggerAsyncRefresh(tracingContext);
      }
      return cached.credentials;
    }
    return awaitSessionCreation(tracingContext);
  }

  /**
   * Invalidates the currently cached session.
   * <p>
   * This method should be called when the service signals that the
   * session is no longer usable, for example a 401 response carrying a
   * {@code session_expiring} status in the {@code WWW-Authenticate} or
   * {@code x-ms-auth-info} response header.
   * <p>
   * The invalidation generation is bumped so that a Create Session call
   * that is already in flight cannot repopulate the cache with a session
   * minted before the invalidation point. Repeated invocations are
   * idempotent.
   */
  public void invalidateCurrentSession() {
    invalidationGeneration.incrementAndGet();
    final CachedSession prev = sessionRef.getAndSet(null);
    if (prev != null) {
      LOG.debug("Invalidated cached session that was due to expire at {}.",
          prev.expiry);
    }
  }

  /**
   * Determines whether the OAuth-fallback window is currently active.
   *
   * @return {@code true} if the fallback window has not yet elapsed;
   *     {@code false} otherwise.
   */
  private boolean inFallback() {
    return clock.instant().isBefore(fallbackUntilRef.get());
  }

  /**
   * Considers arming the OAuth-fallback window after a Create Session
   * failure.
   * <p>
   * If a still-valid cached session remains at the time of failure, the
   * fallback window is not armed and callers continue to be served cached
   * credentials until the session truly expires. This preserves the
   * best-effort contract of background refresh: a transient refresh
   * failure does not demote user requests to OAuth while the cache is
   * still usable.
   * <p>
   * Once the cached session actually expires, a blocking Create Session
   * attempt is made; if that also fails, this method is invoked again
   * with no valid cache and the fallback window arms normally. Cold-cache
   * failures always arm the window on the first failed attempt, because
   * there are no valid credentials to preserve.
   *
   * @param cause the failure that triggered the fallback consideration.
   */
  private void enterFallback(final Throwable cause) {
    final CachedSession cached = sessionRef.get();
    if (cached != null && clock.instant().isBefore(cached.expiry)) {
      LOG.debug("Create Session failed but cached session remains valid "
          + "until {}; not arming fallback window.", cached.expiry, cause);
      return;
    }
    final Instant until = clock.instant().plus(fallbackDuration);
    fallbackUntilRef.set(until);
    LOG.warn("Entering session authentication fallback window until {} "
        + "due to: {}", until, cause.toString());
  }

  /**
   * Clears the OAuth-fallback window after a successful Create Session
   * call. Without this, the manager would keep serving OAuth for the
   * remainder of the window even though session authentication has
   * demonstrably recovered.
   */
  private void exitFallback() {
    final Instant previous = fallbackUntilRef.getAndSet(Instant.EPOCH);
    if (previous.isAfter(Instant.EPOCH)) {
      LOG.debug("Create Session succeeded; clearing fallback window that "
          + "was set until {}.", previous);
    }
  }

  /**
   * Determines whether a proactive refresh should be attempted for a
   * session with the given expiry.
   *
   * @param now the current time.
   * @param expiry the absolute expiry time of the cached session.
   * @return {@code true} if the refresh-skew window has been entered;
   *     {@code false} otherwise.
   */
  private boolean needsRefresh(final Instant now, final Instant expiry) {
    return !now.isBefore(expiry.minus(refreshSkew));
  }

  /**
   * Schedules a proactive refresh of the cached session on the dedicated
   * refresh executor. The refresh is routed through
   * {@link #startOrJoinCreation(TracingContext)} so that single-flight
   * semantics are preserved.
   * <p>
   * At most one refresh is scheduled at a time. Requests that arrive
   * while a refresh is already pending return the cached credentials
   * without queueing another Create Session call. Failures are swallowed
   * because the refresh is best-effort.
   *
   * @param tracingContext tracing context propagated to the refresh call.
   */
  private void triggerAsyncRefresh(final TracingContext tracingContext) {
    if (closed) {
      return;
    }
    if (!refreshInProgress.compareAndSet(false, true)) {
      LOG.trace("Background session refresh already pending; skipping.");
      return;
    }

    try {
      refreshExecutor.execute(() -> {
        try {
          startOrJoinCreation(tracingContext).join();
        } catch (Throwable t) {
          // Best-effort refresh. The cached credentials remain valid; the
          // next request will either serve them or block on a fresh
          // create.
          LOG.debug("Background session refresh failed.", t);
        } finally {
          refreshInProgress.set(false);
        }
      });
    } catch (RuntimeException e) {
      // Rejected execution, for example during shutdown. Release the
      // guard so that a later request can retry.
      refreshInProgress.set(false);
      LOG.debug("Unable to schedule background session refresh.", e);
    }
  }

  /**
   * Blocks the caller until the single-flight Create Session call
   * completes.
   * <p>
   * An {@link AzureBlobFileSystemException} is propagated to the caller.
   * Any other failure is logged and reported as {@code null}, so that the
   * request falls back to OAuth rather than failing the user.
   *
   * @param tracingContext tracing context associated with the request.
   * @return session credentials, or {@code null} if creation failed with
   *     an unexpected runtime error or was cancelled.
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
    } catch (CancellationException ce) {
      LOG.debug("Session creation was cancelled; request will use OAuth.",
          ce);
      return null;
    }
  }

  /**
   * Ensures that only a single Create Session call is issued at a time.
   * <p>
   * If a Create Session call is already in progress, the current
   * in-flight future is returned. Otherwise the calling thread becomes
   * the creator: it installs a fresh future, executes Create Session,
   * completes the future, and clears the guard so that the next expiry
   * cycle can start a new creation.
   * <p>
   * The compare-and-set race is resolved with an iterative retry rather
   * than recursion, so heavy contention cannot exhaust the stack.
   *
   * @param tracingContext tracing context associated with the request.
   * @return the in-flight Create Session future; never {@code null}.
   */
  private CompletableFuture<SessionKeyCredentials> startOrJoinCreation(
      final TracingContext tracingContext) {

    while (true) {
      final CompletableFuture<SessionKeyCredentials> existing =
          inFlightRef.get();
      if (existing != null && !existing.isDone()) {
        return existing;
      }

      final CompletableFuture<SessionKeyCredentials> fresh =
          new CompletableFuture<>();
      if (!inFlightRef.compareAndSet(existing, fresh)) {
        // Lost the race; re-read and either join or install again.
        continue;
      }

      try {
        fresh.complete(doCreateSession(tracingContext));
      } catch (Throwable t) {
        fresh.completeExceptionally(t);
      } finally {
        // Clear the guard so the next expiry cycle can start a new
        // create.
        inFlightRef.compareAndSet(fresh, null);
      }
      return fresh;
    }
  }

  /**
   * Issues the Create Session call with retry on recoverable failures,
   * caches the returned credentials, and returns a
   * {@link SessionKeyCredentials} instance for request signing.
   * <p>
   * The result is published to the cache only if no invalidation occurred
   * while the call was in flight. A successful call also clears the
   * OAuth-fallback window.
   *
   * @param tracingContext tracing context associated with the request.
   * @return session credentials derived from the Create Session response.
   * @throws AzureBlobFileSystemException if Create Session fails after
   *     exhausting retries or on a non-recoverable failure.
   */
  private SessionKeyCredentials doCreateSession(
      final TracingContext tracingContext)
      throws AzureBlobFileSystemException {

    LOG.debug("Creating new Blob Storage session.");

    // Capture the invalidation generation before the wire call so that a
    // concurrent invalidateCurrentSession() cannot be undone by this
    // in-flight result.
    final int generation = invalidationGeneration.get();

    final SessionCredentials sessionResponse =
        callCreateSessionWithRetry(tracingContext);

    final SessionKeyCredentials creds = new SessionKeyCredentials(
        client.getAccountName(),
        sessionResponse.getSessionToken(),
        sessionResponse.getSessionKey());

    final Instant expiry = sessionResponse.getExpirationTime();

    if (invalidationGeneration.get() == generation) {
      sessionRef.set(new CachedSession(creds, expiry));
      LOG.debug("Cached new session expiring at {}.", expiry);
    } else {
      LOG.debug("Session was invalidated while Create Session was in "
          + "flight; not caching the result.");
    }

    // Session authentication has demonstrably recovered.
    exitFallback();

    return creds;
  }

  /**
   * Executes {@code client.createSession(...)} with retry on recoverable
   * failures. Mirrors the retry contract of the driver's OAuth token
   * acquisition in {@code AzureADAuthenticator.getTokenCall}.
   * <p>
   * The total number of wire attempts is bounded by
   * {@code maxRetryCount + 1}. Non-recoverable failures short-circuit the
   * loop, and runtime exceptions propagate immediately without retry. Any
   * final failure is passed to {@link #enterFallback(Throwable)}, which
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

    // maxRetryCount is clamped to >= 0 in the constructor, so at least one
    // attempt always runs and the loop can never fall through with a null
    // failure.
    final int maxAttempts = maxRetryCount + 1;

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return client.createSession(tracingContext);
      } catch (AbfsRestOperationException ex) {
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
        // Non-REST driver exception, for example an XML parse failure or a
        // wrapped network I/O error. Treat as recoverable up to the retry
        // budget.
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
        // Unexpected: do not retry; consider fallback and propagate.
        enterFallback(ex);
        throw ex;
      }
    }

    // Unreachable: the loop either returns on success or throws on the
    // final attempt, but the compiler requires a terminal statement.
    throw new IllegalStateException(
        "Create Session retry loop exited without a result.");
  }

  /**
   * Classifies an {@link AbfsRestOperationException} as recoverable or
   * permanent.
   * <p>
   * 5xx server errors, 408 Request Timeout and 429 Too Many Requests are
   * recoverable. 4xx client errors are treated as permanent for this
   * request, for example {@code FeatureNotEnabled},
   * {@code ContainerNotFound} and {@code InvalidQueryParameterValue}.
   *
   * @param ex the exception to classify.
   * @return {@code true} if the failure is worth retrying; {@code false}
   *     otherwise.
   */
  private static boolean isRecoverable(final AbfsRestOperationException ex) {
    final int status = ex.getStatusCode();
    return status >= 500 || status == 408 || status == 429;
  }

  /**
   * Sleeps for {@link #retryInterval} between Create Session retry
   * attempts. The interrupt flag is restored if the sleep is interrupted.
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

  /**
   * Shuts down the background refresh executor and stops scheduling
   * further refreshes. Should be invoked from {@code AbfsClient.close()}.
   * <p>
   * The worker thread is a daemon, so omitting this call cannot prevent
   * JVM exit, but closing releases the thread promptly. This method does
   * not throw and is safe to call more than once.
   */
  @Override
  public void close() {
    closed = true;
    refreshExecutor.shutdownNow();
  }

  /**
   * Returns the currently cached credentials without triggering creation
   * or refresh. Reserved for unit tests that need to observe cache state.
   *
   * @return the cached credentials, or {@code null} if no session is
   *     cached.
   */
  @VisibleForTesting
  SessionKeyCredentials getCachedCredentialsForTesting() {
    final CachedSession cached = sessionRef.get();
    return cached == null ? null : cached.credentials;
  }

  /**
   * Returns the expiry of the currently cached session without triggering
   * creation or refresh. Reserved for unit tests that need to observe
   * cache state.
   *
   * @return the expiry of the cached session, or {@code null} if no
   *     session is cached.
   */
  @VisibleForTesting
  Instant getCachedExpiryForTesting() {
    final CachedSession cached = sessionRef.get();
    return cached == null ? null : cached.expiry;
  }


  /**
   * Indicates whether a background refresh is currently scheduled or
   * running. Reserved for unit tests that must wait for a refresh to
   * settle before triggering the next one, since the guard is released
   * only after the refresh task unwinds.
   *
   * @return {@code true} if a refresh is pending; {@code false}
   *     otherwise.
   */
  @VisibleForTesting
  boolean isRefreshPendingForTesting() {
    return refreshInProgress.get();
  }

  /**
   * Returns the instant at which the OAuth-fallback window ends.
   * Reserved for unit tests that assert the window is armed or released.
   *
   * @return the fallback deadline, or {@link Instant#EPOCH} if the
   *     manager is not in fallback.
   */
  @VisibleForTesting
  Instant getFallbackUntilForTesting() {
    return fallbackUntilRef.get();
  }
}