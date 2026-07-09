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

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for proactive session refresh and Create Session retry
 * behavior in {@link AbfsSessionManager}.
 *
 * <p>Verifies that the manager refreshes cached credentials before the
 * server-reported expiry, correctly handles refresh failures while
 * preserving still-valid cached credentials, retries Create Session on
 * recoverable server failures up to the configured maximum, and does
 * not retry non-recoverable failures.
 *
 * <p>All time-sensitive behavior is driven through an injected
 * {@link Clock} so tests are deterministic and fast  no wall-clock
 * sleeps are required to exercise expiry or refresh windows. Retry
 * tests set the retry interval to zero so tests execute in
 * milliseconds without real sleeps between attempts.
 *
 * <p>Complements {@code TestAbfsSessionManager} (state-machine paths),
 * {@code TestAbfsSessionAuthFallback} (server failure fallback), and
 * {@code TestAbfsRestOperationSessionAuth} (auth-switch routing).
 *
 */
@Timeout(value = 20, unit = TimeUnit.SECONDS)
public class TestAbfsSessionRefresh {

  private static final String ACCOUNT_NAME = "myaccount";
  private static final String AUTH_TYPE = "HMAC";
  private static final int REFRESH_THRESHOLD_SECONDS = 60;
  private static final int FALLBACK_DURATION_SECONDS = 300;
  private static final int DEFAULT_MAX_RETRY_COUNT = 3;

  @Mock private AbfsClient client;
  @Mock private AbfsConfiguration configuration;
  @Mock private TracingContext tracingContext;

  private AutoCloseable mocks;

  @BeforeEach
  public void setUp() {
    mocks = MockitoAnnotations.openMocks(this);
    when(client.getAccountName()).thenReturn(ACCOUNT_NAME);
    when(configuration.isSessionAuthEnabled()).thenReturn(true);
    when(configuration.getSessionRefreshThresholdSeconds())
        .thenReturn(REFRESH_THRESHOLD_SECONDS);
    when(configuration.getSessionFallbackDurationSeconds())
        .thenReturn(FALLBACK_DURATION_SECONDS);
    // Refresh-focused tests use maxRetryCount = 0 so a single failure
    // surfaces cleanly without triggering the retry loop; retry tests
    // override to specific values inline.
    when(configuration.getSessionMaxRetryCount()).thenReturn(0);
    when(configuration.getSessionRetryIntervalSeconds()).thenReturn(0);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (mocks != null) {
      mocks.close();
    }
  }

  /**
   * Verify that a request inside the refresh-skew window returns the
   * still-valid cached credentials immediately and triggers a
   * background refresh that eventually replaces the cache with the
   * newly-minted session.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testRefreshReplacesCachedSessionAfterSkew() throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant firstExpiry = t0.plusSeconds(300);
    final Instant insideSkew = firstExpiry.minusSeconds(30);
    final Instant secondExpiry = insideSkew.plusSeconds(300);

    final CountDownLatch refreshDone = new CountDownLatch(1);
    final AtomicInteger callCount = new AtomicInteger();

    doAnswer(inv -> {
      int n = callCount.incrementAndGet();
      SessionCredentials creds = n == 1
          ? newSession("token-original", firstExpiry)
          : newSession("token-refreshed", secondExpiry);
      if (n >= 2) {
        refreshDone.countDown();
      }
      return creds;
    }).when(client).createSession(any());

    MutableClock clock = new MutableClock(t0);
    AbfsSessionManager mgr =
        new AbfsSessionManager(client, configuration, clock);

    // Mint the original session at t0.
    SessionKeyCredentials original =
        mgr.getSessionCredentials(tracingContext);
    assertThat(original.getSessionToken()).isEqualTo("token-original");

    // Advance into the refresh-skew window and request again.
    clock.setTo(insideSkew);
    SessionKeyCredentials duringSkew =
        mgr.getSessionCredentials(tracingContext);

    // The caller must observe the still-valid cached credentials
    // immediately, not the background-refresh result.
    assertThat(duringSkew).isSameAs(original);

    // Wait for the background refresh to finish.
    assertThat(refreshDone.await(2, TimeUnit.SECONDS)).isTrue();
    assertThat(callCount.get()).isEqualTo(2);

    // The next request must return the refreshed session a new
    // instance carrying the refreshed token.
    SessionKeyCredentials afterRefresh =
        mgr.getSessionCredentials(tracingContext);
    assertThat(afterRefresh).isNotSameAs(original);
    assertThat(afterRefresh.getSessionToken()).isEqualTo("token-refreshed");
  }

  /**
   * Verify that a request outside the refresh-skew window does not
   * trigger a background refresh. Guards against premature refresh
   * that would generate needless service load.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testRefreshDoesNotFireOutsideSkewWindow() throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant expiry = t0.plusSeconds(300);
    final Instant wellBeforeSkew =
        expiry.minusSeconds(REFRESH_THRESHOLD_SECONDS + 60);

    when(client.createSession(any()))
        .thenReturn(newSession("token-original", expiry));

    MutableClock clock = new MutableClock(t0);
    AbfsSessionManager mgr =
        new AbfsSessionManager(client, configuration, clock);

    mgr.getSessionCredentials(tracingContext);

    // Advance the clock, but stay outside the refresh window.
    clock.setTo(wellBeforeSkew);
    SessionKeyCredentials cached = mgr.getSessionCredentials(tracingContext);
    assertThat(cached).isNotNull();

    // Give any background task a chance to run. If a refresh were
    // spawned, it would land here.
    Thread.sleep(100);

    verify(client, times(1)).createSession(any());
  }

  /**
   * Verify that a background refresh failure preserves the still-valid
   * cached session. Callers must continue to observe the cached
   * credentials as long as they have not truly expired.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testRefreshFailurePreservesStillValidCachedSession()
      throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant expiry = t0.plusSeconds(300);
    final Instant insideSkew = expiry.minusSeconds(30);

    final CountDownLatch refreshAttempted = new CountDownLatch(1);
    final AtomicInteger callCount = new AtomicInteger();

    doAnswer(inv -> {
      int n = callCount.incrementAndGet();
      if (n == 1) {
        return newSession("token-original", expiry);
      }
      refreshAttempted.countDown();
      throw new AbfsDriverException(
          "refresh failure",
          new RuntimeException("service transient error"));
    }).when(client).createSession(any());

    MutableClock clock = new MutableClock(t0);
    AbfsSessionManager mgr =
        new AbfsSessionManager(client, configuration, clock);

    SessionKeyCredentials original =
        mgr.getSessionCredentials(tracingContext);
    assertThat(original.getSessionToken()).isEqualTo("token-original");

    // Enter the refresh window; spawn the doomed background refresh.
    clock.setTo(insideSkew);
    SessionKeyCredentials duringRefresh =
        mgr.getSessionCredentials(tracingContext);
    assertThat(duringRefresh).isSameAs(original);

    // Wait until the failed refresh is confirmed to have run.
    assertThat(refreshAttempted.await(2, TimeUnit.SECONDS)).isTrue();

    // Cache must still hold the original  refresh failure is
    // best-effort and must not wipe still-valid credentials.
    SessionKeyCredentials afterFailedRefresh =
        mgr.getSessionCredentials(tracingContext);
    assertThat(afterFailedRefresh).isSameAs(original);
  }

  /**
   * Verify that a failed background refresh does not permanently
   * disable subsequent refresh attempts. Even after one refresh
   * failure, the next call inside the skew window must trigger
   * another refresh attempt.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSubsequentRefreshAttemptsAllowedAfterFailure()
      throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant expiry = t0.plusSeconds(300);
    final Instant insideSkew1 = expiry.minusSeconds(30);
    // A second later still inside the skew window.
    final Instant insideSkew2 = insideSkew1.plusSeconds(1);

    final CountDownLatch firstRefreshDone = new CountDownLatch(1);
    final CountDownLatch secondRefreshDone = new CountDownLatch(1);
    final AtomicInteger callCount = new AtomicInteger();

    doAnswer(inv -> {
      int n = callCount.incrementAndGet();
      if (n == 1) {
        return newSession("token-original", expiry);
      }
      if (n == 2) {
        firstRefreshDone.countDown();
        throw new AbfsDriverException("first refresh failed",
            new RuntimeException("transient"));
      }
      secondRefreshDone.countDown();
      return newSession("token-refreshed",
          insideSkew2.plusSeconds(300));
    }).when(client).createSession(any());

    MutableClock clock = new MutableClock(t0);
    AbfsSessionManager mgr =
        new AbfsSessionManager(client, configuration, clock);

    mgr.getSessionCredentials(tracingContext);

    // First refresh attempt  fails.
    clock.setTo(insideSkew1);
    mgr.getSessionCredentials(tracingContext);
    assertThat(firstRefreshDone.await(2, TimeUnit.SECONDS)).isTrue();

    // Second refresh attempt must be allowed.
    clock.setTo(insideSkew2);
    mgr.getSessionCredentials(tracingContext);
    assertThat(secondRefreshDone.await(2, TimeUnit.SECONDS)).isTrue();

    assertThat(callCount.get()).isEqualTo(3);
  }

  /**
   * Verify that a single Create Session response is reused across a
   * long series of {@code getSessionCredentials} calls until the
   * cached session actually expires. Proves that the cache is not
   * silently wiped by request volume or lifecycle events.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSessionPersistsUntilExactExpiry() throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant expiry = t0.plusSeconds(300);
    final Instant justBeforeSkew =
        expiry.minusSeconds(REFRESH_THRESHOLD_SECONDS + 1);
    final Instant atExpiry = expiry;
    final Instant afterExpiry = expiry.plusSeconds(1);

    final AtomicInteger callCount = new AtomicInteger();
    doAnswer(inv -> {
      int n = callCount.incrementAndGet();
      if (n == 1) {
        return newSession("token-1", expiry);
      }
      return newSession("token-2", atExpiry.plusSeconds(300));
    }).when(client).createSession(any());

    MutableClock clock = new MutableClock(t0);
    AbfsSessionManager mgr =
        new AbfsSessionManager(client, configuration, clock);

    // Mint at t0.
    SessionKeyCredentials first =
        mgr.getSessionCredentials(tracingContext);

    // Fifty requests spanning most of the session's lifetime  all
    // must be served from the same cached instance.
    for (int i = 0; i < 50; i++) {
      long stepSeconds = (justBeforeSkew.getEpochSecond()
          - t0.getEpochSecond()) / 50;
      clock.setTo(t0.plusSeconds(stepSeconds * i));
      SessionKeyCredentials current =
          mgr.getSessionCredentials(tracingContext);
      assertThat(current).isSameAs(first);
    }

    // Exactly one Create Session call issued so far.
    verify(client, times(1)).createSession(any());

    // At the expiry instant, the cached session is considered expired
    // (the manager compares now.isBefore(expiry); equality is
    // "not before", so the cache is invalid).
    clock.setTo(atExpiry);
    SessionKeyCredentials afterExpiryCall =
        mgr.getSessionCredentials(tracingContext);
    assertThat(afterExpiryCall.getSessionToken()).isEqualTo("token-2");
    verify(client, times(2)).createSession(any());

    // After expiry, calls continue to serve the freshly-minted second
    // session  no drift back to the first.
    clock.setTo(afterExpiry);
    SessionKeyCredentials afterExpiryCall2 =
        mgr.getSessionCredentials(tracingContext);
    assertThat(afterExpiryCall2.getSessionToken()).isEqualTo("token-2");
  }

  /**
   * Verify that a recoverable Create Session failure (503 ServerBusy)
   * is retried up to {@code getSessionMaxRetryCount()} times after the
   * initial attempt before the manager gives up and enters fallback.
   * Total wire calls equal {@code maxRetryCount + 1}.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testCreateSessionRetriesRecoverableFailureUpToMaxCount()
      throws Exception {
    when(configuration.getSessionMaxRetryCount())
        .thenReturn(DEFAULT_MAX_RETRY_COUNT);
    when(client.createSession(any()))
        .thenThrow(newAbfsRestOperationException(503, "ServerBusy",
            "The server is busy."));

    AbfsSessionManager mgr =
        new AbfsSessionManager(client, configuration);

    try {
      mgr.getSessionCredentials(tracingContext);
    } catch (Exception ignored) {
      // Expected on final failure retries exhausted.
    }

    // maxRetryCount is the number of retries after the initial attempt.
    // Total wire calls = 1 (initial) + DEFAULT_MAX_RETRY_COUNT.
    verify(client, times(DEFAULT_MAX_RETRY_COUNT + 1))
        .createSession(any());
  }

  /**
   * Verify that Create Session succeeds on a later retry attempt when
   * the underlying service transitions from failing to healthy. The
   * total number of wire calls equals the number of failures plus one
   * for the eventual success. The returned session carries the token
   * minted by the successful attempt.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testCreateSessionSucceedsOnRetryAfterTransientFailure()
      throws Exception {
    when(configuration.getSessionMaxRetryCount())
        .thenReturn(DEFAULT_MAX_RETRY_COUNT);
    when(client.createSession(any()))
        .thenThrow(newAbfsRestOperationException(503, "ServerBusy", "busy"))
        .thenThrow(newAbfsRestOperationException(503, "ServerBusy", "busy"))
        .thenReturn(newSession("token-recovered",
            Instant.now().plusSeconds(300)));

    AbfsSessionManager mgr =
        new AbfsSessionManager(client, configuration);

    SessionKeyCredentials creds =
        mgr.getSessionCredentials(tracingContext);

    assertThat(creds).isNotNull();
    assertThat(creds.getSessionToken()).isEqualTo("token-recovered");
    verify(client, times(3)).createSession(any());
  }

  /**
   * Verify that a non-recoverable Create Session failure (a 4xx client
   * error such as {@code FeatureNotEnabled}) is not retried, even when
   * the configured retry budget is non-zero. The manager consumes
   * exactly one wire call and enters fallback immediately.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testCreateSessionDoesNotRetryNonRecoverableFailure()
      throws Exception {
    when(configuration.getSessionMaxRetryCount())
        .thenReturn(DEFAULT_MAX_RETRY_COUNT);
    when(client.createSession(any()))
        .thenThrow(newAbfsRestOperationException(403, "FeatureNotEnabled",
            "The feature is not enabled for this account."));

    AbfsSessionManager mgr =
        new AbfsSessionManager(client, configuration);

    try {
      mgr.getSessionCredentials(tracingContext);
    } catch (Exception ignored) {
      // Expected: non-recoverable failures short-circuit retries.
    }

    verify(client, times(1)).createSession(any());
  }

  /**
   * Verify the boundary case where {@code maxRetryCount = 0}: exactly
   * one attempt is made and any failure recoverable or not enters
   * fallback immediately.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testCreateSessionZeroRetryCountMakesExactlyOneAttempt()
      throws Exception {
    when(configuration.getSessionMaxRetryCount()).thenReturn(0);
    when(client.createSession(any()))
        .thenThrow(newAbfsRestOperationException(503, "ServerBusy", "busy"));

    AbfsSessionManager mgr =
        new AbfsSessionManager(client, configuration);

    try {
      mgr.getSessionCredentials(tracingContext);
    } catch (Exception ignored) {
      // Expected on the sole attempt.
    }

    verify(client, times(1)).createSession(any());
  }

  /**
   * Verify that after retries are exhausted and the manager has
   * entered fallback, subsequent calls short-circuit without any
   * additional wire attempts. Combined with the retry-count test,
   * this proves total wire consumption is bounded by
   * {@code maxRetryCount + 1} even under sustained caller pressure.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testFallbackWindowSuppressesAttemptsAfterRetriesExhausted()
      throws Exception {
    when(configuration.getSessionMaxRetryCount())
        .thenReturn(DEFAULT_MAX_RETRY_COUNT);
    when(client.createSession(any()))
        .thenThrow(newAbfsRestOperationException(503, "ServerBusy", "busy"));

    AbfsSessionManager mgr =
        new AbfsSessionManager(client, configuration);

    try {
      mgr.getSessionCredentials(tracingContext);
    } catch (Exception ignored) {
      // Expected on the first request retries exhausted.
    }

    // Twenty follow-up calls; all short-circuit through fallback.
    for (int i = 0; i < 20; i++) {
      assertThat(mgr.getSessionCredentials(tracingContext)).isNull();
    }

    // Total wire calls = maxRetryCount + 1 from the initial burst;
    // the fallback window absorbs everything after.
    verify(client, times(DEFAULT_MAX_RETRY_COUNT + 1))
        .createSession(any());
  }

  private static SessionCredentials newSession(final String token,
      final Instant expiry) {
    return new SessionCredentials("id-" + token, token,
        (token + "-key").getBytes(), AUTH_TYPE, expiry);
  }

  private static AbfsRestOperationException newAbfsRestOperationException(
      final int statusCode, final String errorCode, final String message) {
    return new AbfsRestOperationException(statusCode, errorCode, message,
        null /* innerException */);
  }

  /**
   * Build a fixed-size executor pool whose worker threads are daemons,
   * so the pool never blocks JVM exit even if a test fails before
   * calling {@link #shutdownPool}. Reserved for future concurrent
   * tests in this class.
   *
   * @param size number of worker threads.
   * @param name label embedded in each thread's name for debugging.
   * @return a daemon-backed executor pool.
   */
  @SuppressWarnings("unused")
  private static ExecutorService daemonPool(final int size,
      final String name) {
    final AtomicInteger id = new AtomicInteger();
    ThreadFactory factory = r -> {
      Thread t = new Thread(r);
      t.setDaemon(true);
      t.setName("refresh-test-" + name + "-" + id.incrementAndGet());
      return t;
    };
    return Executors.newFixedThreadPool(size, factory);
  }

  /**
   * Aggressively shut down an executor pool: interrupt any running
   * tasks and wait a bounded time for termination. Called from
   * {@code finally} blocks so it always runs, even after a failed
   * assertion.
   *
   * @param pool the pool to shut down.
   */
  @SuppressWarnings("unused")
  private static void shutdownPool(final ExecutorService pool) {
    pool.shutdownNow();
    try {
      pool.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  // =========================================================================
// Concurrent invalidation + refresh interleaving
// =========================================================================

  /**
   * Verify that when invalidation runs while a background refresh is
   * in flight, the manager reaches a stable state without deadlock.
   * Either the refresh completes or the invalidation wins — both are
   * valid outcomes, and the subsequent request must succeed cleanly.
   */
  @Test
  public void testInvalidateDuringRefreshReachesStableState()
      throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant firstExpiry = t0.plusSeconds(300);
    final Instant insideSkew = firstExpiry.minusSeconds(30);
    final Instant secondExpiry = insideSkew.plusSeconds(300);

    final Semaphore holdRefresh = new Semaphore(0);
    final CountDownLatch refreshStarted = new CountDownLatch(1);
    final AtomicInteger callCount = new AtomicInteger();

    doAnswer(inv -> {
      int n = callCount.incrementAndGet();
      if (n == 1) {
        return newSession("token-original", firstExpiry);
      }
      refreshStarted.countDown();
      holdRefresh.acquire();
      return newSession("token-refreshed-" + n, secondExpiry);
    }).when(client).createSession(any());

    MutableClock clock = new MutableClock(t0);
    AbfsSessionManager mgr =
        new AbfsSessionManager(client, configuration, clock);

    mgr.getSessionCredentials(tracingContext);
    clock.setTo(insideSkew);
    mgr.getSessionCredentials(tracingContext);

    assertThat(refreshStarted.await(2, TimeUnit.SECONDS)).isTrue();
    mgr.invalidateCurrentSession();
    holdRefresh.release();
    Thread.sleep(200);

    SessionKeyCredentials afterRace =
        mgr.getSessionCredentials(tracingContext);
    assertThat(afterRace).isNotNull();
    assertThat(afterRace.getSessionToken()).startsWith("token-");
  }

  /**
   * Verify that concurrent invalidation and get calls make forward
   * progress: no deadlocks, no unhandled exceptions, every get returns
   * a definite result.
   */
  @Test
  public void testConcurrentInvalidateAndGetCallsMakeForwardProgress()
      throws Exception {
    final Instant expiry = Instant.now().plusSeconds(300);
    when(client.createSession(any()))
        .thenAnswer(inv -> newSession("token-cycle", expiry));

    AbfsSessionManager mgr =
        new AbfsSessionManager(client, configuration);

    final int workerThreads = 16;
    final int iterations = 50;
    final java.util.concurrent.CyclicBarrier startBarrier =
        new java.util.concurrent.CyclicBarrier(workerThreads);
    final AtomicInteger definedResults = new AtomicInteger();
    final AtomicReference<Throwable> firstError = new AtomicReference<>();

    ExecutorService pool = daemonPool(workerThreads, "race");
    try {
      CountDownLatch done = new CountDownLatch(workerThreads);
      for (int i = 0; i < workerThreads; i++) {
        final boolean isInvalidator = i % 2 == 0;
        pool.submit(() -> {
          try {
            startBarrier.await();
            for (int j = 0; j < iterations; j++) {
              if (isInvalidator) {
                mgr.invalidateCurrentSession();
              } else {
                mgr.getSessionCredentials(tracingContext);
                definedResults.incrementAndGet();
              }
            }
          } catch (Throwable t) {
            firstError.compareAndSet(null, t);
          } finally {
            done.countDown();
          }
        });
      }

      assertThat(done.await(20, TimeUnit.SECONDS)).isTrue();
    } finally {
      shutdownPool(pool);
    }

    assertThat(firstError.get()).isNull();
    assertThat(definedResults.get())
        .isEqualTo(iterations * (workerThreads / 2));
  }

  /**
   * Test-only clock whose {@link #instant()} value can be advanced
   * atomically. Preferred over {@link Clock#fixed(Instant, ZoneId)}
   * because a single instance can simulate time progression across
   * multiple observations without allocating a new manager.
   */
  private static final class MutableClock extends Clock {
    private final AtomicReference<Instant> now;

    MutableClock(final Instant initial) {
      this.now = new AtomicReference<>(initial);
    }

    void setTo(final Instant t) {
      now.set(t);
    }

    @Override
    public Instant instant() {
      return now.get();
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(final ZoneId zone) {
      return this;
    }
  }
}