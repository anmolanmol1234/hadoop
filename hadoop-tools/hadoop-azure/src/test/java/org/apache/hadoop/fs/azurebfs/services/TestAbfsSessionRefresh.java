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
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
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
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for proactive session refresh, Create Session retry, OAuth
 * fallback and operation-eligibility behavior in
 * {@link AbfsSessionManager}.
 * <p>
 * Verifies that the manager refreshes cached credentials before the
 * server-reported expiry, coalesces concurrent create and refresh
 * requests into a single wire call, preserves still-valid cached
 * credentials across refresh failures, retries Create Session on
 * recoverable server failures up to the configured maximum, does not
 * retry non-recoverable failures, and arms and releases the OAuth
 * fallback window correctly.
 * All time-sensitive behavior is driven through an injected {@link Clock}
 * so that tests are deterministic and fast: no wall-clock sleeps are
 * required to exercise expiry, refresh or fallback windows. Retry tests
 * set the retry interval to zero so that they execute in milliseconds
 * without real sleeps between attempts.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
public class TestAbfsSessionRefresh {
  private static final String ACCOUNT_NAME = "myaccount";
  private static final String AUTH_TYPE = "HMAC";
  private static final int REFRESH_THRESHOLD_SECONDS = 60;
  private static final int FALLBACK_DURATION_SECONDS = 300;
  private static final int DEFAULT_MAX_RETRY_COUNT = 3;
  /**
   * Bound for {@link GenericTestUtils#waitFor} polls, in milliseconds.
   */
  private static final int WAIT_TIMEOUT_MILLIS = 5_000;
  /**
   * Poll interval for {@link GenericTestUtils#waitFor}, in milliseconds.
   */
  private static final int WAIT_INTERVAL_MILLIS = 10;
  @Mock
  private AbfsClient client;
  @Mock
  private AbfsConfiguration configuration;
  @Mock
  private TracingContext tracingContext;
  private AutoCloseable mocks;

  /**
   * Every manager created by a test, closed in {@link #tearDown()}. The
   * manager owns a background refresh executor, so leaking instances
   * leaks threads across the suite.
   */
  private final List<AbfsSessionManager> managers = new ArrayList<>();

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
    for (AbfsSessionManager mgr : managers) {
      mgr.close();
    }
    managers.clear();
    if (mocks != null) {
      mocks.close();
    }
  }

  // =========================================================================
  // Proactive refresh
  // =========================================================================

  /**
   * Verify that a request inside the refresh-skew window returns the
   * still-valid cached credentials immediately and triggers exactly one
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
    // The refreshed session must itself sit outside the skew window, so
    // that observing the cache cannot trigger a further refresh.
    final Instant secondExpiry =
        insideSkew.plusSeconds(REFRESH_THRESHOLD_SECONDS + 300L);

    final AtomicInteger callCount = new AtomicInteger();
    doAnswer(inv -> {
      final int n = callCount.incrementAndGet();
      return n == 1
          ? newSession("token-original", firstExpiry)
          : newSession("token-refreshed", secondExpiry);
    }).when(client).createSession(any());

    final MutableClock clock = new MutableClock(t0);
    final AbfsSessionManager mgr = newManager(clock);

    final SessionKeyCredentials original =
        mgr.getSessionCredentials(tracingContext);
    assertThat(original.getSessionToken()).isEqualTo("token-original");

    // Enter the refresh-skew window and request again.
    clock.setTo(insideSkew);
    final SessionKeyCredentials duringSkew =
        mgr.getSessionCredentials(tracingContext);

    // The caller must observe the still-valid cached credentials
    // immediately, not the background-refresh result.
    assertThat(duringSkew).isSameAs(original);

    // Poll the cache directly: calling the public API here would be a
    // second observation that could itself schedule work.
    awaitCachedToken(mgr, "token-refreshed");

    final SessionKeyCredentials afterRefresh =
        mgr.getSessionCredentials(tracingContext);
    assertThat(afterRefresh).isNotSameAs(original);
    assertThat(afterRefresh.getSessionToken()).isEqualTo("token-refreshed");

    // The refresh guard must coalesce: exactly one create for the mint
    // plus one for the refresh.
    assertThat(callCount.get()).isEqualTo(2);
  }

  /**
   * Verify that the refreshed expiry is published together with the
   * refreshed credentials. Guards against the torn-read window that
   * exists when credentials and expiry live in separate atomics.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testRefreshPublishesCredentialsAndExpiryTogether()
      throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant firstExpiry = t0.plusSeconds(300);
    final Instant insideSkew = firstExpiry.minusSeconds(30);
    final Instant secondExpiry =
        insideSkew.plusSeconds(REFRESH_THRESHOLD_SECONDS + 300L);

    final AtomicInteger callCount = new AtomicInteger();
    doAnswer(inv -> callCount.incrementAndGet() == 1
        ? newSession("token-original", firstExpiry)
        : newSession("token-refreshed", secondExpiry))
        .when(client).createSession(any());

    final MutableClock clock = new MutableClock(t0);
    final AbfsSessionManager mgr = newManager(clock);

    mgr.getSessionCredentials(tracingContext);
    assertThat(mgr.getCachedExpiryForTesting()).isEqualTo(firstExpiry);

    clock.setTo(insideSkew);
    mgr.getSessionCredentials(tracingContext);

    awaitCachedToken(mgr, "token-refreshed");

    // Credentials and expiry must have moved as one snapshot.
    assertThat(mgr.getCachedCredentialsForTesting().getSessionToken())
        .isEqualTo("token-refreshed");
    assertThat(mgr.getCachedExpiryForTesting()).isEqualTo(secondExpiry);
  }

  /**
   * Verify that a request outside the refresh-skew window does not
   * trigger a background refresh. Guards against premature refresh that
   * would generate needless service load.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testRefreshDoesNotFireOutsideSkewWindow() throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant expiry = t0.plusSeconds(300);
    final Instant wellBeforeSkew =
        expiry.minusSeconds(REFRESH_THRESHOLD_SECONDS + 60L);

    when(client.createSession(any()))
        .thenReturn(newSession("token-original", expiry));

    final MutableClock clock = new MutableClock(t0);
    final AbfsSessionManager mgr = newManager(clock);

    mgr.getSessionCredentials(tracingContext);

    // Advance the clock, but stay outside the refresh window.
    clock.setTo(wellBeforeSkew);
    final SessionKeyCredentials cached =
        mgr.getSessionCredentials(tracingContext);
    assertThat(cached).isNotNull();

    // Give any background task a chance to run. If a refresh were
    // spawned, it would land here.
    Thread.sleep(100);

    verify(client, times(1)).createSession(any());
    assertThat(mgr.isRefreshPendingForTesting()).isFalse();
  }

  /**
   * Verify that the refresh fires at the exact skew boundary. The
   * manager treats {@code now == expiry - refreshSkew} as inside the
   * window, so the boundary instant must schedule a refresh.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testRefreshFiresExactlyAtSkewBoundary() throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant expiry = t0.plusSeconds(300);
    final Instant boundary =
        expiry.minusSeconds(REFRESH_THRESHOLD_SECONDS);
    final Instant secondExpiry =
        boundary.plusSeconds(REFRESH_THRESHOLD_SECONDS + 300L);

    final AtomicInteger callCount = new AtomicInteger();
    doAnswer(inv -> callCount.incrementAndGet() == 1
        ? newSession("token-original", expiry)
        : newSession("token-boundary", secondExpiry))
        .when(client).createSession(any());

    final MutableClock clock = new MutableClock(t0);
    final AbfsSessionManager mgr = newManager(clock);

    mgr.getSessionCredentials(tracingContext);

    clock.setTo(boundary);
    mgr.getSessionCredentials(tracingContext);

    awaitCachedToken(mgr, "token-boundary");
    assertThat(callCount.get()).isEqualTo(2);
  }

  /**
   * Verify that a background refresh failure preserves the still-valid
   * cached session. Callers must continue to observe the cached
   * credentials for as long as they have not truly expired.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testRefreshFailurePreservesStillValidCachedSession()
      throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant expiry = t0.plusSeconds(300);
    final Instant insideSkew = expiry.minusSeconds(30);

    final AtomicInteger callCount = new AtomicInteger();
    doAnswer(inv -> {
      if (callCount.incrementAndGet() == 1) {
        return newSession("token-original", expiry);
      }
      throw new AbfsDriverException("refresh failure",
          new RuntimeException("service transient error"));
    }).when(client).createSession(any());

    final MutableClock clock = new MutableClock(t0);
    final AbfsSessionManager mgr = newManager(clock);

    final SessionKeyCredentials original =
        mgr.getSessionCredentials(tracingContext);
    assertThat(original.getSessionToken()).isEqualTo("token-original");

    // Enter the refresh window; spawn the doomed background refresh.
    clock.setTo(insideSkew);
    final SessionKeyCredentials duringRefresh =
        mgr.getSessionCredentials(tracingContext);
    assertThat(duringRefresh).isSameAs(original);

    awaitRefreshSettled(mgr, callCount, 2);

    // The cache must still hold the original: a refresh failure is
    // best-effort and must not wipe still-valid credentials, nor arm
    // the fallback window.
    assertThat(mgr.getCachedCredentialsForTesting()).isSameAs(original);
    assertThat(mgr.getFallbackUntilForTesting()).isEqualTo(Instant.EPOCH);
  }

  /**
   * Verify that a failed background refresh does not permanently disable
   * subsequent refresh attempts. Once the first attempt has settled, the
   * next call inside the skew window must schedule another attempt.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSubsequentRefreshAttemptsAllowedAfterFailure()
      throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant expiry = t0.plusSeconds(300);
    final Instant insideSkew1 = expiry.minusSeconds(30);
    // A second later, still inside the skew window.
    final Instant insideSkew2 = insideSkew1.plusSeconds(1);

    final AtomicInteger callCount = new AtomicInteger();
    doAnswer(inv -> {
      final int n = callCount.incrementAndGet();
      if (n == 1) {
        return newSession("token-original", expiry);
      }
      if (n == 2) {
        throw new AbfsDriverException("first refresh failed",
            new RuntimeException("transient"));
      }
      return newSession("token-refreshed",
          insideSkew2.plusSeconds(REFRESH_THRESHOLD_SECONDS + 300L));
    }).when(client).createSession(any());

    final MutableClock clock = new MutableClock(t0);
    final AbfsSessionManager mgr = newManager(clock);

    mgr.getSessionCredentials(tracingContext);

    // First refresh attempt: fails.
    clock.setTo(insideSkew1);
    mgr.getSessionCredentials(tracingContext);

    // Wait for the attempt to run *and* for the refresh guard to be
    // released. Waiting only on the wire call would race the guard,
    // causing the second request below to skip scheduling.
    awaitRefreshSettled(mgr, callCount, 2);

    // Second refresh attempt must be allowed.
    clock.setTo(insideSkew2);
    mgr.getSessionCredentials(tracingContext);

    awaitCachedToken(mgr, "token-refreshed");
    assertThat(callCount.get()).isEqualTo(3);
  }

  /**
   * Verify that a burst of concurrent requests inside the refresh-skew
   * window is coalesced into exactly one background refresh. Without the
   * refresh guard, every request in the window schedules its own Create
   * Session call.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testConcurrentSkewRequestsTriggerSingleRefresh()
      throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant firstExpiry = t0.plusSeconds(300);
    final Instant insideSkew = firstExpiry.minusSeconds(30);
    final Instant secondExpiry =
        insideSkew.plusSeconds(REFRESH_THRESHOLD_SECONDS + 300L);

    final Semaphore holdRefresh = new Semaphore(0);
    final CountDownLatch refreshStarted = new CountDownLatch(1);
    final AtomicInteger callCount = new AtomicInteger();

    doAnswer(inv -> {
      if (callCount.incrementAndGet() == 1) {
        return newSession("token-original", firstExpiry);
      }
      refreshStarted.countDown();
      // Hold the refresh open for the duration of the burst so the
      // guard cannot be released and re-acquired mid-test.
      holdRefresh.acquire();
      return newSession("token-refreshed", secondExpiry);
    }).when(client).createSession(any());

    final MutableClock clock = new MutableClock(t0);
    final AbfsSessionManager mgr = newManager(clock);

    final SessionKeyCredentials original =
        mgr.getSessionCredentials(tracingContext);

    clock.setTo(insideSkew);

    final int workerThreads = 16;
    final CyclicBarrier startBarrier = new CyclicBarrier(workerThreads);
    final CountDownLatch done = new CountDownLatch(workerThreads);
    final AtomicReference<Throwable> firstError = new AtomicReference<>();

    final ExecutorService pool = daemonPool(workerThreads, "skew-burst");
    try {
      for (int i = 0; i < workerThreads; i++) {
        pool.submit(() -> {
          try {
            startBarrier.await();
            // Every caller must be served the cached credentials
            // without blocking on the in-flight refresh.
            assertThat(mgr.getSessionCredentials(tracingContext))
                .isSameAs(original);
          } catch (Throwable t) {
            firstError.compareAndSet(null, t);
          } finally {
            done.countDown();
          }
        });
      }
      assertThat(done.await(20, TimeUnit.SECONDS)).isTrue();
    } finally {
      holdRefresh.release(workerThreads + 1);
      shutdownPool(pool);
    }

    assertThat(firstError.get()).isNull();

    awaitCachedToken(mgr, "token-refreshed");
    assertThat(refreshStarted.getCount()).isZero();

    // One mint plus exactly one coalesced refresh, regardless of how
    // many callers observed the skew window.
    assertThat(callCount.get()).isEqualTo(2);
  }

  // =========================================================================
  // Create Session single-flight
  // =========================================================================

  /**
   * Verify that a burst of concurrent requests against a cold cache
   * issues exactly one Create Session call, with all other callers
   * joining the same in-flight future.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testConcurrentColdCacheRequestsIssueSingleCreate()
      throws Exception {
    final Instant expiry = Instant.now().plusSeconds(3600);
    final AtomicInteger callCount = new AtomicInteger();

    doAnswer(inv -> {
      callCount.incrementAndGet();
      // Stay in flight long enough for the other callers to arrive and
      // observe the single-flight future.
      Thread.sleep(200);
      return newSession("token-single-flight", expiry);
    }).when(client).createSession(any());

    final AbfsSessionManager mgr = newManager();

    final int workerThreads = 16;
    final CyclicBarrier startBarrier = new CyclicBarrier(workerThreads);
    final CountDownLatch done = new CountDownLatch(workerThreads);
    final AtomicReference<Throwable> firstError = new AtomicReference<>();
    final AtomicInteger nonNullResults = new AtomicInteger();

    final ExecutorService pool = daemonPool(workerThreads, "cold-burst");
    try {
      for (int i = 0; i < workerThreads; i++) {
        pool.submit(() -> {
          try {
            startBarrier.await();
            final SessionKeyCredentials creds =
                mgr.getSessionCredentials(tracingContext);
            if (creds != null) {
              nonNullResults.incrementAndGet();
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
    assertThat(nonNullResults.get()).isEqualTo(workerThreads);
    assertThat(callCount.get()).isEqualTo(1);
  }

  // =========================================================================
  // Cache lifetime
  // =========================================================================

  /**
   * Verify that a single Create Session response is reused across a long
   * series of {@code getSessionCredentials} calls until the cached
   * session actually expires. Proves that the cache is not silently
   * wiped by request volume or lifecycle events.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSessionPersistsUntilExactExpiry() throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant expiry = t0.plusSeconds(300);
    final Instant justBeforeSkew =
        expiry.minusSeconds(REFRESH_THRESHOLD_SECONDS + 1L);
    final Instant afterExpiry = expiry.plusSeconds(1);

    final AtomicInteger callCount = new AtomicInteger();
    doAnswer(inv -> callCount.incrementAndGet() == 1
        ? newSession("token-1", expiry)
        : newSession("token-2", expiry.plusSeconds(300)))
        .when(client).createSession(any());

    final MutableClock clock = new MutableClock(t0);
    final AbfsSessionManager mgr = newManager(clock);

    // Mint at t0.
    final SessionKeyCredentials first =
        mgr.getSessionCredentials(tracingContext);

    // Fifty requests spanning most of the session's lifetime: all must
    // be served from the same cached instance.
    final long stepSeconds =
        (justBeforeSkew.getEpochSecond() - t0.getEpochSecond()) / 50;
    for (int i = 0; i < 50; i++) {
      clock.setTo(t0.plusSeconds(stepSeconds * i));
      assertThat(mgr.getSessionCredentials(tracingContext))
          .isSameAs(first);
    }

    // Exactly one Create Session call issued so far.
    verify(client, times(1)).createSession(any());

    // At the expiry instant the cached session is considered expired:
    // the manager tests now.isBefore(expiry), and equality is "not
    // before", so the cache is invalid.
    clock.setTo(expiry);
    assertThat(mgr.getSessionCredentials(tracingContext).getSessionToken())
        .isEqualTo("token-2");
    verify(client, times(2)).createSession(any());

    // After expiry, calls continue to serve the freshly-minted second
    // session: no drift back to the first.
    clock.setTo(afterExpiry);
    assertThat(mgr.getSessionCredentials(tracingContext).getSessionToken())
        .isEqualTo("token-2");
  }

  // =========================================================================
  // Create Session retry
  // =========================================================================

  /**
   * Verify that a recoverable Create Session failure (503 ServerBusy) is
   * retried up to {@code getSessionMaxRetryCount()} times after the
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

    final AbfsSessionManager mgr = newManager();

    final AbfsRestOperationException ex = intercept(
        AbfsRestOperationException.class,
        () -> mgr.getSessionCredentials(tracingContext));
    assertThat(ex.getStatusCode()).isEqualTo(503);

    // maxRetryCount is the number of retries after the initial attempt,
    // so total wire calls = 1 + DEFAULT_MAX_RETRY_COUNT.
    verify(client, times(DEFAULT_MAX_RETRY_COUNT + 1))
        .createSession(any());
  }

  /**
   * Verify that Create Session succeeds on a later retry attempt when
   * the underlying service transitions from failing to healthy. The
   * total number of wire calls equals the number of failures plus one
   * for the eventual success, and the returned session carries the token
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
            Instant.now().plusSeconds(3600)));

    final AbfsSessionManager mgr = newManager();

    final SessionKeyCredentials creds =
        mgr.getSessionCredentials(tracingContext);

    assertThat(creds).isNotNull();
    assertThat(creds.getSessionToken()).isEqualTo("token-recovered");
    verify(client, times(3)).createSession(any());

    // A success on a retry must not leave the fallback window armed.
    assertThat(mgr.getFallbackUntilForTesting()).isEqualTo(Instant.EPOCH);
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

    final AbfsSessionManager mgr = newManager();

    final AbfsRestOperationException ex = intercept(
        AbfsRestOperationException.class,
        () -> mgr.getSessionCredentials(tracingContext));
    assertThat(ex.getStatusCode()).isEqualTo(403);

    verify(client, times(1)).createSession(any());
  }

  /**
   * Verify the boundary case where {@code maxRetryCount = 0}: exactly
   * one attempt is made, and any failure, recoverable or not, enters
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

    final AbfsSessionManager mgr = newManager();

    intercept(AbfsRestOperationException.class,
        () -> mgr.getSessionCredentials(tracingContext));

    verify(client, times(1)).createSession(any());
  }

  /**
   * Verify that a negative configured retry count is clamped to zero
   * rather than producing a degenerate retry loop. Exactly one attempt
   * must be made, and the original service failure must surface.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testNegativeRetryCountIsClampedToSingleAttempt()
      throws Exception {
    when(configuration.getSessionMaxRetryCount()).thenReturn(-1);
    when(client.createSession(any()))
        .thenThrow(newAbfsRestOperationException(503, "ServerBusy", "busy"));

    final AbfsSessionManager mgr = newManager();

    final AbfsRestOperationException ex = intercept(
        AbfsRestOperationException.class,
        () -> mgr.getSessionCredentials(tracingContext));
    assertThat(ex.getStatusCode()).isEqualTo(503);

    verify(client, times(1)).createSession(any());
  }

  /**
   * Verify that an unexpected {@link RuntimeException} from Create
   * Session is not retried and does not fail the caller's request. The
   * manager arms fallback and returns {@code null} so that the request
   * proceeds over OAuth.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testRuntimeExceptionFallsBackToOAuthWithoutRetry()
      throws Exception {
    when(configuration.getSessionMaxRetryCount())
        .thenReturn(DEFAULT_MAX_RETRY_COUNT);
    when(client.createSession(any()))
        .thenThrow(new IllegalStateException("unexpected parser state"));

    final AbfsSessionManager mgr = newManager();

    assertThat(mgr.getSessionCredentials(tracingContext)).isNull();

    verify(client, times(1)).createSession(any());
    assertThat(mgr.getFallbackUntilForTesting())
        .isAfter(Instant.EPOCH);
  }

  // =========================================================================
  // OAuth fallback window
  // =========================================================================

  /**
   * Verify that after retries are exhausted and the manager has entered
   * fallback, subsequent calls short-circuit without any additional wire
   * attempts. Combined with the retry-count test, this proves total wire
   * consumption is bounded by {@code maxRetryCount + 1} even under
   * sustained caller pressure.
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

    final AbfsSessionManager mgr = newManager();

    intercept(AbfsRestOperationException.class,
        () -> mgr.getSessionCredentials(tracingContext));

    // Twenty follow-up calls; all short-circuit through fallback.
    for (int i = 0; i < 20; i++) {
      assertThat(mgr.getSessionCredentials(tracingContext)).isNull();
    }

    // Total wire calls = maxRetryCount + 1 from the initial burst; the
    // fallback window absorbs everything after.
    verify(client, times(DEFAULT_MAX_RETRY_COUNT + 1))
        .createSession(any());
  }

  /**
   * Verify that the fallback window elapses: once the configured
   * duration has passed, the manager makes a fresh Create Session
   * attempt rather than serving OAuth indefinitely.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testFallbackWindowExpiresAndAllowsFreshAttempt()
      throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant afterWindow =
        t0.plusSeconds(FALLBACK_DURATION_SECONDS + 1L);

    final AtomicInteger callCount = new AtomicInteger();
    doAnswer(inv -> {
      if (callCount.incrementAndGet() == 1) {
        throw newAbfsRestOperationException(503, "ServerBusy", "busy");
      }
      return newSession("token-after-fallback",
          afterWindow.plusSeconds(3600));
    }).when(client).createSession(any());

    final MutableClock clock = new MutableClock(t0);
    final AbfsSessionManager mgr = newManager(clock);

    intercept(AbfsRestOperationException.class,
        () -> mgr.getSessionCredentials(tracingContext));
    assertThat(mgr.getSessionCredentials(tracingContext)).isNull();
    verify(client, times(1)).createSession(any());

    // Step past the fallback window.
    clock.setTo(afterWindow);
    final SessionKeyCredentials creds =
        mgr.getSessionCredentials(tracingContext);

    assertThat(creds).isNotNull();
    assertThat(creds.getSessionToken()).isEqualTo("token-after-fallback");
    verify(client, times(2)).createSession(any());
  }

  /**
   * Verify that a successful Create Session clears the fallback window
   * immediately rather than leaving it armed for the remainder of the
   * configured duration.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSuccessfulCreateClearsFallbackWindow() throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant afterWindow =
        t0.plusSeconds(FALLBACK_DURATION_SECONDS + 1L);

    final AtomicInteger callCount = new AtomicInteger();
    doAnswer(inv -> {
      if (callCount.incrementAndGet() == 1) {
        throw newAbfsRestOperationException(503, "ServerBusy", "busy");
      }
      return newSession("token-recovered", afterWindow.plusSeconds(3600));
    }).when(client).createSession(any());

    final MutableClock clock = new MutableClock(t0);
    final AbfsSessionManager mgr = newManager(clock);

    intercept(AbfsRestOperationException.class,
        () -> mgr.getSessionCredentials(tracingContext));
    assertThat(mgr.getFallbackUntilForTesting()).isAfter(Instant.EPOCH);

    clock.setTo(afterWindow);
    assertThat(mgr.getSessionCredentials(tracingContext)).isNotNull();

    // The window must be released, not merely elapsed.
    assertThat(mgr.getFallbackUntilForTesting()).isEqualTo(Instant.EPOCH);
  }

  // =========================================================================
  // Invalidation
  // =========================================================================

  /**
   * Verify that an invalidation issued while a Create Session call is in
   * flight is not undone by that call. The in-flight result may still be
   * handed to its own caller, but it must not repopulate the cache.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testInvalidationDuringInFlightCreateDoesNotRepopulateCache()
      throws Exception {
    final Instant expiry = Instant.now().plusSeconds(3600);
    final Semaphore holdCreate = new Semaphore(0);
    final CountDownLatch createStarted = new CountDownLatch(1);

    doAnswer(inv -> {
      createStarted.countDown();
      holdCreate.acquire();
      return newSession("token-in-flight", expiry);
    }).when(client).createSession(any());

    final AbfsSessionManager mgr = newManager();

    final AtomicReference<SessionKeyCredentials> result =
        new AtomicReference<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final CountDownLatch done = new CountDownLatch(1);

    final ExecutorService pool = daemonPool(1, "in-flight");
    try {
      pool.submit(() -> {
        try {
          result.set(mgr.getSessionCredentials(tracingContext));
        } catch (Throwable t) {
          error.set(t);
        } finally {
          done.countDown();
        }
      });

      assertThat(createStarted.await(5, TimeUnit.SECONDS)).isTrue();

      // Invalidate while the create is parked on the wire.
      mgr.invalidateCurrentSession();

      holdCreate.release();
      assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
    } finally {
      shutdownPool(pool);
    }

    assertThat(error.get()).isNull();
    // The originating caller still receives usable credentials.
    assertThat(result.get()).isNotNull();
    // But the invalidation must win for every subsequent caller.
    assertThat(mgr.getCachedCredentialsForTesting()).isNull();
    assertThat(mgr.getCachedExpiryForTesting()).isNull();
  }

  /**
   * Verify that when invalidation runs while a background refresh is in
   * flight, the manager reaches a stable state without deadlock. Either
   * the refresh completes or the invalidation wins; both are valid, and
   * the subsequent request must succeed cleanly.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testInvalidateDuringRefreshReachesStableState()
      throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant firstExpiry = t0.plusSeconds(300);
    final Instant insideSkew = firstExpiry.minusSeconds(30);
    final Instant secondExpiry =
        insideSkew.plusSeconds(REFRESH_THRESHOLD_SECONDS + 300L);

    final Semaphore holdRefresh = new Semaphore(0);
    final CountDownLatch refreshStarted = new CountDownLatch(1);
    final AtomicInteger callCount = new AtomicInteger();

    doAnswer(inv -> {
      final int n = callCount.incrementAndGet();
      if (n == 1) {
        return newSession("token-original", firstExpiry);
      }
      refreshStarted.countDown();
      holdRefresh.acquire();
      return newSession("token-refreshed-" + n, secondExpiry);
    }).when(client).createSession(any());

    final MutableClock clock = new MutableClock(t0);
    final AbfsSessionManager mgr = newManager(clock);

    mgr.getSessionCredentials(tracingContext);
    clock.setTo(insideSkew);
    mgr.getSessionCredentials(tracingContext);

    assertThat(refreshStarted.await(5, TimeUnit.SECONDS)).isTrue();
    mgr.invalidateCurrentSession();
    holdRefresh.release(2);

    // The invalidation raced an in-flight refresh, so the cache is
    // cleared; the next request must mint cleanly rather than hang or
    // surface a stale instance.
    final SessionKeyCredentials afterRace =
        mgr.getSessionCredentials(tracingContext);
    assertThat(afterRace).isNotNull();
    assertThat(afterRace.getSessionToken()).startsWith("token-");
  }

  /**
   * Verify that invalidating an empty cache is a no-op and that the next
   * request mints a fresh session.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testInvalidateOnEmptyCacheIsIdempotent() throws Exception {
    final Instant expiry = Instant.now().plusSeconds(3600);
    when(client.createSession(any()))
        .thenReturn(newSession("token-fresh", expiry));

    final AbfsSessionManager mgr = newManager();

    mgr.invalidateCurrentSession();
    mgr.invalidateCurrentSession();

    verify(client, never()).createSession(any());
    assertThat(mgr.getCachedCredentialsForTesting()).isNull();

    final SessionKeyCredentials creds =
        mgr.getSessionCredentials(tracingContext);
    assertThat(creds.getSessionToken()).isEqualTo("token-fresh");
    verify(client, times(1)).createSession(any());
  }

  /**
   * Verify that concurrent invalidation and get calls make forward
   * progress: no deadlocks, no unhandled exceptions, and every get
   * returns a definite result.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testConcurrentInvalidateAndGetCallsMakeForwardProgress()
      throws Exception {
    final Instant expiry = Instant.now().plusSeconds(3600);
    when(client.createSession(any()))
        .thenAnswer(inv -> newSession("token-cycle", expiry));

    final AbfsSessionManager mgr = newManager();

    final int workerThreads = 16;
    final int iterations = 50;
    final CyclicBarrier startBarrier = new CyclicBarrier(workerThreads);
    final AtomicInteger definedResults = new AtomicInteger();
    final AtomicReference<Throwable> firstError = new AtomicReference<>();

    final ExecutorService pool = daemonPool(workerThreads, "race");
    try {
      final CountDownLatch done = new CountDownLatch(workerThreads);
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

  // =========================================================================
  // Configuration gating and tracing
  // =========================================================================

  /**
   * Verify that a manager with session authentication disabled never
   * touches the wire and always defers to OAuth.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testDisabledSessionAuthNeverCallsCreateSession()
      throws Exception {
    when(configuration.isSessionAuthEnabled()).thenReturn(false);

    final AbfsSessionManager mgr = newManager();

    assertThat(mgr.isEnabled()).isFalse();
    assertThat(mgr.getSessionCredentials(tracingContext)).isNull();
    assertThat(mgr.isEligible(operation("GET",
        "https://myaccount.blob.core.windows.net/container/blob")))
        .isFalse();

    verify(client, never()).createSession(any());
  }

  /**
   * Verify that the caller's tracing context is propagated to the Create
   * Session call, so that a session-mint request can be correlated with
   * the user request that triggered it.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testTracingContextPropagatedToCreateSession()
      throws Exception {
    final Instant expiry = Instant.now().plusSeconds(3600);
    when(client.createSession(any()))
        .thenReturn(newSession("token-traced", expiry));

    final AbfsSessionManager mgr = newManager();
    mgr.getSessionCredentials(tracingContext);

    verify(client, times(1)).createSession(same(tracingContext));
  }

  // =========================================================================
  // Operation eligibility
  // =========================================================================

  /**
   * Verify that a plain blob-level GET is eligible for session
   * authentication.
   *
   * @throws Exception if the test URL cannot be constructed.
   */
  @Test
  public void testEligibilityAcceptsBlobLevelGet() throws Exception {
    assertThat(AbfsSessionManager.supportsSession(operation("GET",
        "https://myaccount.blob.core.windows.net/container/dir/blob.txt")))
        .isTrue();
  }

  /**
   * Verify that HEAD is rejected. HEAD is deliberately excluded from
   * session authentication and must continue to use OAuth; this test
   * exists so that the exclusion is not silently reverted.
   *
   * @throws Exception if the test URL cannot be constructed.
   */
  @Test
  public void testEligibilityRejectsHead() throws Exception {
    assertThat(AbfsSessionManager.supportsSession(operation("HEAD",
        "https://myaccount.blob.core.windows.net/container/blob")))
        .isFalse();
  }

  /**
   * Verify that mutating methods are rejected.
   *
   * @throws Exception if the test URL cannot be constructed.
   */
  @Test
  public void testEligibilityRejectsMutatingMethods() throws Exception {
    final String url =
        "https://myaccount.blob.core.windows.net/container/blob";
    for (String method : new String[] {"PUT", "POST", "DELETE", "PATCH"}) {
      assertThat(AbfsSessionManager.supportsSession(operation(method, url)))
          .as("method %s must not be session-eligible", method)
          .isFalse();
    }
  }

  /**
   * Verify that a {@code comp} query parameter disqualifies an
   * operation, in every position and with or without a value, while a
   * parameter that merely starts with {@code comp} does not.
   *
   * @throws Exception if the test URL cannot be constructed.
   */
  @Test
  public void testEligibilityHandlesCompParameterVariants()
      throws Exception {
    final String base =
        "https://myaccount.blob.core.windows.net/container/blob";

    // Disqualifying forms.
    assertThat(AbfsSessionManager.supportsSession(
        operation("GET", base + "?comp=list"))).isFalse();
    assertThat(AbfsSessionManager.supportsSession(
        operation("GET", base + "?restype=container&comp=list"))).isFalse();
    assertThat(AbfsSessionManager.supportsSession(
        operation("GET", base + "?COMP=metadata"))).isFalse();
    assertThat(AbfsSessionManager.supportsSession(
        operation("GET", base + "?comp&x=1"))).isFalse();

    // Not a comp parameter: must remain eligible.
    assertThat(AbfsSessionManager.supportsSession(
        operation("GET", base + "?composed=x"))).isTrue();
    assertThat(AbfsSessionManager.supportsSession(
        operation("GET", base + "?x=comp"))).isTrue();
    assertThat(AbfsSessionManager.supportsSession(
        operation("GET", base + "?x=1&composite=2"))).isTrue();
  }

  /**
   * Verify that container-level and directory-style paths are rejected:
   * session authentication applies to blobs only.
   *
   * @throws Exception if the test URL cannot be constructed.
   */
  @Test
  public void testEligibilityRejectsNonBlobPaths() throws Exception {
    final String host = "https://myaccount.blob.core.windows.net";

    // Container root, with and without a trailing slash.
    assertThat(AbfsSessionManager.supportsSession(
        operation("GET", host + "/container"))).isFalse();
    assertThat(AbfsSessionManager.supportsSession(
        operation("GET", host + "/container/"))).isFalse();

    // Account root.
    assertThat(AbfsSessionManager.supportsSession(
        operation("GET", host + "/"))).isFalse();

    // Directory-style path with a trailing slash.
    assertThat(AbfsSessionManager.supportsSession(
        operation("GET", host + "/container/dir/"))).isFalse();
  }

  /**
   * Verify the defensive null paths: a null operation and an operation
   * carrying a null URL are both ineligible rather than throwing.
   */
  @Test
  public void testEligibilityHandlesNullInputs() {
    assertThat(AbfsSessionManager.supportsSession(null)).isFalse();

    final AbfsRestOperation op = mock(AbfsRestOperation.class);
    when(op.getMethod()).thenReturn("GET");
    when(op.getUrl()).thenReturn(null);
    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  /**
   * Creates a manager backed by the supplied clock and registers it for
   * shutdown in {@link #tearDown()}.
   *
   * @param clock the simulated time source.
   * @return a registered session manager.
   */
  private AbfsSessionManager newManager(final Clock clock) {
    final AbfsSessionManager mgr =
        new AbfsSessionManager(client, configuration, clock);
    managers.add(mgr);
    return mgr;
  }

  /**
   * Creates a manager backed by the system clock and registers it for
   * shutdown in {@link #tearDown()}.
   *
   * @return a registered session manager.
   */
  private AbfsSessionManager newManager() {
    final AbfsSessionManager mgr =
        new AbfsSessionManager(client, configuration);
    managers.add(mgr);
    return mgr;
  }

  /**
   * Waits until the cached session carries the expected token. Polls the
   * cache directly rather than calling
   * {@code getSessionCredentials(...)}, because each such call is itself
   * an observation that could schedule further work and perturb the wire
   * call count under test.
   *
   * @param mgr the manager under test.
   * @param expectedToken the token the refreshed session should carry.
   * @throws Exception if the condition is not met within the timeout.
   */
  private static void awaitCachedToken(final AbfsSessionManager mgr,
      final String expectedToken) throws Exception {
    GenericTestUtils.waitFor(() -> {
      final SessionKeyCredentials cached =
          mgr.getCachedCredentialsForTesting();
      return cached != null
          && expectedToken.equals(cached.getSessionToken());
    }, WAIT_INTERVAL_MILLIS, WAIT_TIMEOUT_MILLIS);
  }

  /**
   * Waits until a background refresh has both issued its wire call and
   * released the refresh guard. Waiting on the call count alone races
   * the guard, because the guard is released only after the refresh task
   * unwinds.
   *
   * @param mgr the manager under test.
   * @param callCount counter incremented by the mocked Create Session.
   * @param expectedCalls the call count to wait for.
   * @throws Exception if the condition is not met within the timeout.
   */
  private static void awaitRefreshSettled(final AbfsSessionManager mgr,
      final AtomicInteger callCount, final int expectedCalls)
      throws Exception {
    GenericTestUtils.waitFor(
        () -> callCount.get() >= expectedCalls
            && !mgr.isRefreshPendingForTesting(),
        WAIT_INTERVAL_MILLIS, WAIT_TIMEOUT_MILLIS);
  }

  /**
   * Builds a mocked REST operation with the given method and URL.
   *
   * @param method the HTTP method.
   * @param url the request URL.
   * @return a mocked operation for eligibility checks.
   * @throws Exception if the URL is malformed.
   */
  private static AbfsRestOperation operation(final String method,
      final String url) throws Exception {
    final AbfsRestOperation op = mock(AbfsRestOperation.class);
    when(op.getMethod()).thenReturn(method);
    when(op.getUrl()).thenReturn(new URL(url));
    return op;
  }

  /**
   * Builds a Create Session response carrying the given token and
   * expiry.
   *
   * @param token the session token.
   * @param expiry the absolute expiry of the session.
   * @return the synthesized response.
   */
  private static SessionCredentials newSession(final String token,
      final Instant expiry) {
    return new SessionCredentials("id-" + token, token,
        (token + "-key").getBytes(), AUTH_TYPE, expiry);
  }

  /**
   * Builds a REST failure with the given status and error code.
   *
   * @param statusCode the HTTP status code.
   * @param errorCode the service error code.
   * @param message the error message.
   * @return the synthesized exception.
   */
  private static AbfsRestOperationException newAbfsRestOperationException(
      final int statusCode, final String errorCode, final String message) {
    return new AbfsRestOperationException(statusCode, errorCode, message,
        null /* innerException */);
  }

  /**
   * Builds a fixed-size executor pool whose worker threads are daemons,
   * so that the pool never blocks JVM exit even if a test fails before
   * calling {@link #shutdownPool(ExecutorService)}.
   *
   * @param size number of worker threads.
   * @param name label embedded in each thread's name for debugging.
   * @return a daemon-backed executor pool.
   */
  private static ExecutorService daemonPool(final int size,
      final String name) {
    final AtomicInteger id = new AtomicInteger();
    final ThreadFactory factory = r -> {
      final Thread t = new Thread(r);
      t.setDaemon(true);
      t.setName("refresh-test-" + name + "-" + id.incrementAndGet());
      return t;
    };
    return Executors.newFixedThreadPool(size, factory);
  }

  /**
   * Aggressively shuts down an executor pool: interrupts any running
   * tasks and waits a bounded time for termination. Called from
   * {@code finally} blocks so that it always runs, even after a failed
   * assertion.
   *
   * @param pool the pool to shut down.
   */
  private static void shutdownPool(final ExecutorService pool) {
    pool.shutdownNow();
    try {
      pool.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Test-only clock whose {@link #instant()} value can be advanced
   * atomically. Preferred over {@link Clock#fixed(Instant, ZoneId)}
   * because a single instance can simulate time progression across
   * multiple observations without allocating a new manager.
   */
  private static final class MutableClock extends Clock {

    /**
     * The current simulated instant.
     */
    private final AtomicReference<Instant> now;

    /**
     * Creates a clock positioned at the given instant.
     *
     * @param initial the starting instant.
     */
    MutableClock(final Instant initial) {
      this.now = new AtomicReference<>(initial);
    }

    /**
     * Moves the clock to the given instant.
     *
     * @param t the new simulated instant.
     */
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