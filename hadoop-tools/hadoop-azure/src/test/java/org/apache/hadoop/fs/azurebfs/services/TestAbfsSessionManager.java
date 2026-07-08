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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link AbfsSessionManager}: feature-gating, cache
 * behavior, invalidation, expiry, OAuth fallback, single-flight
 * concurrency, time-sensitive behavior via injected {@link Clock},
 * and the {@code supportsSession} eligibility rule.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
public class TestAbfsSessionManager {

  private static final String ACCOUNT_NAME = "myaccount";
  private static final String SESSION_ID = "session-id";
  private static final String SESSION_TOKEN = "session-token";
  private static final byte[] SESSION_KEY = "session-key-bytes".getBytes();
  private static final String AUTH_TYPE = "HMAC";

  private static final int REFRESH_THRESHOLD_SECONDS = 60;
  private static final int FALLBACK_DURATION_SECONDS = 300;

  @Mock private AbfsClient client;
  @Mock private AbfsConfiguration configuration;
  @Mock private TracingContext tracingContext;
  @Mock private AbfsRestOperation restOp;

  private AbfsSessionManager manager;
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
    manager = new AbfsSessionManager(client, configuration);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (mocks != null) {
      mocks.close();
    }
  }

  /** Disabled feature must short-circuit without calling the client. */
  @Test
  public void testFeatureDisabledReturnsNullWithoutCallingClient()
      throws Exception {
    when(configuration.isSessionAuthEnabled()).thenReturn(false);
    manager = new AbfsSessionManager(client, configuration);
    assertThat(manager.isEnabled()).isFalse();
    assertThat(manager.isEligible(restOp)).isFalse();
    assertThat(manager.getSessionCredentials(tracingContext)).isNull();
    verify(client, never()).createSession(any());
  }

  /** Enabled feature reports eligible for a plain operation. */
  @Test
  public void testFeatureEnabledIsEligibleReturnsTrue() throws Exception {
    // Stub the URL/method so supportsSession classifies this as eligible.
    when(restOp.getMethod()).thenReturn("GET");
    when(restOp.getUrl()).thenReturn(
        new URL("https://acct.blob.core.windows.net/mycontainer/myblob"));

    assertThat(manager.isEnabled()).isTrue();
    assertThat(manager.isEligible(restOp)).isTrue();
  }

  /** First call fires exactly one Create Session. */
  @Test
  public void testColdCacheTriggersCreateSession() throws Exception {
    when(client.createSession(tracingContext))
        .thenReturn(newSession(Instant.now().plusSeconds(300)));

    SessionKeyCredentials creds = manager.getSessionCredentials(tracingContext);

    assertThat(creds).isNotNull();
    assertThat(creds.getSessionToken()).isEqualTo(SESSION_TOKEN);
    verify(client, times(1)).createSession(tracingContext);
  }

  /** Warm cache returns the same instance without hitting the service. */
  @Test
  public void testWarmCacheReusesSession() throws Exception {
    when(client.createSession(tracingContext))
        .thenReturn(newSession(Instant.now().plusSeconds(300)));

    SessionKeyCredentials first = manager.getSessionCredentials(tracingContext);
    SessionKeyCredentials second = manager.getSessionCredentials(tracingContext);

    assertThat(second).isSameAs(first);
    verify(client, times(1)).createSession(tracingContext);
  }

  /** After invalidation the next call must re-create. */
  @Test
  public void testInvalidateForcesFreshCreateOnNextCall() throws Exception {
    when(client.createSession(tracingContext))
        .thenReturn(newSession(Instant.now().plusSeconds(300)),
            newSession(Instant.now().plusSeconds(300)));

    manager.getSessionCredentials(tracingContext);
    manager.invalidateCurrentSession();
    manager.getSessionCredentials(tracingContext);

    verify(client, times(2)).createSession(tracingContext);
  }

  /** Repeated invalidations are idempotent. */
  @Test
  public void testInvalidateIsIdempotent() throws Exception {
    when(client.createSession(tracingContext))
        .thenReturn(newSession(Instant.now().plusSeconds(300)));

    manager.getSessionCredentials(tracingContext);
    manager.invalidateCurrentSession();
    manager.invalidateCurrentSession();
    manager.invalidateCurrentSession();

    verify(client, times(1)).createSession(tracingContext);
  }

  /** Invalidation on an empty cache is a silent no-op. */
  @Test
  public void testInvalidateOnEmptyCacheIsNoOp() throws AzureBlobFileSystemException {
    manager.invalidateCurrentSession();

    verify(client, never()).createSession(any());
  }

  /** Expired cached session triggers a fresh create. */
  @Test
  public void testExpiredSessionTriggersFreshCreate() throws Exception {
    when(client.createSession(tracingContext))
        .thenReturn(newSession(Instant.now().minusSeconds(1)),
            newSession(Instant.now().plusSeconds(300)));

    manager.getSessionCredentials(tracingContext);
    manager.getSessionCredentials(tracingContext);

    verify(client, times(2)).createSession(tracingContext);
  }


  /** ABFS exception propagates and arms the fallback window. */
  @Test
  public void testAbfsExceptionPropagatesAndArmsFallback() throws Exception {
    final Exception cause = new RuntimeException("Create Session failed");
    doThrow(new AbfsDriverException("Create Session failed", cause))
        .when(client).createSession(tracingContext);

    assertThatThrownBy(() -> manager.getSessionCredentials(tracingContext))
        .isInstanceOf(AzureBlobFileSystemException.class);

    assertThat(manager.getSessionCredentials(tracingContext)).isNull();
    verify(client, times(1)).createSession(tracingContext);
  }

  /** Runtime failure degrades to null and arms the fallback window. */
  @Test
  public void testRuntimeExceptionDegradesToNullAndArmsFallback()
      throws Exception {
    doThrow(new IllegalStateException("unexpected"))
        .when(client).createSession(tracingContext);

    assertThat(manager.getSessionCredentials(tracingContext)).isNull();
    assertThat(manager.getSessionCredentials(tracingContext)).isNull();
    verify(client, times(1)).createSession(tracingContext);
  }

  /** Fallback window suppresses further Create Session attempts. */
  @Test
  public void testFallbackWindowSuppressesRetries() throws Exception {
    final Exception cause = new RuntimeException("fail");
    doThrow(new AbfsDriverException("fail", cause))
        .when(client).createSession(tracingContext);

    try {
      manager.getSessionCredentials(tracingContext);
    } catch (AzureBlobFileSystemException ignored) {
      // Expected on the first call.
    }
    for (int i = 0; i < 5; i++) {
      assertThat(manager.getSessionCredentials(tracingContext)).isNull();
    }

    verify(client, times(1)).createSession(tracingContext);
  }

  /**
   * Single-flight invariant: N concurrent cold-cache callers must
   * produce exactly one Create Session and share its result.
   */
  @Test
  public void testConcurrentRequestsIssueSingleCreateSession() throws Exception {
    final int threadCount = 32;
    final SessionCredentials response =
        newSession(Instant.now().plusSeconds(300));
    final AtomicInteger createCount = new AtomicInteger(0);
    final CountDownLatch inFlight = new CountDownLatch(1);
    final CountDownLatch startGate = new CountDownLatch(1);

    doAnswer(inv -> {
      createCount.incrementAndGet();
      inFlight.await(2, TimeUnit.SECONDS);
      return response;
    }).when(client).createSession(any());

    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch done = new CountDownLatch(threadCount);
    AtomicReference<SessionKeyCredentials> observed = new AtomicReference<>();
    AtomicInteger nonNullResults = new AtomicInteger(0);

    for (int i = 0; i < threadCount; i++) {
      pool.submit(() -> {
        try {
          startGate.await();
          SessionKeyCredentials c =
              manager.getSessionCredentials(tracingContext);
          if (c != null) {
            observed.compareAndSet(null, c);
            nonNullResults.incrementAndGet();
          }
        } catch (Exception ignored) {
          // Failures counted only via createCount; asserted below.
        } finally {
          done.countDown();
        }
      });
    }

    startGate.countDown();
    Thread.sleep(50);
    inFlight.countDown();

    assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
    pool.shutdown();

    assertThat(createCount.get()).isEqualTo(1);
    assertThat(nonNullResults.get()).isEqualTo(threadCount);
    verify(client, times(1)).createSession(any());
  }

  /** Concurrent invalidation followed by a get must trigger a re-create. */
  @Test
  public void testConcurrentInvalidationEventuallyForcesFreshCreate()
      throws Exception {
    when(client.createSession(any()))
        .thenReturn(newSession(Instant.now().plusSeconds(300)),
            newSession(Instant.now().plusSeconds(300)));

    manager.getSessionCredentials(tracingContext);

    ExecutorService pool = Executors.newFixedThreadPool(4);
    CountDownLatch done = new CountDownLatch(4);
    for (int i = 0; i < 4; i++) {
      pool.submit(() -> {
        manager.invalidateCurrentSession();
        done.countDown();
      });
    }
    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    pool.shutdown();

    manager.getSessionCredentials(tracingContext);

    verify(client, times(2)).createSession(any());
  }

  // =========================================================================
  // Time-sensitive behavior (via injected Clock)
  // =========================================================================

  /**
   * Inside the refresh-skew window, cached credentials are returned to the
   * caller immediately and a background refresh is triggered.
   */
  @Test
  public void testWithinRefreshSkewReturnsCachedAndTriggersBackgroundRefresh()
      throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant expiry = t0.plusSeconds(300);
    final Instant insideSkew = expiry.minusSeconds(30);

    final CountDownLatch refreshDone = new CountDownLatch(1);
    final AtomicInteger createCount = new AtomicInteger();
    doAnswer(inv -> {
      if (createCount.incrementAndGet() >= 2) {
        refreshDone.countDown();
      }
      return newSession(expiry);
    }).when(client).createSession(any());

    MutableClock testClock = new MutableClock(t0);
    AbfsSessionManager mgr =
        new AbfsSessionManager(client, configuration, testClock);

    SessionKeyCredentials first = mgr.getSessionCredentials(tracingContext);
    assertThat(first).isNotNull();
    assertThat(createCount.get()).isEqualTo(1);

    testClock.setTo(insideSkew);

    SessionKeyCredentials second = mgr.getSessionCredentials(tracingContext);
    assertThat(second).isSameAs(first);

    assertThat(refreshDone.await(2, TimeUnit.SECONDS)).isTrue();
    assertThat(createCount.get()).isEqualTo(2);
  }

  /**
   * After the OAuth-fallback window elapses, the next call retries
   * Create Session instead of returning null indefinitely.
   */
  @Test
  public void testFallbackWindowExpiryAllowsRetry() throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant afterFallback =
        t0.plusSeconds(FALLBACK_DURATION_SECONDS + 1);

    final Exception cause = new RuntimeException("simulated failure");
    when(client.createSession(any()))
        .thenThrow(new AbfsDriverException("boom", cause))
        .thenReturn(newSession(afterFallback.plusSeconds(300)));

    MutableClock testClock = new MutableClock(t0);
    AbfsSessionManager mgr =
        new AbfsSessionManager(client, configuration, testClock);

    assertThatThrownBy(() -> mgr.getSessionCredentials(tracingContext))
        .isInstanceOf(AzureBlobFileSystemException.class);

    assertThat(mgr.getSessionCredentials(tracingContext)).isNull();
    verify(client, times(1)).createSession(any());

    testClock.setTo(afterFallback);

    SessionKeyCredentials creds = mgr.getSessionCredentials(tracingContext);
    assertThat(creds).isNotNull();
    verify(client, times(2)).createSession(any());
  }

  /**
   * Single-flight invariant for the refresh path: even when many callers
   * notice the refresh-skew window at the same simulated instant, at most
   * one Create Session call executes concurrently.
   */
  @Test
  public void testConcurrentRefreshIsSingleFlight() throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant expiry = t0.plusSeconds(300);
    final Instant insideSkew = expiry.minusSeconds(30);

    final AtomicInteger concurrentCreates = new AtomicInteger();
    final AtomicInteger maxConcurrent = new AtomicInteger();
    final AtomicInteger totalCreates = new AtomicInteger();

    doAnswer(inv -> {
      int inFlight = concurrentCreates.incrementAndGet();
      maxConcurrent.accumulateAndGet(inFlight, Math::max);
      totalCreates.incrementAndGet();
      try {
        Thread.sleep(30);
        return newSession(expiry);
      } finally {
        concurrentCreates.decrementAndGet();
      }
    }).when(client).createSession(any());

    MutableClock testClock = new MutableClock(t0);
    AbfsSessionManager mgr =
        new AbfsSessionManager(client, configuration, testClock);

    mgr.getSessionCredentials(tracingContext);

    testClock.setTo(insideSkew);

    final int callerThreads = 16;
    ExecutorService pool = Executors.newFixedThreadPool(callerThreads);
    CountDownLatch startGate = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(callerThreads);
    for (int i = 0; i < callerThreads; i++) {
      pool.submit(() -> {
        try {
          startGate.await();
          assertThat(mgr.getSessionCredentials(tracingContext)).isNotNull();
        } catch (Exception ignored) {
          // Failure surfaces via the assertions below.
        } finally {
          done.countDown();
        }
      });
    }
    startGate.countDown();

    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    pool.shutdown();

    long deadline = System.currentTimeMillis() + 3000;
    while (totalCreates.get() < 2 && System.currentTimeMillis() < deadline) {
      Thread.sleep(20);
    }

    assertThat(totalCreates.get()).isGreaterThanOrEqualTo(2);
    assertThat(maxConcurrent.get()).isEqualTo(1);
  }

  /** GET on a plain blob URL is eligible for session auth. */
  @Test
  public void testSupportsSessionAcceptsGetBlob() throws Exception {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    when(op.getMethod()).thenReturn("GET");
    when(op.getUrl()).thenReturn(
        new URL("https://acct.blob.core.windows.net/mycontainer/myblob"));

    assertThat(AbfsSessionManager.supportsSession(op)).isTrue();
  }

  /** HEAD blob (GetBlobProperties) is eligible for session auth. */
  @Test
  public void testSupportsSessionAcceptsHeadBlob() throws Exception {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    when(op.getMethod()).thenReturn("HEAD");
    when(op.getUrl()).thenReturn(
        new URL("https://acct.blob.core.windows.net/mycontainer/myblob"));

    assertThat(AbfsSessionManager.supportsSession(op)).isTrue();
  }

  /** PUT blob (create/write) is rejected. */
  @Test
  public void testSupportsSessionRejectsPut() throws Exception {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    when(op.getMethod()).thenReturn("PUT");
    when(op.getUrl()).thenReturn(
        new URL("https://acct.blob.core.windows.net/mycontainer/myblob"));

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** DELETE blob is rejected. */
  @Test
  public void testSupportsSessionRejectsDelete() throws Exception {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    when(op.getMethod()).thenReturn("DELETE");
    when(op.getUrl()).thenReturn(
        new URL("https://acct.blob.core.windows.net/mycontainer/myblob"));

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** POST (Create Session's request shape) is rejected. */
  @Test
  public void testSupportsSessionRejectsPost() throws Exception {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    when(op.getMethod()).thenReturn("POST");
    when(op.getUrl()).thenReturn(new URL(
        "https://acct.blob.core.windows.net/mycontainer"
            + "?restype=container&comp=session"));

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** GET blob ?comp=metadata is rejected. */
  @Test
  public void testSupportsSessionRejectsCompQueryParam() throws Exception {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    when(op.getMethod()).thenReturn("GET");
    when(op.getUrl()).thenReturn(new URL(
        "https://acct.blob.core.windows.net/mycontainer/myblob?comp=metadata"));

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** HEAD blob ?comp=metadata is rejected (comp= trumps HEAD eligibility). */
  @Test
  public void testSupportsSessionRejectsHeadWithComp() throws Exception {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    when(op.getMethod()).thenReturn("HEAD");
    when(op.getUrl()).thenReturn(new URL(
        "https://acct.blob.core.windows.net/mycontainer/myblob?comp=metadata"));

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** GET container ?comp=list is rejected. */
  @Test
  public void testSupportsSessionRejectsListContainer() throws Exception {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    when(op.getMethod()).thenReturn("GET");
    when(op.getUrl()).thenReturn(new URL(
        "https://acct.blob.core.windows.net/mycontainer"
            + "?restype=container&comp=list"));

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** Container-only path is rejected. */
  @Test
  public void testSupportsSessionRejectsContainerOnlyPath() throws Exception {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    when(op.getMethod()).thenReturn("GET");
    when(op.getUrl()).thenReturn(
        new URL("https://acct.blob.core.windows.net/mycontainer"));

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** HEAD on container-only path is rejected. */
  @Test
  public void testSupportsSessionRejectsHeadContainer() throws Exception {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    when(op.getMethod()).thenReturn("HEAD");
    when(op.getUrl()).thenReturn(
        new URL("https://acct.blob.core.windows.net/mycontainer"));

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** Root path is rejected. */
  @Test
  public void testSupportsSessionRejectsRoot() throws Exception {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    when(op.getMethod()).thenReturn("GET");
    when(op.getUrl()).thenReturn(
        new URL("https://acct.blob.core.windows.net/"));

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** Null op is rejected without throwing. */
  @Test
  public void testSupportsSessionRejectsNullOp() {
    assertThat(AbfsSessionManager.supportsSession(null)).isFalse();
  }

  /** comp= detector must not match "composed=" or other substrings. */
  @Test
  public void testSupportsSessionCompDetectionAvoidsFalsePositives()
      throws Exception {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    when(op.getMethod()).thenReturn("GET");
    when(op.getUrl()).thenReturn(new URL(
        "https://acct.blob.core.windows.net/mycontainer/myblob?composed=xyz"));

    assertThat(AbfsSessionManager.supportsSession(op)).isTrue();
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  private static SessionCredentials newSession(Instant expiration) {
    return new SessionCredentials(SESSION_ID, SESSION_TOKEN,
        SESSION_KEY, AUTH_TYPE, expiration);
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