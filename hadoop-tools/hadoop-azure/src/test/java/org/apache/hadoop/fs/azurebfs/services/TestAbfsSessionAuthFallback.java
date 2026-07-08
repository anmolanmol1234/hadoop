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

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests that mock every server-side and network failure the
 * {@code Create Session} API might realistically emit and verify that
 * {@link AbfsSessionManager} degrades gracefully.
 *
 * <p>Covers 5xx service failures, 4xx client-visible failures, non-HTTP
 * network and parsing failures, and unexpected runtime exceptions. Also
 * verifies that once armed, the OAuth-fallback window suppresses further
 * attempts against the service, that explicit invalidation during the
 * window does not force a retry, and that the manager exits fallback
 * once the window elapses so a transient service issue does not
 * permanently disable session authentication.
 **/
@Timeout(value = 20)
public class TestAbfsSessionAuthFallback {

  private static final int REFRESH_THRESHOLD_SECONDS = 60;
  private static final int FALLBACK_DURATION_SECONDS = 300;
  private static final String ACCOUNT_NAME = "myaccount";

  @Mock private AbfsClient client;
  @Mock private AbfsConfiguration configuration;
  @Mock private TracingContext tracingContext;

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

  /**
   * Verify a 503 {@code ServerBusy} response propagates on the first
   * call and arms the OAuth-fallback window so we do not retry against
   * a throttled service.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void test503ServerBusyArmsFallback() throws Exception {
    doThrow(newAbfsRestOperationException(503, "ServerBusy",
        "The server is busy."))
        .when(client).createSession(any());

    assertFirstCallThrowsThenFallsBack();
  }

  /**
   * Verify a 503 {@code SessionOperationsTemporarilyUnavailable}
   * response (the service-specific error code documented in the
   * Create Session contract) arms the fallback window and behaves
   * identically to any other 5xx.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void test503SessionOperationsUnavailableArmsFallback()
      throws Exception {
    doThrow(newAbfsRestOperationException(503,
        "SessionOperationsTemporarilyUnavailable",
        "Session operations are temporarily unavailable."))
        .when(client).createSession(any());

    assertFirstCallThrowsThenFallsBack();
  }

  /**
   * Verify a 500 {@code InternalServerError} response arms the
   * fallback window like any other 5xx failure.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void test500InternalServerErrorArmsFallback() throws Exception {
    doThrow(newAbfsRestOperationException(500, "InternalServerError",
        "The server encountered an internal error."))
        .when(client).createSession(any());

    assertFirstCallThrowsThenFallsBack();
  }

  /**
   * Verify a 504 {@code GatewayTimeout} response arms the fallback
   * window like any other 5xx failure.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void test504GatewayTimeoutArmsFallback() throws Exception {
    doThrow(newAbfsRestOperationException(504, "GatewayTimeout",
        "The gateway timed out."))
        .when(client).createSession(any());

    assertFirstCallThrowsThenFallsBack();
  }

  /**
   * Verify a 403 {@code FeatureNotEnabled} response arms the fallback
   * window. This is the response returned when the storage account has
   * not been allow-listed for the Create Session preview; retrying will
   * not help so the driver must not hammer the service.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void test403FeatureNotEnabledArmsFallback() throws Exception {
    doThrow(newAbfsRestOperationException(403, "FeatureNotEnabled",
        "The feature is not enabled for this account."))
        .when(client).createSession(any());

    assertFirstCallThrowsThenFallsBack();
  }

  /**
   * Verify a 403 {@code AuthenticationFailed} response arms the
   * fallback window. Indicates the credential presented to Create
   * Session was rejected server-side; the driver must not loop
   * retrying with a broken token.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void test403AuthenticationFailedArmsFallback() throws Exception {
    doThrow(newAbfsRestOperationException(403, "AuthenticationFailed",
        "Server failed to authenticate the request."))
        .when(client).createSession(any());

    assertFirstCallThrowsThenFallsBack();
  }

  /**
   * Verify a 401 Unauthorized on Create Session itself arms the
   * fallback window. Represents a bad or expired OAuth token; the
   * driver must degrade cleanly to the underlying credential.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void test401UnauthorizedArmsFallback() throws Exception {
    doThrow(newAbfsRestOperationException(401, "AuthenticationFailed",
        "The credential is not authorized."))
        .when(client).createSession(any());

    assertFirstCallThrowsThenFallsBack();
  }

  /**
   * Verify a 404 {@code ContainerNotFound} response arms the fallback
   * window. The user has pointed the driver at a container that does
   * not exist; retrying will never succeed, so the driver must not
   * spin against a nonexistent resource.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void test404ContainerNotFoundArmsFallback() throws Exception {
    doThrow(newAbfsRestOperationException(404, "ContainerNotFound",
        "The specified container does not exist."))
        .when(client).createSession(any());

    assertFirstCallThrowsThenFallsBack();
  }

  /**
   * Verify a 400 {@code InvalidQueryParameterValue} response arms the
   * fallback window. Represents a service-contract mismatch (for
   * example, an {@code x-ms-version} value that predates Create
   * Session); the driver must not loop retrying an unsupported request.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void test400InvalidQueryParameterArmsFallback() throws Exception {
    doThrow(newAbfsRestOperationException(400, "InvalidQueryParameterValue",
        "Value for one of the query parameters is invalid."))
        .when(client).createSession(any());

    assertFirstCallThrowsThenFallsBack();
  }

  /**
   * Verify a network I/O failure during Create Session wrapped by
   * the driver as {@link AbfsDriverException} propagates on the
   * first call and arms the fallback window.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testNetworkIoFailureArmsFallback() throws Exception {
    doThrow(new AbfsDriverException("network unreachable",
        new IOException("connect timed out")))
        .when(client).createSession(any());

    assertFirstCallThrowsThenFallsBack();
  }

  /**
   * Verify a malformed XML response body surfaced by the driver's
   * XML parser as {@link AbfsDriverException} propagates on the
   * first call and arms the fallback window.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testMalformedResponseBodyArmsFallback() throws Exception {
    doThrow(new AbfsDriverException("XML parse failure",
        new IOException("unexpected end of document")))
        .when(client).createSession(any());

    assertFirstCallThrowsThenFallsBack();
  }

  /**
   * Verify an unexpected {@link IllegalStateException} escaping the
   * client degrades to {@code null} for the caller and still arms the
   * fallback window. The design intentionally keeps user requests
   * alive by falling back to the underlying credential rather than
   * propagating runtime failures.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testIllegalStateExceptionDegradesToNullAndArmsFallback()
      throws Exception {
    doThrow(new IllegalStateException("unexpected"))
        .when(client).createSession(any());

    assertThat(manager.getSessionCredentials(tracingContext)).isNull();
    assertThat(manager.getSessionCredentials(tracingContext)).isNull();
    verify(client, times(1)).createSession(any());
  }

  /**
   * Verify a {@link NullPointerException} escaping the client  for
   * example, from a bad response parser is treated identically to
   * any other runtime failure: degrade to {@code null}, arm the
   * fallback window.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testNullPointerExceptionDegradesToNullAndArmsFallback()
      throws Exception {
    doThrow(new NullPointerException("bad response"))
        .when(client).createSession(any());

    assertThat(manager.getSessionCredentials(tracingContext)).isNull();
    assertThat(manager.getSessionCredentials(tracingContext)).isNull();
    verify(client, times(1)).createSession(any());
  }

  // =========================================================================
  // Fallback-window persistence and post-failure invalidation
  // =========================================================================

  /**
   * Verify that after the fallback window is armed, further calls do
   * not re-hit the service even under many attempts. Guards against
   * request storms when the account is misconfigured or the service
   * is unavailable.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testFallbackWindowSuppressesRepeatedAttempts() throws Exception {
    doThrow(newAbfsRestOperationException(503, "ServerBusy", "busy"))
        .when(client).createSession(any());

    try {
      manager.getSessionCredentials(tracingContext);
    } catch (AzureBlobFileSystemException ignored) {
      // Expected propagation on the first call.
    }

    for (int i = 0; i < 20; i++) {
      assertThat(manager.getSessionCredentials(tracingContext)).isNull();
    }

    verify(client, times(1)).createSession(any());
  }

  /**
   * Verify that explicit invalidation during the fallback window does
   * not by itself trigger a fresh Create Session attempt. The manager
   * stays in fallback until the window elapses (covered by
   * {@link #testFallbackExpiryPermitsRetryAfterFailure}).
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testInvalidationDuringFallbackDoesNotRetry() throws Exception {
    doThrow(newAbfsRestOperationException(503, "ServerBusy", "busy"))
        .when(client).createSession(any());

    try {
      manager.getSessionCredentials(tracingContext);
    } catch (AzureBlobFileSystemException ignored) {
      // Expected propagation on the first call.
    }
    manager.invalidateCurrentSession();
    manager.invalidateCurrentSession();

    assertThat(manager.getSessionCredentials(tracingContext)).isNull();
    verify(client, times(1)).createSession(any());
  }

  /**
   * Verify a fresh manager against a healthy client succeeds normally
   * after a prior failing manager has been created. Simulates the
   * post-fallback-window state without waiting for real time to elapse
   * and guards against any hidden global state that might taint a
   * subsequent successful creation.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testRecoveryAfterFailureIsClean() throws Exception {
    doThrow(newAbfsRestOperationException(503, "ServerBusy", "busy"))
        .when(client).createSession(any());
    try {
      manager.getSessionCredentials(tracingContext);
    } catch (AzureBlobFileSystemException ignored) {
      // Expected propagation on the first call.
    }

    AbfsClient healthyClient = Mockito.mock(AbfsClient.class);
    when(healthyClient.getAccountName()).thenReturn(ACCOUNT_NAME);
    when(healthyClient.createSession(any()))
        .thenReturn(new SessionCredentials("id", "token",
            "key".getBytes(), "HMAC",
            Instant.now().plusSeconds(300)));

    AbfsSessionManager freshManager =
        new AbfsSessionManager(healthyClient, configuration);
    SessionKeyCredentials creds =
        freshManager.getSessionCredentials(tracingContext);

    assertThat(creds).isNotNull();
    assertThat(creds.getSessionToken()).isEqualTo("token");
  }

  /**
   * Verify that after the OAuth-fallback window elapses, the next call
   * retries Create Session and succeeds. Locks in the invariant that
   * fallback is bounded and self-healing: a transient service issue
   * arms the window, subsequent calls short-circuit, but once the
   * cool-off passes the driver resumes attempting session auth.
   *
   * <p>Uses an injected {@link Clock} to simulate time progression
   * without a real sleep so the test remains fast and deterministic.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testFallbackExpiryPermitsRetryAfterFailure() throws Exception {
    final Instant t0 = Instant.parse("2026-07-02T10:00:00Z");
    final Instant afterFallback =
        t0.plusSeconds(FALLBACK_DURATION_SECONDS + 1);

    // First call throws 503; subsequent calls succeed.
    when(client.createSession(any()))
        .thenThrow(newAbfsRestOperationException(503, "ServerBusy",
            "The server is busy."))
        .thenReturn(new SessionCredentials("id", "token",
            "key".getBytes(), "HMAC",
            afterFallback.plusSeconds(300)));

    MutableClock testClock = new MutableClock(t0);
    AbfsSessionManager mgr =
        new AbfsSessionManager(client, configuration, testClock);

    // First call: failure propagates and arms the fallback window at t0.
    assertThatThrownBy(() -> mgr.getSessionCredentials(tracingContext))
        .isInstanceOf(AzureBlobFileSystemException.class);

    // Clock still inside the fallback window no retry.
    assertThat(mgr.getSessionCredentials(tracingContext)).isNull();
    verify(client, times(1)).createSession(any());

    // Advance the clock past the fallback window.
    testClock.setTo(afterFallback);

    // The retry now fires and succeeds.
    SessionKeyCredentials creds =
        mgr.getSessionCredentials(tracingContext);
    assertThat(creds).isNotNull();
    assertThat(creds.getSessionToken()).isEqualTo("token");
    verify(client, times(2)).createSession(any());
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  /**
   * Assert the standard fallback contract: the first call to
   * {@link AbfsSessionManager#getSessionCredentials(TracingContext)}
   * propagates {@link AzureBlobFileSystemException} to the caller,
   * subsequent calls return {@code null} while the fallback window is
   * active, and the underlying client is invoked exactly once.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  private void assertFirstCallThrowsThenFallsBack() throws Exception {
    assertThatThrownBy(() -> manager.getSessionCredentials(tracingContext))
        .isInstanceOf(AzureBlobFileSystemException.class);

    assertThat(manager.getSessionCredentials(tracingContext)).isNull();
    assertThat(manager.getSessionCredentials(tracingContext)).isNull();

    verify(client, times(1)).createSession(any());
  }

  /**
   * Build an {@link AbfsRestOperationException} carrying the specified
   * HTTP status, storage error code, and message. Represents the shape
   * the driver's REST-operation layer surfaces from a failed Create
   * Session call.
   *
   * @param statusCode HTTP status code.
   * @param errorCode Azure Storage error code string.
   * @param message error message body.
   * @return a constructed rest-operation exception.
   */
  private static AbfsRestOperationException newAbfsRestOperationException(
      final int statusCode, final String errorCode, final String message) {
    return new AbfsRestOperationException(statusCode, errorCode, message,
        null /* innerException */);
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