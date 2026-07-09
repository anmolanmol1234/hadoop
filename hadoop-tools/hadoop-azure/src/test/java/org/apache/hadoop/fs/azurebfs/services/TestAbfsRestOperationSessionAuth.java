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

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests that verify the request-signing switch in
 * {@link AbfsRestOperation} routes correctly between session
 * authentication and the underlying wire credential.
 *
 * <p>Directly asserts the observable outcomes of {@code signRequest} for
 * every combination of client authentication type, feature-gate state,
 * per-operation opt-out, and session-manager response:
 *
 * <ul>
 *   <li>When session auth is available and eligible, requests are
 *       session-signed and the OAuth {@code Authorization} header is
 *       not set.</li>
 *   <li>When session auth is unavailable because the manager returns
 *       {@code null}, the feature is disabled, or the operation is
 *       opted out requests fall back to the OAuth bearer token
 *       without touching the session code path.</li>
 *   <li>When session auth throws, the exception propagates cleanly
 *       through {@code signRequest} without silent swallowing.</li>
 *   <li>SharedKey and SAS authentication types are unaffected by
 *       session code and continue to sign requests as before.</li>
 * </ul>
 *
 */
@Timeout(value = 15)
public class TestAbfsRestOperationSessionAuth {

  private static final String OAUTH_HEADER = "Bearer test-oauth-token";
  private static final int BYTES_TO_SIGN = 42;
  private static final URL BLOB_URL;

  static {
    try {
      BLOB_URL = new URL(
          "https://acct.blob.core.windows.net/mycontainer/myblob");
    } catch (Exception ex) {
      throw new ExceptionInInitializerError(ex);
    }
  }

  @Mock private AbfsClient client;
  @Mock private AbfsConfiguration configuration;
  @Mock private AbfsHttpOperation httpOperation;
  @Mock private TracingContext tracingContext;
  @Mock private AbfsSessionManager sessionManager;
  @Mock private SessionKeyCredentials sessionCreds;
  @Mock private SharedKeyCredentials sharedKeyCreds;

  private AbfsRestOperation op;
  private AutoCloseable mocks;

  @BeforeEach
  public void setUp() throws Exception {
    mocks = MockitoAnnotations.openMocks(this);

    // The AbfsRestOperation constructor requires these stubs. All other
    // client-side collaborators are consulted only from paths outside
    // signRequest, so leaving them null is safe.
    when(configuration.getMaxIoRetries()).thenReturn(0);

    op = new AbfsRestOperation(
        AbfsRestOperationType.GetPathStatus,
        client, "GET", BLOB_URL, new ArrayList<>(), configuration);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (mocks != null) {
      mocks.close();
    }
  }

  /**
   * Verify that a session-eligible OAuth-authenticated request is
   * signed with session credentials when the manager returns a live
   * session. The session signer is invoked exactly once with the
   * caller-supplied byte length, and the OAuth {@code Authorization}
   * header is not set.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSignRequestUsesSessionCredentialsWhenAvailable()
      throws Exception {
    when(client.getAuthType()).thenReturn(AuthType.OAuth);
    when(client.isSessionAuthEnabled()).thenReturn(true);
    when(client.getSessionManager()).thenReturn(sessionManager);
    when(sessionManager.isEligible(op)).thenReturn(true);
    when(sessionManager.getSessionCredentials(tracingContext))
        .thenReturn(sessionCreds);

    op.signRequest(httpOperation, BYTES_TO_SIGN, tracingContext);

    verify(sessionCreds, times(1)).signRequest(httpOperation, BYTES_TO_SIGN);
    verify(client, never()).getAccessToken();
    verify(httpOperation, never()).setRequestProperty(
        eq(HttpHeaderConfigurations.AUTHORIZATION), anyString());
    verify(sharedKeyCreds, never()).signRequest(any(), anyInt());
  }

  /**
   * Verify that a Custom authentication type follows the same session
   * routing as OAuth. The {@code Custom} branch of the auth switch
   * shares its body with {@code OAuth}.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSignRequestUsesSessionForCustomAuthType() throws Exception {
    when(client.getAuthType()).thenReturn(AuthType.Custom);
    when(client.isSessionAuthEnabled()).thenReturn(true);
    when(client.getSessionManager()).thenReturn(sessionManager);
    when(sessionManager.isEligible(op)).thenReturn(true);
    when(sessionManager.getSessionCredentials(tracingContext))
        .thenReturn(sessionCreds);

    op.signRequest(httpOperation, BYTES_TO_SIGN, tracingContext);

    verify(sessionCreds, times(1)).signRequest(httpOperation, BYTES_TO_SIGN);
    verify(httpOperation, never()).setRequestProperty(
        eq(HttpHeaderConfigurations.AUTHORIZATION), anyString());
  }

  /**
   * Verify that when the session manager returns {@code null}, the
   * auth switch falls through to the OAuth bearer token path. The
   * OAuth token is set as the {@code Authorization} header and the
   * session signer is not invoked.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSignRequestFallsBackToOAuthWhenSessionIsNull()
      throws Exception {
    when(client.getAuthType()).thenReturn(AuthType.OAuth);
    when(client.isSessionAuthEnabled()).thenReturn(true);
    when(client.getSessionManager()).thenReturn(sessionManager);
    when(sessionManager.isEligible(op)).thenReturn(true);
    when(sessionManager.getSessionCredentials(tracingContext))
        .thenReturn(null);
    when(client.getAccessToken()).thenReturn(OAUTH_HEADER);

    op.signRequest(httpOperation, BYTES_TO_SIGN, tracingContext);

    verify(httpOperation, times(1)).setRequestProperty(
        HttpHeaderConfigurations.AUTHORIZATION, OAUTH_HEADER);
    verify(sessionCreds, never()).signRequest(any(), anyInt());
  }

  /**
   * Verify that when session auth is disabled globally via
   * configuration, the manager is not consulted and the request goes
   * directly through the OAuth path.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSignRequestUsesOAuthWhenSessionAuthDisabledGlobally()
      throws Exception {
    when(client.getAuthType()).thenReturn(AuthType.OAuth);
    when(client.isSessionAuthEnabled()).thenReturn(false);
    when(client.getAccessToken()).thenReturn(OAUTH_HEADER);

    op.signRequest(httpOperation, BYTES_TO_SIGN, tracingContext);

    verify(httpOperation, times(1)).setRequestProperty(
        HttpHeaderConfigurations.AUTHORIZATION, OAUTH_HEADER);
    verify(client, never()).getSessionManager();
    verify(sessionManager, never()).getSessionCredentials(any());
  }

  /**
   * Verify that when the operation opts out of session auth via
   * {@link AbfsRestOperation#setSessionAuthDisabledForOperation}, the
   * manager is not consulted even though the feature is enabled
   * globally. Used by the Create Session request itself.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSignRequestUsesOAuthWhenSessionAuthDisabledPerOperation()
      throws Exception {
    when(client.getAuthType()).thenReturn(AuthType.OAuth);
    when(client.isSessionAuthEnabled()).thenReturn(true);
    when(client.getAccessToken()).thenReturn(OAUTH_HEADER);

    op.setSessionAuthDisabledForOperation(true);
    op.signRequest(httpOperation, BYTES_TO_SIGN, tracingContext);

    verify(httpOperation, times(1)).setRequestProperty(
        HttpHeaderConfigurations.AUTHORIZATION, OAUTH_HEADER);
    verify(sessionManager, never()).getSessionCredentials(any());
  }

  /**
   * Verify that when the session manager reports the operation
   * ineligible (for example, a non-GET/HEAD request per
   * {@code supportsSession}), the manager is not asked to produce
   * credentials and the request falls through to OAuth.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSignRequestUsesOAuthWhenOperationIneligible()
      throws Exception {
    when(client.getAuthType()).thenReturn(AuthType.OAuth);
    when(client.isSessionAuthEnabled()).thenReturn(true);
    when(client.getSessionManager()).thenReturn(sessionManager);
    when(sessionManager.isEligible(op)).thenReturn(false);
    when(client.getAccessToken()).thenReturn(OAUTH_HEADER);

    op.signRequest(httpOperation, BYTES_TO_SIGN, tracingContext);

    verify(httpOperation, times(1)).setRequestProperty(
        HttpHeaderConfigurations.AUTHORIZATION, OAUTH_HEADER);
    verify(sessionManager, never()).getSessionCredentials(any());
    verify(sessionCreds, never()).signRequest(any(), anyInt());
  }

  /**
   * Verify that when the session manager throws a driver-level
   * exception during {@code getSessionCredentials}, the exception
   * propagates through {@code signRequest} to the caller without being
   * silently swallowed or masked as an OAuth fallback.
   *
   * <p>The manager itself arms the OAuth-fallback window on failure
   * (covered by {@code TestAbfsSessionAuthFallback}); subsequent
   * requests fall back correctly. The very first request whose Create
   * Session attempt failed surfaces the failure to its caller.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSignRequestPropagatesSessionManagerException()
      throws Exception {
    when(client.getAuthType()).thenReturn(AuthType.OAuth);
    when(client.isSessionAuthEnabled()).thenReturn(true);
    when(client.getSessionManager()).thenReturn(sessionManager);
    when(sessionManager.isEligible(op)).thenReturn(true);
    when(sessionManager.getSessionCredentials(tracingContext))
        .thenThrow(new AbfsDriverException(
            "Create Session failed",
            new RuntimeException("boom")));

    assertThatThrownBy(() ->
        op.signRequest(httpOperation, BYTES_TO_SIGN, tracingContext))
        .isInstanceOf(AbfsDriverException.class);

    verify(httpOperation, never()).setRequestProperty(
        eq(HttpHeaderConfigurations.AUTHORIZATION), anyString());
    verify(sessionCreds, never()).signRequest(any(), anyInt());
  }

  /**
   * Verify that SharedKey authentication is unaffected by session
   * code. The request is signed with the shared key credentials and
   * the session manager is not consulted.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSignRequestUsesSharedKeyForSharedKeyAuthType()
      throws Exception {
    when(client.getAuthType()).thenReturn(AuthType.SharedKey);
    when(client.getSharedKeyCredentials()).thenReturn(sharedKeyCreds);

    op.signRequest(httpOperation, BYTES_TO_SIGN, tracingContext);

    verify(sharedKeyCreds, times(1)).signRequest(httpOperation, BYTES_TO_SIGN);
    verify(client, never()).getSessionManager();
    verify(client, never()).getAccessToken();
    verify(sessionCreds, never()).signRequest(any(), anyInt());
  }

  /**
   * Verify that SAS authentication is unaffected by session code. The
   * SAS token travels in the URL query string; the auth switch only
   * masks the URL for logging and does not consult the session
   * manager or set an {@code Authorization} header.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSignRequestForSasAuthTypeMasksUrl() throws Exception {
    when(client.getAuthType()).thenReturn(AuthType.SAS);

    op.signRequest(httpOperation, BYTES_TO_SIGN, tracingContext);

    verify(httpOperation, times(1)).setMaskForSAS();
    verify(httpOperation, never()).setRequestProperty(
        eq(HttpHeaderConfigurations.AUTHORIZATION), anyString());
    verify(client, never()).getSessionManager();
    verify(sessionCreds, never()).signRequest(any(), anyInt());
    verify(sharedKeyCreds, never()).signRequest(any(), anyInt());
  }

  /**
   * Verify that the {@code bytesToSign} parameter is forwarded
   * verbatim to the session signer.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSessionSignReceivesExactBytesToSign() throws Exception {
    when(client.getAuthType()).thenReturn(AuthType.OAuth);
    when(client.isSessionAuthEnabled()).thenReturn(true);
    when(client.getSessionManager()).thenReturn(sessionManager);
    when(sessionManager.isEligible(op)).thenReturn(true);
    when(sessionManager.getSessionCredentials(tracingContext))
        .thenReturn(sessionCreds);

    op.signRequest(httpOperation, 8192, tracingContext);

    verify(sessionCreds, times(1)).signRequest(httpOperation, 8192);
  }

  /**
   * Verify that the {@code bytesToSign} parameter is forwarded
   * verbatim to the SharedKey signer.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSharedKeySignReceivesExactBytesToSign() throws Exception {
    when(client.getAuthType()).thenReturn(AuthType.SharedKey);
    when(client.getSharedKeyCredentials()).thenReturn(sharedKeyCreds);

    op.signRequest(httpOperation, 8192, tracingContext);

    verify(sharedKeyCreds, times(1)).signRequest(httpOperation, 8192);
  }

  /**
   * Verify that {@code shouldUseSessionAuth} handles a null session
   * manager returned from the client without throwing. Falls back to
   * OAuth cleanly.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSignRequestFallsBackToOAuthWhenSessionManagerIsNull()
      throws Exception {
    when(client.getAuthType()).thenReturn(AuthType.OAuth);
    when(client.isSessionAuthEnabled()).thenReturn(true);
    when(client.getSessionManager()).thenReturn(null);
    when(client.getAccessToken()).thenReturn(OAUTH_HEADER);

    op.signRequest(httpOperation, BYTES_TO_SIGN, tracingContext);

    verify(httpOperation, times(1)).setRequestProperty(
        HttpHeaderConfigurations.AUTHORIZATION, OAUTH_HEADER);
  }

  /**
   * Verify that {@code setSessionAuthDisabledForOperation(true)}
   * followed by {@code setSessionAuthDisabledForOperation(false)}
   * restores the default eligibility path — the manager is again
   * consulted for credentials.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSessionAuthDisabledFlagCanBeToggledOff() throws Exception {
    when(client.getAuthType()).thenReturn(AuthType.OAuth);
    when(client.isSessionAuthEnabled()).thenReturn(true);
    when(client.getSessionManager()).thenReturn(sessionManager);
    when(sessionManager.isEligible(op)).thenReturn(true);
    when(sessionManager.getSessionCredentials(tracingContext))
        .thenReturn(sessionCreds);

    op.setSessionAuthDisabledForOperation(true);
    op.setSessionAuthDisabledForOperation(false);
    op.signRequest(httpOperation, BYTES_TO_SIGN, tracingContext);

    // Manager consulted → session credentials used.
    verify(sessionCreds, times(1)).signRequest(httpOperation, BYTES_TO_SIGN);
  }

  /**
   * Verify that {@code setSessionAuthDisabledForOperation(true)} called
   * twice in a row remains equivalent to a single call — the flag is
   * idempotent.
   *
   * @throws Exception on failure of the mocked call chain.
   */
  @Test
  public void testSessionAuthDisabledFlagIsIdempotent() throws Exception {
    when(client.getAuthType()).thenReturn(AuthType.OAuth);
    when(client.isSessionAuthEnabled()).thenReturn(true);
    when(client.getAccessToken()).thenReturn(OAUTH_HEADER);

    op.setSessionAuthDisabledForOperation(true);
    op.setSessionAuthDisabledForOperation(true);
    op.setSessionAuthDisabledForOperation(true);
    op.signRequest(httpOperation, BYTES_TO_SIGN, tracingContext);

    // Manager still not consulted; OAuth path taken.
    verify(sessionManager, never()).getSessionCredentials(any());
    verify(httpOperation, times(1)).setRequestProperty(
        HttpHeaderConfigurations.AUTHORIZATION, OAUTH_HEADER);
  }

  // =========================================================================
// 401 retry hook — invalidation on session-signed request failure
// =========================================================================

  /**
   * Verify that a 401 on a session-signed request triggers session
   * invalidation and signals retry.
   */
  @Test
  public void test401OnSessionSignedRequestTriggersInvalidateAndRetry()
      throws Exception {
    when(client.getAuthType()).thenReturn(AuthType.OAuth);
    when(client.isSessionAuthEnabled()).thenReturn(true);
    when(client.getSessionManager()).thenReturn(sessionManager);
    when(sessionManager.isEligible(op)).thenReturn(true);
    when(sessionManager.getSessionCredentials(tracingContext))
        .thenReturn(sessionCreds);

    op.signRequest(httpOperation, BYTES_TO_SIGN, tracingContext);

    boolean shouldRetry = op.handleSessionInvalidation401(
        HttpURLConnection.HTTP_UNAUTHORIZED);

    Assertions.assertThat(shouldRetry).isTrue();
    verify(sessionManager, times(1)).invalidateCurrentSession();
  }

  /** Verify that a second 401 on the same operation does not retry. */
  @Test
  public void testSecond401DoesNotTriggerSecondInvalidation()
      throws Exception {
    when(client.getAuthType()).thenReturn(AuthType.OAuth);
    when(client.isSessionAuthEnabled()).thenReturn(true);
    when(client.getSessionManager()).thenReturn(sessionManager);
    when(sessionManager.isEligible(op)).thenReturn(true);
    when(sessionManager.getSessionCredentials(tracingContext))
        .thenReturn(sessionCreds);

    op.signRequest(httpOperation, BYTES_TO_SIGN, tracingContext);

    op.handleSessionInvalidation401(
       HttpURLConnection.HTTP_UNAUTHORIZED);
    boolean shouldRetry = op.handleSessionInvalidation401(
        HttpURLConnection.HTTP_UNAUTHORIZED);

    Assertions.assertThat(shouldRetry).isFalse();
    verify(sessionManager, times(1)).invalidateCurrentSession();
  }

  /** Verify that 401 on a non-session-signed request does not invalidate. */
  @Test
  public void test401OnNonSessionSignedRequestDoesNotInvalidate()
      throws Exception {
    boolean shouldRetry = op.handleSessionInvalidation401(
        java.net.HttpURLConnection.HTTP_UNAUTHORIZED);

    Assertions.assertThat(shouldRetry).isFalse();
    verify(sessionManager, never()).invalidateCurrentSession();
  }

  /** Verify that non-401 statuses do not trigger the hook. */
  @Test
  public void testNon401StatusDoesNotTriggerHook() throws Exception {
    when(client.getAuthType()).thenReturn(AuthType.OAuth);
    when(client.isSessionAuthEnabled()).thenReturn(true);
    when(client.getSessionManager()).thenReturn(sessionManager);
    when(sessionManager.isEligible(op)).thenReturn(true);
    when(sessionManager.getSessionCredentials(tracingContext))
        .thenReturn(sessionCreds);

    op.signRequest(httpOperation, BYTES_TO_SIGN, tracingContext);

    Assertions.assertThat(op.handleSessionInvalidation401(200)).isFalse();
    Assertions.assertThat(op.handleSessionInvalidation401(403)).isFalse();
    Assertions.assertThat(op.handleSessionInvalidation401(500)).isFalse();

    verify(sessionManager, never()).invalidateCurrentSession();
  }
}
