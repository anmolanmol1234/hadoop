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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the Create Session parsing helpers on
 * {@link AbfsBlobClient}: {@code parseExpiration} and
 * {@code parseCreateSessionResponse}.
 *
 * <p>Uses JUnit 5 with manual Mockito initialization
 * ({@link MockitoAnnotations#openMocks(Object)}) to avoid a dependency
 * on {@code mockito-junit-jupiter}.
 *
 * <p>The parser reads the response body from
 * {@link AbfsHttpOperation#getSessionResultStream()} (not the raw HTTP
 * stream), so the tests stub that accessor. This mirrors the pattern
 * used by list responses and reflects the buffered-body handling added
 * to {@code AbfsHttpOperation.parseResponse}.
 */
@Timeout(value = 15, unit = TimeUnit.SECONDS)
public class TestAbfsBlobClientCreateSession {

  @Mock private AbfsRestOperation restOp;
  @Mock private AbfsHttpOperation httpResult;

  private AbfsBlobClient client;
  private AutoCloseable mocks;

  @BeforeEach
  public void setUp() {
    mocks = MockitoAnnotations.openMocks(this);
    client = mock(AbfsBlobClient.class, CALLS_REAL_METHODS);
    when(restOp.getResult()).thenReturn(httpResult);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (mocks != null) {
      mocks.close();
    }
  }

  // =========================================================================
  // parseExpiration
  // =========================================================================

  /** RFC 1123 timestamp is parsed as the primary format. */
  @Test
  public void testParseExpirationRfc1123() throws Exception {
    Instant parsed =
        AbfsBlobClient.parseExpiration("Thu, 02 Jul 2026 10:15:30 GMT");

    assertThat(parsed).isEqualTo(Instant.parse("2026-07-02T10:15:30Z"));
  }

  /** ISO-8601 timestamp is parsed via fallback. */
  @Test
  public void testParseExpirationIso8601Fallback() throws Exception {
    Instant parsed =
        AbfsBlobClient.parseExpiration("2026-07-02T10:15:30Z");

    assertThat(parsed).isEqualTo(Instant.parse("2026-07-02T10:15:30Z"));
  }

  /** Unrecognized format surfaces an IOException carrying both failures. */
  @Test
  public void testParseExpirationGarbageThrows() {
    assertThatThrownBy(
        () -> AbfsBlobClient.parseExpiration("not-a-timestamp"))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Unrecognized Expiration format")
        .satisfies(t -> {
          assertThat(t.getCause())
              .as("primary parse failure must be the cause")
              .isNotNull();
          assertThat(t.getSuppressed())
              .as("fallback parse failure must be suppressed")
              .hasSize(1);
        });
  }

  /** Empty string is treated as garbage. */
  @Test
  public void testParseExpirationEmptyThrows() {
    assertThatThrownBy(() -> AbfsBlobClient.parseExpiration(""))
        .isInstanceOf(IOException.class);
  }

  // =========================================================================
  // parseCreateSessionResponse — happy path
  // =========================================================================

  /** Valid response yields SessionCredentials with all fields populated. */
  @Test
  public void testParseResponseValidXmlReturnsCredentials() throws Exception {
    stubResponseBody(validCreateSessionResponse());

    SessionCredentials creds = client.parseCreateSessionResponse(restOp);

    assertThat(creds.getSessionId()).isEqualTo("session-1234");
    assertThat(creds.getSessionToken()).isEqualTo("opaque-token");
    assertThat(creds.getAuthenticationType()).isEqualTo("HMAC");
    assertThat(creds.getExpirationTime())
        .isEqualTo(Instant.parse("2026-07-02T10:15:30Z"));
    assertThat(creds.getSessionKey())
        .isEqualTo(java.util.Base64.getDecoder().decode("a2V5LWJ5dGVz"));
  }

  /** Whitespace inside element text nodes is trimmed. */
  @Test
  public void testParseResponseTrimsElementWhitespace() throws Exception {
    String xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
        + "<CreateSessionResult>"
        + "  <Id>  session-1234\n</Id>"
        + "  <AuthenticationType>  HMAC\n</AuthenticationType>"
        + "  <Expiration>  Thu, 02 Jul 2026 10:15:30 GMT\n</Expiration>"
        + "  <SessionToken>  opaque-token\n</SessionToken>"
        + "  <SessionKey>  a2V5LWJ5dGVz\n</SessionKey>"
        + "</CreateSessionResult>";
    stubResponseBody(xml);

    SessionCredentials creds = client.parseCreateSessionResponse(restOp);

    assertThat(creds.getSessionId()).isEqualTo("session-1234");
    assertThat(creds.getSessionToken()).isEqualTo("opaque-token");
  }

  /** Null session result stream produces a clean AbfsDriverException. */
  @Test
  public void testParseResponseNullStreamThrows() throws Exception {
    when(httpResult.getSessionResultStream()).thenReturn(null);

    assertThatThrownBy(() -> client.parseCreateSessionResponse(restOp))
        .isInstanceOf(AbfsDriverException.class);
  }

  // =========================================================================
  // parseCreateSessionResponse — error surfaces
  // =========================================================================

  /** Missing SessionToken element throws AbfsDriverException. */
  @Test
  public void testParseResponseMissingSessionTokenThrows() throws Exception {
    stubResponseBody(responseWithoutTag("SessionToken"));

    assertThatThrownBy(() -> client.parseCreateSessionResponse(restOp))
        .isInstanceOf(AbfsDriverException.class);
  }

  /** Missing SessionKey element throws. */
  @Test
  public void testParseResponseMissingSessionKeyThrows() throws Exception {
    stubResponseBody(responseWithoutTag("SessionKey"));

    assertThatThrownBy(() -> client.parseCreateSessionResponse(restOp))
        .isInstanceOf(AbfsDriverException.class);
  }

  /** Missing Expiration element throws. */
  @Test
  public void testParseResponseMissingExpirationThrows() throws Exception {
    stubResponseBody(responseWithoutTag("Expiration"));

    assertThatThrownBy(() -> client.parseCreateSessionResponse(restOp))
        .isInstanceOf(AbfsDriverException.class);
  }

  /** Empty required element throws. */
  @Test
  public void testParseResponseEmptySessionTokenThrows() throws Exception {
    String xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
        + "<CreateSessionResult>"
        + "  <Id>i</Id>"
        + "  <AuthenticationType>HMAC</AuthenticationType>"
        + "  <Expiration>Thu, 02 Jul 2026 10:15:30 GMT</Expiration>"
        + "  <SessionToken></SessionToken>"
        + "  <SessionKey>a2V5</SessionKey>"
        + "</CreateSessionResult>";
    stubResponseBody(xml);

    assertThatThrownBy(() -> client.parseCreateSessionResponse(restOp))
        .isInstanceOf(AbfsDriverException.class);
  }

  /** Whitespace-only required element throws. */
  @Test
  public void testParseResponseWhitespaceOnlyExpirationThrows()
      throws Exception {
    String xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
        + "<CreateSessionResult>"
        + "  <Id>i</Id>"
        + "  <AuthenticationType>HMAC</AuthenticationType>"
        + "  <Expiration>   </Expiration>"
        + "  <SessionToken>t</SessionToken>"
        + "  <SessionKey>a2V5</SessionKey>"
        + "</CreateSessionResult>";
    stubResponseBody(xml);

    assertThatThrownBy(() -> client.parseCreateSessionResponse(restOp))
        .isInstanceOf(AbfsDriverException.class);
  }

  /** Malformed XML throws. */
  @Test
  public void testParseResponseMalformedXmlThrows() throws Exception {
    stubResponseBody("<CreateSessionResult><Id>oops");

    assertThatThrownBy(() -> client.parseCreateSessionResponse(restOp))
        .isInstanceOf(AbfsDriverException.class);
  }

  /** Invalid Base64 in SessionKey throws. */
  @Test
  public void testParseResponseInvalidBase64SessionKeyThrows()
      throws Exception {
    String xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
        + "<CreateSessionResult>"
        + "  <Id>i</Id>"
        + "  <AuthenticationType>HMAC</AuthenticationType>"
        + "  <Expiration>Thu, 02 Jul 2026 10:15:30 GMT</Expiration>"
        + "  <SessionToken>t</SessionToken>"
        + "  <SessionKey>NOT-BASE-64!!!</SessionKey>"
        + "</CreateSessionResult>";
    stubResponseBody(xml);

    assertThatThrownBy(() -> client.parseCreateSessionResponse(restOp))
        .isInstanceOf(AbfsDriverException.class);
  }

  /** Invalid Expiration format throws. */
  @Test
  public void testParseResponseInvalidExpirationThrows() throws Exception {
    String xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
        + "<CreateSessionResult>"
        + "  <Id>i</Id>"
        + "  <AuthenticationType>HMAC</AuthenticationType>"
        + "  <Expiration>not-a-date</Expiration>"
        + "  <SessionToken>t</SessionToken>"
        + "  <SessionKey>a2V5</SessionKey>"
        + "</CreateSessionResult>";
    stubResponseBody(xml);

    assertThatThrownBy(() -> client.parseCreateSessionResponse(restOp))
        .isInstanceOf(AbfsDriverException.class);
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  private void stubResponseBody(String xml) throws IOException {
    // Parser now reads from getSessionResultStream() — the buffered body
    // captured during AbfsHttpOperation.parseResponse. Stub that path.
    when(httpResult.getSessionResultStream()).thenReturn(streamOf(xml));
  }

  private static InputStream streamOf(String s) {
    return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
  }

  private static String validCreateSessionResponse() {
    return "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
        + "<CreateSessionResult>"
        + "  <Id>session-1234</Id>"
        + "  <AuthenticationType>HMAC</AuthenticationType>"
        + "  <Expiration>Thu, 02 Jul 2026 10:15:30 GMT</Expiration>"
        + "  <SessionToken>opaque-token</SessionToken>"
        + "  <SessionKey>a2V5LWJ5dGVz</SessionKey>"
        + "</CreateSessionResult>";
  }

  /** Builds a valid response body with the named element removed. */
  private static String responseWithoutTag(String tagName) {
    String open = "<" + tagName + ">";
    String close = "</" + tagName + ">";
    String full = validCreateSessionResponse();
    int openIdx = full.indexOf(open);
    int closeIdx = full.indexOf(close);
    if (openIdx < 0 || closeIdx < 0) {
      throw new IllegalArgumentException("tag not found: " + tagName);
    }
    return full.substring(0, openIdx)
        + full.substring(closeIdx + close.length());
  }
}
