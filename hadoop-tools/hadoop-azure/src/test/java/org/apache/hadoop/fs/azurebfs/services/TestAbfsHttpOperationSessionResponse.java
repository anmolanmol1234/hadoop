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
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AbfsHttpOperation#parseResponse} routing of
 * Create Session responses into the buffered {@code sessionResultStream}.
 *
 * <p>Verifies that Create Session responses ({@code POST ?comp=session})
 * are captured into an in-memory buffer during response processing so
 * that {@link AbfsBlobClient#parseCreateSessionResponse} can read the
 * full XML body after the underlying HTTP stream has been drained and
 * closed.
 *
 * <p>Also verifies that non-Create-Session responses do not touch the
 * session buffer, so the routing is scoped narrowly enough to not
 * interfere with other operations.
 */
@Timeout(value = 15, unit = TimeUnit.SECONDS)
public class TestAbfsHttpOperationSessionResponse {

  private static final String CREATE_SESSION_XML =
      "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
          + "<CreateSessionResult>"
          + "<Id>session-id</Id>"
          + "<AuthenticationType>HMAC</AuthenticationType>"
          + "<Expiration>Thu, 02 Jul 2026 10:15:30 GMT</Expiration>"
          + "<SessionToken>opaque-token</SessionToken>"
          + "<SessionKey>a2V5LWJ5dGVz</SessionKey>"
          + "</CreateSessionResult>";

  /**
   * Verify that a Create Session response body is captured into
   * {@code sessionResultStream} so that the caller-side parser can
   * read it after {@code processResponse} completes.
   *
   * @throws Exception on failure of the mocked I/O flow.
   */
  @Test
  public void testSessionResponseBodyBufferedIntoSessionResultStream()
      throws Exception {
    URL url = new URL(
        "https://acct.blob.core.windows.net/mycontainer"
            + "?restype=container&comp=session");
    FakeAbfsHttpOperation op = new FakeAbfsHttpOperation(
        url, "POST", CREATE_SESSION_XML.getBytes(StandardCharsets.UTF_8));

    op.processResponse(null, 0, 0);

    InputStream buffered = op.getSessionResultStream();
    assertThat(buffered)
        .as("Create Session response must be buffered")
        .isNotNull();

    byte[] read = readAll(buffered);
    assertThat(new String(read, StandardCharsets.UTF_8))
        .isEqualTo(CREATE_SESSION_XML);
  }

  /**
   * Verify that a Create Session response captured into the buffer
   * can be read after the underlying wire stream has been drained.
   * Guards against regressions where the parser accidentally reads
   * from the raw HTTP stream.
   *
   * @throws Exception on failure of the mocked I/O flow.
   */
  @Test
  public void testSessionResultStreamSurvivesRawStreamDrain()
      throws Exception {
    URL url = new URL(
        "https://acct.blob.core.windows.net/mycontainer"
            + "?restype=container&comp=session");
    FakeAbfsHttpOperation op = new FakeAbfsHttpOperation(
        url, "POST", CREATE_SESSION_XML.getBytes(StandardCharsets.UTF_8));

    op.processResponse(null, 0, 0);

    // The raw wire stream should be exhausted after processResponse.
    assertThat(op.rawStreamIsExhausted()).isTrue();

    // But the buffered session result stream must still hold the body.
    InputStream buffered = op.getSessionResultStream();
    assertThat(buffered).isNotNull();
    assertThat(readAll(buffered)).isNotEmpty();
  }

  /**
   * Verify that a non-session response — for example, a plain GET blob
   * — does not populate {@code sessionResultStream}. Guards against
   * accidental over-capture that would waste memory on every response.
   *
   * @throws Exception on failure of the mocked I/O flow.
   */
  @Test
  public void testNonSessionGetResponseDoesNotPopulateSessionStream()
      throws Exception {
    URL url = new URL(
        "https://acct.blob.core.windows.net/mycontainer/myblob");
    byte[] readBuffer = new byte[128];
    FakeAbfsHttpOperation op = new FakeAbfsHttpOperation(
        url, "GET", "blob data".getBytes(StandardCharsets.UTF_8));

    op.processResponse(readBuffer, 0, readBuffer.length);

    assertThat(op.getSessionResultStream())
        .as("Session result stream must be null for non-session responses")
        .isNull();
  }

  /**
   * Verify that a POST to a URL without {@code comp=session} does not
   * populate {@code sessionResultStream}. The routing must match on
   * both method and query parameter.
   *
   * @throws Exception on failure of the mocked I/O flow.
   */
  @Test
  public void testPostWithoutSessionCompDoesNotPopulateSessionStream()
      throws Exception {
    URL url = new URL(
        "https://acct.blob.core.windows.net/mycontainer/myblob"
            + "?comp=block&blockid=xyz");
    FakeAbfsHttpOperation op = new FakeAbfsHttpOperation(
        url, "POST", "response".getBytes(StandardCharsets.UTF_8));

    op.processResponse(null, 0, 0);

    assertThat(op.getSessionResultStream())
        .as("Session result stream must be null for non-session POSTs")
        .isNull();
  }

  /**
   * Verify that a Create Session response with an empty body still
   * produces a non-null (but empty) buffered stream. Guards against
   * NPEs if the service ever returns an unexpectedly empty body.
   *
   * @throws Exception on failure of the mocked I/O flow.
   */
  @Test
  public void testEmptySessionResponseBodyBuffersEmptyStream()
      throws Exception {
    URL url = new URL(
        "https://acct.blob.core.windows.net/mycontainer"
            + "?restype=container&comp=session");
    FakeAbfsHttpOperation op = new FakeAbfsHttpOperation(
        url, "POST", new byte[0]);

    op.processResponse(null, 0, 0);

    InputStream buffered = op.getSessionResultStream();
    assertThat(buffered).isNotNull();
    assertThat(readAll(buffered)).isEmpty();
  }

  // =========================================================================
  // Test double — a minimal AbfsHttpOperation that exposes the wire body
  // via a controllable input stream.
  // =========================================================================

  private static byte[] readAll(InputStream in) throws IOException {
    java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
    byte[] chunk = new byte[512];
    int n;
    while ((n = in.read(chunk)) != -1) {
      buf.write(chunk, 0, n);
    }
    return buf.toByteArray();
  }

  /**
   * Minimal concrete {@link AbfsHttpOperation} used only for exercising
   * {@code parseResponse} routing. Simulates a wire-level input stream
   * with a caller-supplied payload and stubs out the abstract methods
   * that are not on the response-parsing path.
   */
  private static final class FakeAbfsHttpOperation
      extends AbfsHttpOperation {

    private final URL url;
    private final ByteArrayInputStream rawStream;

    FakeAbfsHttpOperation(final URL url, final String method,
        final byte[] body) {
      // The fixed-result base constructor is invoked because it does not
      // require network primitives. Status is set to 200 so parseResponse
      // takes the success path (buffered body routing).
      super(url, method, 200);
      this.url = url;
      this.rawStream = new ByteArrayInputStream(body);
    }

    boolean rawStreamIsExhausted() {
      return rawStream.available() == 0;
    }

    // ----- Response-parsing-path stubs -----

    @Override
    protected InputStream getContentInputStream() {
      return rawStream;
    }

    @Override
    protected InputStream getErrorStream() {
      return null;
    }

    @Override
    public String getResponseHeader(final String httpHeader) {
      return null;
    }

    @Override
    public Map<String, List<String>> getResponseHeaders() {
      return new HashMap<>();
    }

    @Override
    public String getResponseHeaderIgnoreCase(final String httpHeader) {
      return null;
    }

    @Override
    URL getConnUrl() {
      return url;
    }

    // ----- Unused methods stubbed to satisfy the abstract contract -----

    @Override
    public void sendPayload(final byte[] buffer, final int offset,
        final int length) {
    }

    @Override
    public void processResponse(final byte[] buffer, final int offset,
        final int length) throws IOException {
      // Delegate to the real parseResponse logic, which is package-private
      // on the base class.
      parseResponse(buffer, offset, length);
    }

    @Override
    public void setRequestProperty(final String key, final String value) {
    }

    @Override
    String getConnProperty(final String key) {
      return null;
    }

    @Override
    Integer getConnResponseCode() {
      return 200;
    }

    @Override
    String getConnResponseMessage() {
      return "OK";
    }

    @Override
    Map<String, List<String>> getRequestProperties() {
      return Collections.emptyMap();
    }

    @Override
    String getRequestProperty(final String headerName) {
      return null;
    }

    @Override
    public String getTracingContextSuffix() {
      return "";
    }
  }
}
