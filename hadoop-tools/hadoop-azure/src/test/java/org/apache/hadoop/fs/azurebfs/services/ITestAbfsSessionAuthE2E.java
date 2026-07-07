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
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test end-to-end Blob Storage session authentication against a live
 * account. Verifies that the {@link AbfsBlobClient#createSession} call,
 * the {@link AbfsSessionManager} cache, and the request-signing
 * integration in {@link AbfsRestOperation} interoperate correctly with
 * the service.
 *
 * <p>Requires a Blob-endpoint-capable storage account whose service side
 * has the Create Session API enabled. Tests are skipped automatically
 * when the configured account is not on the Blob endpoint, or when
 * {@code fs.azure.enable.session.auth} is not set to true.
 *
 * <p>Compatible with both authentication paths. In production, the
 * account uses OAuth and Create Session is authorized by the OAuth
 * bearer token. In pre-OAuth test environments, the account uses
 * SharedKey with {@code fs.azure.allow.shared.key.session.auth} set to
 * true so that Create Session is authorized by the account key; this
 * config is a test-only escape hatch and must not be enabled in
 * production.
 */
public class ITestAbfsSessionAuthE2E extends AbstractAbfsIntegrationTest {

  private static final String TEST_DATA = "session-auth-e2e-payload";
  private static final int CONCURRENT_READERS = 8;

  private AzureBlobFileSystem fs;
  private AbfsBlobClient blobClient;
  private AbfsSessionManager sessionManager;

  public ITestAbfsSessionAuthE2E() throws Exception {
    super();
  }

  @BeforeEach
  public void setUp() throws Exception {
    Assumptions.assumeTrue(
        getAbfsServiceType() == AbfsServiceType.BLOB,
        "Session authentication is only supported on the Blob endpoint.");

    Assumptions.assumeTrue(
        getConfiguration().isSessionAuthEnabled(),
        "Session authentication is disabled; set "
            + "fs.azure.enable.session.auth=true to enable this suite.");

    fs = getFileSystem();
    blobClient = (AbfsBlobClient) fs.getAbfsStore()
        .getClient(AbfsServiceType.BLOB);
    sessionManager = blobClient.getSessionManager();
  }

  /**
   * Verify Create Session returns a well-formed response with all
   * required fields populated and an expiration time in the expected
   * range.
   *
   * @throws Exception on failure of the round-trip call.
   */
  @Test
  public void testCreateSessionRoundTrip() throws Exception {
    final Instant callTime = Instant.now();
    SessionCredentials creds = blobClient.createSession(
        getTestTracingContext(fs, true));

    assertThat(creds).isNotNull();
    assertThat(creds.getSessionId()).isNotBlank();
    assertThat(creds.getSessionToken()).isNotBlank();
    assertThat(creds.getSessionKey()).isNotEmpty();
    assertThat(creds.getAuthenticationType())
        .isEqualToIgnoringCase("HMAC");
    assertThat(creds.getExpirationTime()).isAfter(callTime);
    // Sessions default to a five-minute lifetime per the Create Session
    // contract. Allow a ten-minute upper bound to tolerate service
    // variance without pinning the test to an exact value.
    assertThat(creds.getExpirationTime())
        .isBefore(callTime.plusSeconds(10 * 60));
  }

  /**
   * Verify that two back-to-back Create Session calls mint distinct
   * sessions on the server. Guards against any accidental server-side
   * or client-side caching of session identifiers.
   *
   * @throws Exception on failure of either call.
   */
  @Test
  public void testTwoCreateSessionCallsReturnDistinctSessions()
      throws Exception {
    SessionCredentials first = blobClient.createSession(
        getTestTracingContext(fs, true));
    SessionCredentials second = blobClient.createSession(
        getTestTracingContext(fs, true));

    assertThat(second.getSessionId()).isNotEqualTo(first.getSessionId());
    assertThat(second.getSessionToken())
        .isNotEqualTo(first.getSessionToken());
  }

  /**
   * Verify that the session manager caches a live session across
   * requests. The first call mints a session via the wire; the second
   * call must return the same instance without hitting the service.
   *
   * @throws Exception on failure of the manager or the wire call.
   */
  @Test
  public void testSessionManagerCachesLiveSession() throws Exception {
    // Any previous test may have populated the cache. Reset to isolate.
    sessionManager.invalidateCurrentSession();

    SessionKeyCredentials first = sessionManager.getSessionCredentials(
        getTestTracingContext(fs, true));
    SessionKeyCredentials second = sessionManager.getSessionCredentials(
        getTestTracingContext(fs, true));

    assertThat(first).isNotNull();
    assertThat(second).isSameAs(first);
  }

  /**
   * Verify that {@link AbfsSessionManager#invalidateCurrentSession}
   * clears the cache and forces a fresh Create Session on the next
   * request. The new credentials must carry a different session token
   * from the invalidated ones.
   *
   * @throws Exception on failure of any manager or wire call.
   */
  @Test
  public void testInvalidationForcesFreshLiveSession() throws Exception {
    sessionManager.invalidateCurrentSession();

    SessionKeyCredentials before = sessionManager.getSessionCredentials(
        getTestTracingContext(fs, true));
    assertThat(before).isNotNull();
    final String tokenBefore = before.getSessionToken();

    sessionManager.invalidateCurrentSession();

    SessionKeyCredentials after = sessionManager.getSessionCredentials(
        getTestTracingContext(fs, true));
    assertThat(after).isNotNull();
    assertThat(after).isNotSameAs(before);
    assertThat(after.getSessionToken()).isNotEqualTo(tokenBefore);
  }

  /**
   * Verify a full create-read-delete file lifecycle with session
   * authentication enabled. Exercises the request-signing switch, the
   * eligibility check on the session manager, and wire-level session
   * signing on the read path.
   *
   * @throws Exception on failure of any step of the lifecycle.
   */
  @Test
  public void testCreateReadDeleteWithSessionAuth() throws Exception {
    final Path path = uniqueTestPath("basic");

    writeFile(path, TEST_DATA);
    assertReadBack(path, TEST_DATA);
    assertThat(fs.delete(path, false)).isTrue();
  }

  /**
   * Verify session-signed reads work correctly across a large file that
   * triggers multiple range reads under the hood. Confirms the cached
   * session survives the sequence of GETs issued during the read.
   *
   * @throws Exception on failure of the write, the read, or cleanup.
   */
  @Test
  public void testLargeReadWithSessionAuth() throws Exception {
    final Path path = uniqueTestPath("large");
    final int size = 4 * 1024 * 1024;   // 4 MiB
    final byte[] payload = generatePayload(size);

    try (OutputStream out = fs.create(path, true)) {
      out.write(payload);
    }

    try (FSDataInputStream in = fs.open(path)) {
      byte[] readBuffer = new byte[size];
      int totalRead = 0;
      while (totalRead < size) {
        int n = in.read(readBuffer, totalRead, size - totalRead);
        if (n < 0) {
          break;
        }
        totalRead += n;
      }
      assertThat(totalRead).isEqualTo(size);
      assertThat(readBuffer).isEqualTo(payload);
    } finally {
      fs.delete(path, false);
    }
  }

  /**
   * Verify the single-flight guarantee on a cold cache against the live
   * service. When N threads race for credentials, exactly one Create
   * Session call is issued and every thread observes the same instance.
   *
   * @throws Exception on failure of any worker or the join.
   */
  @Test
  public void testSingleFlightUnderConcurrentReaders() throws Exception {
    sessionManager.invalidateCurrentSession();

    final int threads = CONCURRENT_READERS;
    final CountDownLatch startGate = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);
    final SessionKeyCredentials[] observed =
        new SessionKeyCredentials[threads];
    final AtomicInteger errors = new AtomicInteger();

    ExecutorService pool = Executors.newFixedThreadPool(threads);
    for (int i = 0; i < threads; i++) {
      final int idx = i;
      pool.submit(() -> {
        try {
          startGate.await();
          observed[idx] = sessionManager.getSessionCredentials(
              getTestTracingContext(fs, true));
        } catch (Exception e) {
          errors.incrementAndGet();
        } finally {
          done.countDown();
        }
      });
    }

    startGate.countDown();
    assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    pool.shutdown();

    assertThat(errors.get()).isZero();

    SessionKeyCredentials winner = observed[0];
    assertThat(winner).isNotNull();
    for (int i = 1; i < threads; i++) {
      assertThat(observed[i]).isSameAs(winner);
    }
  }

  /**
   * Verify concurrent readers against the same file reuse a single
   * cached session across all reads. The manager must not re-mint a
   * session on parallel data-plane requests.
   *
   * @throws Exception on failure of any reader or cleanup.
   */
  @Test
  public void testConcurrentReadersReuseSingleSession() throws Exception {
    final Path path = uniqueTestPath("concurrent");
    writeFile(path, TEST_DATA);
    sessionManager.invalidateCurrentSession();

    // Prime the cache so we can capture the initial session instance.
    SessionKeyCredentials initial = sessionManager.getSessionCredentials(
        getTestTracingContext(fs, true));
    assertThat(initial).isNotNull();

    final int readers = CONCURRENT_READERS;
    final CountDownLatch startGate = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(readers);
    final AtomicInteger successes = new AtomicInteger();
    final AtomicInteger errors = new AtomicInteger();

    ExecutorService pool = Executors.newFixedThreadPool(readers);
    for (int i = 0; i < readers; i++) {
      pool.submit(() -> {
        try {
          startGate.await();
          assertReadBack(path, TEST_DATA);
          successes.incrementAndGet();
        } catch (Throwable t) {
          errors.incrementAndGet();
        } finally {
          done.countDown();
        }
      });
    }

    startGate.countDown();
    assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
    pool.shutdown();

    assertThat(errors.get()).isZero();
    assertThat(successes.get()).isEqualTo(readers);

    // Parallel reads must not cause the manager to re-mint a session.
    SessionKeyCredentials afterReads = sessionManager.getSessionCredentials(
        getTestTracingContext(fs, true));
    assertThat(afterReads).isSameAs(initial);

    fs.delete(path, false);
  }

  /**
   * Verify session invalidation between reads is transparent to the
   * caller. After the cached session is cleared, the next read must
   * still succeed either by minting a fresh session or by falling back
   * to the underlying OAuth or SharedKey credential.
   *
   * @throws Exception on failure of any operation in the sequence.
   */
  @Test
  public void testInvalidateBetweenReadsIsTransparent() throws Exception {
    final Path path = uniqueTestPath("invalidate");
    writeFile(path, TEST_DATA);
    try {
      assertReadBack(path, TEST_DATA);

      sessionManager.invalidateCurrentSession();

      // The subsequent read must still succeed via a fresh session or
      // via the OAuth or SharedKey fallback path.
      assertReadBack(path, TEST_DATA);
    } finally {
      fs.delete(path, false);
    }
  }

  /**
   * Write the given payload to the specified path.
   *
   * @param path path to write.
   * @param content payload to write as UTF-8.
   * @throws IOException on failure of the write.
   */
  private void writeFile(Path path, String content) throws IOException {
    try (OutputStream out = fs.create(path, true)) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }
  }

  /**
   * Read the specified path and assert the contents match the expected
   * payload.
   *
   * @param path path to read.
   * @param expected expected UTF-8 payload.
   * @throws IOException on failure of the read.
   */
  private void assertReadBack(Path path, String expected) throws IOException {
    try (FSDataInputStream in = fs.open(path)) {
      byte[] buf = new byte[expected.length()];
      int totalRead = 0;
      while (totalRead < expected.length()) {
        int n = in.read(buf, totalRead, expected.length() - totalRead);
        if (n < 0) {
          break;
        }
        totalRead += n;
      }
      assertThat(new String(buf, 0, totalRead, StandardCharsets.UTF_8))
          .isEqualTo(expected);
    }
  }

  /**
   * Build a unique test path under the shared scratch directory.
   *
   * @param label short descriptor embedded in the file name.
   * @return unique path.
   */
  private Path uniqueTestPath(String label) {
    return new Path("/session-auth-e2e/" + label + "-"
        + UUID.randomUUID() + ".txt");
  }

  /**
   * Generate a deterministic payload of the given size.
   *
   * @param size number of bytes to generate.
   * @return payload buffer.
   */
  private static byte[] generatePayload(int size) {
    byte[] payload = new byte[size];
    for (int i = 0; i < size; i++) {
      payload[i] = (byte) (i & 0xff);
    }
    return payload;
  }
}
