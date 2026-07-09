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

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link SessionCredentials}: field accessors,
 * defensive-copy semantics for the session key, and null-argument
 * handling.
 *
 * <p>Verifies that the session key — a sensitive HMAC secret — is
 * defensively copied on both construction and access, so callers
 * cannot mutate the internal key material through either the
 * constructor argument or the getter return value.
 */
@Timeout(value = 10)
public class TestSessionCredentials {

  private static final Instant EXPIRY =
      Instant.parse("2026-07-02T10:15:30Z");

  /**
   * Verify that all fields supplied to the constructor are round-tripped
   * through the accessors.
   *
   * @throws Exception on failure of any assertion.
   */
  @Test
  public void testConstructorPreservesAllFields() throws Exception {
    byte[] key = "secret-key-bytes".getBytes();

    SessionCredentials creds = new SessionCredentials(
        "session-id", "session-token", key, "HMAC", EXPIRY);

    assertThat(creds.getSessionId()).isEqualTo("session-id");
    assertThat(creds.getSessionToken()).isEqualTo("session-token");
    assertThat(creds.getAuthenticationType()).isEqualTo("HMAC");
    assertThat(creds.getExpirationTime()).isEqualTo(EXPIRY);
    assertThat(creds.getSessionKey()).isEqualTo(key);
  }

  /**
   * Verify that mutating the caller's byte array after construction
   * does not affect the credentials' internal key. Proves the
   * constructor defensively copies the input.
   *
   * @throws Exception on failure of any assertion.
   */
  @Test
  public void testConstructorDefensivelyCopiesSessionKey()
      throws Exception {
    byte[] key = {1, 2, 3, 4};
    SessionCredentials creds = new SessionCredentials(
        "id", "token", key, "HMAC", EXPIRY);

    // Mutate the caller's copy.
    key[0] = (byte) 99;

    // The credentials must still reflect the original bytes.
    assertThat(creds.getSessionKey()[0]).isEqualTo((byte) 1);
    assertThat(creds.getSessionKey()).isEqualTo(new byte[]{1, 2, 3, 4});
  }

  /**
   * Verify that mutating the byte array returned by {@code getSessionKey}
   * does not affect the credentials' internal key. Proves the getter
   * defensively copies on return.
   *
   * @throws Exception on failure of any assertion.
   */
  @Test
  public void testGetSessionKeyReturnsDefensiveCopy() throws Exception {
    byte[] key = {1, 2, 3, 4};
    SessionCredentials creds = new SessionCredentials(
        "id", "token", key, "HMAC", EXPIRY);

    byte[] copy = creds.getSessionKey();
    copy[0] = (byte) 99;

    byte[] freshCopy = creds.getSessionKey();
    assertThat(freshCopy[0]).isEqualTo((byte) 1);
    assertThat(freshCopy).isEqualTo(new byte[]{1, 2, 3, 4});
  }

  /**
   * Verify that repeated calls to {@code getSessionKey} each return a
   * distinct array instance. Guarantees that no shared reference is
   * exposed across calls.
   *
   * @throws Exception on failure of any assertion.
   */
  @Test
  public void testGetSessionKeyReturnsFreshArrayEachCall()
      throws Exception {
    SessionCredentials creds = new SessionCredentials(
        "id", "token", new byte[]{1, 2, 3}, "HMAC", EXPIRY);

    byte[] a = creds.getSessionKey();
    byte[] b = creds.getSessionKey();

    assertThat(a).isNotSameAs(b);
    assertThat(a).isEqualTo(b);
  }

  /**
   * Verify that null constructor arguments are rejected with
   * {@link NullPointerException}. Prevents partially-constructed
   * credentials from silently propagating through the driver.
   */
  @Test
  public void testNullConstructorArgumentsAreRejected() {
    assertThatThrownBy(() -> new SessionCredentials(
        null, "token", new byte[]{1}, "HMAC", EXPIRY))
        .isInstanceOf(NullPointerException.class);

    assertThatThrownBy(() -> new SessionCredentials(
        "id", null, new byte[]{1}, "HMAC", EXPIRY))
        .isInstanceOf(NullPointerException.class);

    assertThatThrownBy(() -> new SessionCredentials(
        "id", "token", null, "HMAC", EXPIRY))
        .isInstanceOf(NullPointerException.class);

    assertThatThrownBy(() -> new SessionCredentials(
        "id", "token", new byte[]{1}, null, EXPIRY))
        .isInstanceOf(NullPointerException.class);

    assertThatThrownBy(() -> new SessionCredentials(
        "id", "token", new byte[]{1}, "HMAC", null))
        .isInstanceOf(NullPointerException.class);
  }

  /**
   * Verify that an empty session key byte array is preserved as-is
   * through both construction and access without special-case
   * substitution.
   *
   * @throws Exception on failure of any assertion.
   */
  @Test
  public void testEmptySessionKeyPreserved() throws Exception {
    SessionCredentials creds = new SessionCredentials(
        "id", "token", new byte[0], "HMAC", EXPIRY);

    assertThat(creds.getSessionKey()).isEmpty();
    assertThat(creds.getSessionKey()).isNotNull();
  }
}
