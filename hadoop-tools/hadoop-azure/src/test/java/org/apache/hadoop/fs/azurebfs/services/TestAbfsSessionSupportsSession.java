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
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link AbfsSessionManager#supportsSession(AbfsRestOperation)}
 * — the request-shape eligibility rule that mirrors the Go SDK's
 * {@code supportsSession} function.
 *
 * <p>Verifies which HTTP methods and URL shapes are eligible for session
 * authentication (accepted by the service) and which must fall back to
 * OAuth (rejected server-side with 403 "Authentication scheme Session is
 * not supported").
 */
@Timeout(value = 10, unit = TimeUnit.SECONDS)
public class TestAbfsSessionSupportsSession {

  private static final String ACCT_HOST =
      "https://acct.blob.core.windows.net";

  private AutoCloseable mocks;

  @BeforeEach
  public void setUp() {
    mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (mocks != null) {
      mocks.close();
    }
  }

  // =========================================================================
  // Accepted: GET blob (data reads)
  // =========================================================================

  /** GET on a plain blob URL is eligible for session auth. */
  @Test
  public void testAcceptsGetBlob() throws Exception {
    AbfsRestOperation op = opFor("GET",
        ACCT_HOST + "/mycontainer/myblob");

    assertThat(AbfsSessionManager.supportsSession(op)).isTrue();
  }

  /** GET on a nested blob path is eligible. */
  @Test
  public void testAcceptsGetNestedBlob() throws Exception {
    AbfsRestOperation op = opFor("GET",
        ACCT_HOST + "/mycontainer/dir1/dir2/myblob.txt");

    assertThat(AbfsSessionManager.supportsSession(op)).isTrue();
  }

  /** GET blob with unrelated query params (no comp=) remains eligible. */
  @Test
  public void testAcceptsGetBlobWithUnrelatedQueryParams() throws Exception {
    AbfsRestOperation op = opFor("GET",
        ACCT_HOST + "/mycontainer/myblob?snapshot=2026-01-01T00:00:00Z");

    assertThat(AbfsSessionManager.supportsSession(op)).isTrue();
  }

  /**
   * The comp= detector must not false-positive on query keys that merely
   * contain the substring "comp" (e.g. "composed=").
   */
  @Test
  public void testAcceptsGetBlobWithCompSubstringInOtherKey()
      throws Exception {
    AbfsRestOperation op = opFor("GET",
        ACCT_HOST + "/mycontainer/myblob?composed=xyz");

    assertThat(AbfsSessionManager.supportsSession(op)).isTrue();
  }

  /** PUT blob (create/write) is rejected. */
  @Test
  public void testRejectsPutBlob() throws Exception {
    AbfsRestOperation op = opFor("PUT",
        ACCT_HOST + "/mycontainer/myblob");

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** DELETE blob is rejected. */
  @Test
  public void testRejectsDeleteBlob() throws Exception {
    AbfsRestOperation op = opFor("DELETE",
        ACCT_HOST + "/mycontainer/myblob");

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** POST is rejected — covers Create Session's request shape. */
  @Test
  public void testRejectsPost() throws Exception {
    AbfsRestOperation op = opFor("POST",
        ACCT_HOST + "/mycontainer?restype=container&comp=session");

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** PATCH is rejected. */
  @Test
  public void testRejectsPatch() throws Exception {
    AbfsRestOperation op = opFor("PATCH",
        ACCT_HOST + "/mycontainer/myblob");

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** GET blob ?comp=metadata is rejected. */
  @Test
  public void testRejectsGetBlobWithCompMetadata() throws Exception {
    AbfsRestOperation op = opFor("GET",
        ACCT_HOST + "/mycontainer/myblob?comp=metadata");

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** GET blob ?comp=blocklist is rejected. */
  @Test
  public void testRejectsGetBlobWithCompBlocklist() throws Exception {
    AbfsRestOperation op = opFor("GET",
        ACCT_HOST + "/mycontainer/myblob?comp=blocklist");

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** comp=list on a container is rejected. */
  @Test
  public void testRejectsListContainer() throws Exception {
    AbfsRestOperation op = opFor("GET",
        ACCT_HOST + "/mycontainer?restype=container&comp=list");

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** comp= appearing anywhere in the query is detected, not just at start. */
  @Test
  public void testRejectsCompAsSecondQueryParam() throws Exception {
    AbfsRestOperation op = opFor("GET",
        ACCT_HOST + "/mycontainer/myblob?other=x&comp=tags");

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** Container-only path (no blob) is rejected. */
  @Test
  public void testRejectsContainerOnlyPath() throws Exception {
    AbfsRestOperation op = opFor("GET",
        ACCT_HOST + "/mycontainer");

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** Container-only path with trailing slash is rejected. */
  @Test
  public void testRejectsContainerOnlyPathTrailingSlash() throws Exception {
    AbfsRestOperation op = opFor("GET",
        ACCT_HOST + "/mycontainer/");

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** Root path (no container, no blob) is rejected. */
  @Test
  public void testRejectsRootPath() throws Exception {
    AbfsRestOperation op = opFor("GET", ACCT_HOST + "/");

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  /** Null operation is rejected without throwing. */
  @Test
  public void testRejectsNullOp() {
    assertThat(AbfsSessionManager.supportsSession(null)).isFalse();
  }

  /** Operation with null URL is rejected without throwing. */
  @Test
  public void testRejectsNullUrl() {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    when(op.getMethod()).thenReturn("GET");
    when(op.getUrl()).thenReturn(null);

    assertThat(AbfsSessionManager.supportsSession(op)).isFalse();
  }

  private static AbfsRestOperation opFor(String method, String url)
      throws Exception {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    when(op.getMethod()).thenReturn(method);
    when(op.getUrl()).thenReturn(new URL(url));
    return op;
  }
}
