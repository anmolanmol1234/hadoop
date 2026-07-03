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

import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.utils.Base64;

/**
 * Session credentials used to authenticate requests within an Azure Storage
 * Blob session.
 *
 * <p>This implementation signs requests using the Shared Key signing
 * algorithm with the session key returned by the Create Session API and
 * constructs the {@code Authorization} header using the Session
 * authentication scheme.</p>
 */
public class SessionKeyCredentials extends StorageRequestSigner {

  private static final Logger LOG = LoggerFactory.getLogger(SessionKeyCredentials.class);

  private static final String SESSION_SCHEME = "Session";

  /**
   * The opaque session token returned by Create Session. Included verbatim
   * in the {@code Authorization} header — it identifies the session on the
   * server and stands in the place occupied by the account name in the
   * classic Shared Key scheme.
   */
  private final String sessionToken;

  /**
   * Constructs session credentials from raw session key bytes. Use this
   * overload when the session key was already Base64-decoded by the
   * response parser.
   *
   * @param accountName  storage account name (used for canonicalized resource)
   * @param sessionToken opaque session token returned by Create Session
   * @param sessionKey   raw HMAC-SHA256 signing key bytes
   */
  public SessionKeyCredentials(final String accountName,
      final String sessionToken,
      final byte[] sessionKey) {
    super(accountName, sessionKey);
    if (sessionToken == null || sessionToken.isEmpty()) {
      throw new IllegalArgumentException("Invalid session token.");
    }
    this.sessionToken = sessionToken;
  }

  /**
   * Constructs session credentials from a Base64-encoded session key.
   * Convenience overload that mirrors the {@link SharedKeyCredentials}
   * string constructor.
   *
   * @param accountName        storage account name
   * @param sessionToken       opaque session token returned by Create Session
   * @param base64SessionKey   Base64-encoded session key from the service response
   */
  public SessionKeyCredentials(final String accountName,
      final String sessionToken,
      final String base64SessionKey) {
    this(accountName, sessionToken, decodeSessionKey(base64SessionKey));
  }

  /**
   * Signs the request using the Session authentication scheme.
   *
   * <ol>
   *   <li>Sets the {@code x-ms-date} request header with the current GMT timestamp.</li>
   *   <li>Builds the canonicalized string-to-sign for the request.</li>
   *   <li>Computes the HMAC-SHA256 signature using the session key.</li>
   *   <li>Constructs the {@code Authorization} header in the format
   *       {@code Session <sessionToken>:<signature>}.</li>
   *   <li>Adds the {@code Authorization} header to the outgoing request.</li>
   * </ol>
   *
   * @param connection HTTP operation to sign.
   * @param contentLength request body content length.
   * @throws UnsupportedEncodingException if request canonicalization fails.
   */
  @Override
  public void signRequest(final AbfsHttpOperation connection,
      final long contentLength)
      throws UnsupportedEncodingException {

    final String gmtTime = stampGmtDate(connection);

    final String stringToSign = buildStringToSign(connection, contentLength);
    final String signature = computeHmac256(stringToSign);
    final String authorizationValue = String.format("%s %s:%s",
        SESSION_SCHEME, sessionToken, signature);
    setAuthorizationHeader(connection, authorizationValue);
    LOG.debug("Session-signing request with timestamp {} and signature {}",
        gmtTime, authorizationValue);
  }

  /**
   * Returns the session token.
   *
   * @return session token.
   */
  public String getSessionToken() {
    return sessionToken;
  }

  /**
   * Decodes the Base64-encoded session signing key.
   *
   * @param base64SessionKey Base64-encoded session signing key.
   * @return decoded session signing key.
   * @throws IllegalArgumentException if the session key is null or empty.
   */
  private static byte[] decodeSessionKey(final String base64SessionKey) {
    if (base64SessionKey == null || base64SessionKey.isEmpty()) {
      throw new IllegalArgumentException("Invalid session key.");
    }
    return Base64.decode(base64SessionKey);
  }
}
