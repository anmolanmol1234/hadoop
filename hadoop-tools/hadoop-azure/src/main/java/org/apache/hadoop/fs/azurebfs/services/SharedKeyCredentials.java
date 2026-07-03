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
 * Shared Key credentials used to authenticate requests to an Azure Storage
 * account.
 *
 * <p>This implementation signs requests using the Shared Key authentication
 * scheme by generating a canonicalized string, computing its HMAC-SHA256
 * signature with the storage account key, and adding the corresponding
 * {@code Authorization} header.</p>
 */
public class SharedKeyCredentials extends StorageRequestSigner {

  private static final Logger LOG = LoggerFactory.getLogger(SharedKeyCredentials.class);
  private static final String SHARED_KEY_SCHEME = "SharedKey";

  /**
   * Creates Shared Key credentials for the specified storage account.
   *
   * @param accountName storage account name.
   * @param accountKey Base64-encoded storage account key.
   */
  public SharedKeyCredentials(final String accountName, final String accountKey) {
    super(accountName, decodeAccountKey(accountKey));
  }

  /**
   * Signs the request using the Shared Key authentication scheme.
   *
   * <ol>
   *   <li>Sets the {@code x-ms-date} request header with the current GMT timestamp.</li>
   *   <li>Builds the canonicalized string-to-sign for the request.</li>
   *   <li>Computes the HMAC-SHA256 signature using the storage account key.</li>
   *   <li>Constructs the {@code Authorization} header in the format
   *       {@code SharedKey <accountName>:<signature>}.</li>
   *   <li>Adds the {@code Authorization} header to the outgoing request.</li>
   * </ol>
   *
   * @param connection HTTP operation to sign.
   * @param contentLength request body content length.
   * @throws UnsupportedEncodingException if request canonicalization fails.
   */
  @Override
  public void signRequest(final AbfsHttpOperation connection, final long contentLength)
      throws UnsupportedEncodingException {

    final String gmtTime = stampGmtDate(connection);
    final String stringToSign = buildStringToSign(connection, contentLength);
    final String signature = computeHmac256(stringToSign);
    final String authorizationValue = String.format("%s %s:%s",
        SHARED_KEY_SCHEME, getAccountName(), signature);
    setAuthorizationHeader(connection, authorizationValue);
    LOG.debug("SharedKey-signing request with timestamp {} and signature {}",
        gmtTime, authorizationValue);
  }

  /**
   * Decodes the Base64-encoded storage account key.
   *
   * @param accountKey Base64-encoded storage account key.
   * @return decoded account key bytes.
   * @throws IllegalArgumentException if the account key is null or empty.
   */
  private static byte[] decodeAccountKey(final String accountKey) {
    if (accountKey == null || accountKey.isEmpty()) {
      throw new IllegalArgumentException("Invalid account key.");
    }
    return Base64.decode(accountKey);
  }
}
