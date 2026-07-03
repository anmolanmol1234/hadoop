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

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.utils.Base64;

/**
 * Abstract base class providing common functionality for signing Azure Storage
 * requests.
 *
 * <p>This class encapsulates the canonicalization and HMAC-SHA256 signing
 * logic shared by different authentication mechanisms. Concrete subclasses
 * implement {@link #signRequest(AbfsHttpOperation, long)} to construct the
 * appropriate authorization header for the request.</p>
 */
public abstract class StorageRequestSigner {

  private static final int EXPECTED_BLOB_QUEUE_CANONICALIZED_STRING_LENGTH = 300;
  private static final Pattern CRLF = Pattern.compile("\r\n", Pattern.LITERAL);
  private static final String HMAC_SHA256 = "HmacSHA256";

  /**
   * Stores a reference to the RFC1123 date/time pattern.
   */
  private static final String RFC1123_PATTERN = "EEE, dd MMM yyyy HH:mm:ss z";

  public static final TimeZone GMT_ZONE =
      TimeZone.getTimeZone(AbfsHttpConstants.GMT_TIMEZONE);

  /**
   * Thread local for storing GMT date format.
   */
  private static final ThreadLocal<DateFormat> RFC1123_GMT_DATE_TIME_FORMATTER =
      new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
          final DateFormat formatter =
              new SimpleDateFormat(RFC1123_PATTERN, Locale.ROOT);
          formatter.setTimeZone(GMT_ZONE);
          return formatter;
        }
      };

  /**
   * The account name used when constructing the canonicalized resource
   * string (i.e. the first segment after the leading '/'). Both Shared Key
   * and Session authentication canonicalize the resource against the
   * storage account name.
   */
  private final String accountName;

  /**
   * HMAC-SHA256 instance keyed with the signing key supplied by the
   * concrete subclass. Access must be synchronized on {@code this} because
   * {@link Mac} is not thread-safe.
   */
  private final Mac hmacSha256;

  /**
   * Creates a request signer using the specified storage account and signing key.
   *
   * @param accountName storage account name used for request canonicalization.
   * @param signingKey raw signing key bytes.
   */
  protected StorageRequestSigner(final String accountName,
      final byte[] signingKey) {
    if (accountName == null || accountName.isEmpty()) {
      throw new IllegalArgumentException("Invalid account name.");
    }
    if (signingKey == null || signingKey.length == 0) {
      throw new IllegalArgumentException("Invalid signing key.");
    }
    this.accountName = accountName;
    this.hmacSha256 = initializeMac(signingKey);
  }

  /**
   * Returns the storage account name associated with this signer.
   *
   * @return storage account name.
   */
  protected final String getAccountName() {
    return accountName;
  }

   /**
   * Signs the specified request using the authentication mechanism implemented
   * by the concrete subclass.
   *
   * @param connection HTTP operation to sign.
   * @param contentLength request body content length.
   * @throws UnsupportedEncodingException if request canonicalization fails.
   */
  public abstract void signRequest(AbfsHttpOperation connection,
      long contentLength)
      throws UnsupportedEncodingException;

  /**
   * Sets the current GMT timestamp in the {@code x-ms-date} request header.
   *
   * @param connection HTTP operation being signed.
   * @return RFC1123 formatted GMT timestamp.
   */
  protected final String stampGmtDate(final AbfsHttpOperation connection) {
    final String gmtTime = getGMTTime();
    connection.setRequestProperty(HttpHeaderConfigurations.X_MS_DATE, gmtTime);
    return gmtTime;
  }

  /**
   * Builds the canonicalized string used to generate the request signature.
   *
   * @param conn HTTP operation to canonicalize.
   * @param contentLength request body content length.
   * @return canonicalized string to sign.
   * @throws UnsupportedEncodingException if URL decoding fails.
   */
  protected final String buildStringToSign(final AbfsHttpOperation conn,
      final long contentLength)
      throws UnsupportedEncodingException {
    if (contentLength < -1) {
      throw new IllegalArgumentException(
          "The Content-Length header must be greater than or equal to -1.");
    }

    final String contentType =
        getHeaderValue(conn, HttpHeaderConfigurations.CONTENT_TYPE, "");

    return canonicalizeHttpRequest(conn.getConnUrl(), accountName,
        conn.getMethod(), contentType, contentLength, null, conn);
  }

  /**
   * Computes the HMAC-SHA256 signature for the specified canonicalized string.
   *
   * @param stringToSign canonicalized request string.
   * @return Base64-encoded HMAC-SHA256 signature.
   */
  protected final String computeHmac256(final String stringToSign) {
    final byte[] utf8Bytes = stringToSign.getBytes(StandardCharsets.UTF_8);
    byte[] hmac;
    synchronized (this) {
      hmac = hmacSha256.doFinal(utf8Bytes);
    }
    return Base64.encode(hmac);
  }

  /**
   * Sets the Authorization header on the specified request.
   *
   * @param connection HTTP operation.
   * @param authorizationValue authorization header value.
   */
  protected final void setAuthorizationHeader(final AbfsHttpOperation connection,
      final String authorizationValue) {
    connection.setRequestProperty(HttpHeaderConfigurations.AUTHORIZATION,
        authorizationValue);
  }

  /**
   * Initializes an HMAC-SHA256 instance using the specified signing key.
   *
   * @param signingKey raw signing key bytes.
   * @return initialized HMAC-SHA256 instance.
   */
  private static Mac initializeMac(final byte[] signingKey) {
    try {
      final Mac mac = Mac.getInstance(HMAC_SHA256);
      mac.init(new SecretKeySpec(signingKey, HMAC_SHA256));
      return mac;
    } catch (final Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Builds the canonicalized representation of the specified HTTP request.
   *
   * @param address request URL.
   * @param accountName storage account name.
   * @param method HTTP method.
   * @param contentType request content type.
   * @param contentLength request body content length.
   * @param date request date.
   * @param conn HTTP operation.
   * @return canonicalized request string.
   * @throws UnsupportedEncodingException if URL decoding fails.
   */
  private static String canonicalizeHttpRequest(final URL address,
      final String accountName, final String method, final String contentType,
      final long contentLength, final String date, final AbfsHttpOperation conn)
      throws UnsupportedEncodingException {

    final StringBuilder canonicalizedString =
        new StringBuilder(EXPECTED_BLOB_QUEUE_CANONICALIZED_STRING_LENGTH);
    canonicalizedString.append(conn.getMethod());

    appendCanonicalizedElement(canonicalizedString,
        getHeaderValue(conn, HttpHeaderConfigurations.CONTENT_ENCODING,
            AbfsHttpConstants.EMPTY_STRING));
    appendCanonicalizedElement(canonicalizedString,
        getHeaderValue(conn, HttpHeaderConfigurations.CONTENT_LANGUAGE,
            AbfsHttpConstants.EMPTY_STRING));
    appendCanonicalizedElement(canonicalizedString,
        contentLength <= 0 ? "" : String.valueOf(contentLength));
    appendCanonicalizedElement(canonicalizedString,
        getHeaderValue(conn, HttpHeaderConfigurations.CONTENT_MD5,
            AbfsHttpConstants.EMPTY_STRING));
    appendCanonicalizedElement(canonicalizedString,
        contentType != null ? contentType : AbfsHttpConstants.EMPTY_STRING);

    final String dateString = getHeaderValue(conn,
        HttpHeaderConfigurations.X_MS_DATE, AbfsHttpConstants.EMPTY_STRING);
    // If x-ms-date header exists, Date should be empty string
    appendCanonicalizedElement(canonicalizedString,
        dateString.equals(AbfsHttpConstants.EMPTY_STRING) ? date : "");

    appendCanonicalizedElement(canonicalizedString,
        getHeaderValue(conn, HttpHeaderConfigurations.IF_MODIFIED_SINCE,
            AbfsHttpConstants.EMPTY_STRING));
    appendCanonicalizedElement(canonicalizedString,
        getHeaderValue(conn, HttpHeaderConfigurations.IF_MATCH,
            AbfsHttpConstants.EMPTY_STRING));
    appendCanonicalizedElement(canonicalizedString,
        getHeaderValue(conn, HttpHeaderConfigurations.IF_NONE_MATCH,
            AbfsHttpConstants.EMPTY_STRING));
    appendCanonicalizedElement(canonicalizedString,
        getHeaderValue(conn, HttpHeaderConfigurations.IF_UNMODIFIED_SINCE,
            AbfsHttpConstants.EMPTY_STRING));
    appendCanonicalizedElement(canonicalizedString,
        getHeaderValue(conn, HttpHeaderConfigurations.RANGE,
            AbfsHttpConstants.EMPTY_STRING));

    addCanonicalizedHeaders(conn, canonicalizedString);

    appendCanonicalizedElement(canonicalizedString,
        getCanonicalizedResource(address, accountName));

    return canonicalizedString.toString();
  }

  /**
   * Appends all canonicalized {@code x-ms-*} headers to the string being signed.
   *
   * @param conn HTTP operation.
   * @param canonicalizedString canonicalized string builder.
   */
  private static void addCanonicalizedHeaders(final AbfsHttpOperation conn,
      final StringBuilder canonicalizedString) {
    final Map<String, List<String>> headers = conn.getRequestProperties();
    final ArrayList<String> httpStorageHeaderNameArray = new ArrayList<String>();

    for (final String key : headers.keySet()) {
      if (key.toLowerCase(Locale.ROOT)
          .startsWith(AbfsHttpConstants.HTTP_HEADER_PREFIX)) {
        httpStorageHeaderNameArray.add(key.toLowerCase(Locale.ROOT));
      }
    }

    Collections.sort(httpStorageHeaderNameArray);

    for (final String key : httpStorageHeaderNameArray) {
      final StringBuilder canonicalizedElement = new StringBuilder(key);
      String delimiter = ":";
      final ArrayList<String> values = getHeaderValues(headers, key);

      boolean appendCanonicalizedElement = false;
      for (final String value : values) {
        if (value != null) {
          appendCanonicalizedElement = true;
        }

        // Unfolding is simply removal of CRLF.
        final String unfoldedValue = CRLF.matcher(value)
            .replaceAll(Matcher.quoteReplacement(""));

        canonicalizedElement.append(delimiter);
        canonicalizedElement.append(unfoldedValue);
        delimiter = ",";
      }

      if (appendCanonicalizedElement) {
        appendCanonicalizedElement(canonicalizedString,
            canonicalizedElement.toString());
      }
    }
  }

  /**
   * Returns the canonicalized resource string for the specified request URL.
   *
   * @param address request URL.
   * @param accountName storage account name.
   * @return canonicalized resource string.
   * @throws UnsupportedEncodingException if URL decoding fails.
   */
  private static String getCanonicalizedResource(final URL address,
      final String accountName) throws UnsupportedEncodingException {
    final StringBuilder resourcepath =
        new StringBuilder(AbfsHttpConstants.FORWARD_SLASH);
    resourcepath.append(accountName);

    resourcepath.append(address.getPath());
    final StringBuilder canonicalizedResource =
        new StringBuilder(resourcepath.toString());

    if (address.getQuery() == null
        || !address.getQuery().contains(AbfsHttpConstants.EQUAL)) {
      return canonicalizedResource.toString();
    }

    final Map<String, String[]> queryVariables =
        parseQueryString(address.getQuery());

    final Map<String, String> lowercasedKeyNameValue = new HashMap<>();

    for (final Entry<String, String[]> entry : queryVariables.entrySet()) {
      final List<String> sortedValues = Arrays.asList(entry.getValue());
      Collections.sort(sortedValues);

      final StringBuilder stringValue = new StringBuilder();

      for (final String value : sortedValues) {
        if (stringValue.length() > 0) {
          stringValue.append(AbfsHttpConstants.COMMA);
        }
        stringValue.append(value);
      }

      lowercasedKeyNameValue.put((entry.getKey()) == null ? null
          : entry.getKey().toLowerCase(Locale.ROOT), stringValue.toString());
    }

    final ArrayList<String> sortedKeys =
        new ArrayList<String>(lowercasedKeyNameValue.keySet());
    Collections.sort(sortedKeys);

    for (final String key : sortedKeys) {
      final StringBuilder queryParamString = new StringBuilder();
      queryParamString.append(key);
      queryParamString.append(":");
      queryParamString.append(lowercasedKeyNameValue.get(key));

      appendCanonicalizedElement(canonicalizedResource,
          queryParamString.toString());
    }

    return canonicalizedResource.toString();
  }

  /**
   * Appends a canonicalized element followed by a newline separator.
   *
   * @param builder canonicalized string builder.
   * @param element canonicalized element.
   */
  private static void appendCanonicalizedElement(final StringBuilder builder,
      final String element) {
    builder.append("\n");
    builder.append(element);
  }

  /**
   * Returns all values associated with the specified request header.
   *
   * @param headers request headers.
   * @param headerName header name.
   * @return list of header values.
   */
  private static ArrayList<String> getHeaderValues(
      final Map<String, List<String>> headers,
      final String headerName) {

    final ArrayList<String> arrayOfValues = new ArrayList<String>();
    List<String> values = null;

    for (final Entry<String, List<String>> entry : headers.entrySet()) {
      if (entry.getKey().toLowerCase(Locale.ROOT).equals(headerName)) {
        values = entry.getValue();
        break;
      }
    }
    if (values != null) {
      for (final String value : values) {
        arrayOfValues.add(trimStart(value));
      }
    }
    return arrayOfValues;
  }

  /**
   * Parses the specified query string into a map of query parameters.
   *
   * @param parseString query string.
   * @return parsed query parameters.
   * @throws UnsupportedEncodingException if URL decoding fails.
   */
  private static HashMap<String, String[]> parseQueryString(String parseString)
      throws UnsupportedEncodingException {
    final HashMap<String, String[]> retVals = new HashMap<>();
    if (parseString == null || parseString.isEmpty()) {
      return retVals;
    }

    final int queryDex = parseString.indexOf(AbfsHttpConstants.QUESTION_MARK);
    if (queryDex >= 0 && parseString.length() > 0) {
      parseString = parseString.substring(queryDex + 1);
    }

    final String[] valuePairs =
        parseString.contains(AbfsHttpConstants.AND_MARK)
            ? parseString.split(AbfsHttpConstants.AND_MARK)
            : parseString.split(AbfsHttpConstants.SEMICOLON);

    for (int m = 0; m < valuePairs.length; m++) {
      final int equalDex = valuePairs[m].indexOf(AbfsHttpConstants.EQUAL);

      if (equalDex < 0 || equalDex == valuePairs[m].length() - 1) {
        continue;
      }

      String key = valuePairs[m].substring(0, equalDex);
      String value = valuePairs[m].substring(equalDex + 1);

      key = safeDecode(key);
      value = safeDecode(value);

      String[] values = retVals.get(key);

      if (values == null) {
        values = new String[]{value};
        if (!value.equals("")) {
          retVals.put(key, values);
        }
      }
    }

    return retVals;
  }

  /**
   * URL-decodes the specified string while preserving '+' characters.
   *
   * @param stringToDecode string to decode.
   * @return decoded string.
   * @throws UnsupportedEncodingException if URL decoding fails.
   */
  private static String safeDecode(final String stringToDecode)
      throws UnsupportedEncodingException {
    if (stringToDecode == null) {
      return null;
    }

    if (stringToDecode.length() == 0) {
      return "";
    }

    if (stringToDecode.contains(AbfsHttpConstants.PLUS)) {
      final StringBuilder outBuilder = new StringBuilder();

      int startDex = 0;
      for (int m = 0; m < stringToDecode.length(); m++) {
        if (stringToDecode.charAt(m) == '+') {
          if (m > startDex) {
            outBuilder.append(URLDecoder.decode(
                stringToDecode.substring(startDex, m),
                AbfsHttpConstants.UTF_8));
          }

          outBuilder.append(AbfsHttpConstants.PLUS);
          startDex = m + 1;
        }
      }

      if (startDex != stringToDecode.length()) {
        outBuilder.append(URLDecoder.decode(
            stringToDecode.substring(startDex, stringToDecode.length()),
            AbfsHttpConstants.UTF_8));
      }

      return outBuilder.toString();
    } else {
      return URLDecoder.decode(stringToDecode, AbfsHttpConstants.UTF_8);
    }
  }

  /**
   * Removes leading spaces from the specified string.
   *
   * @param value input string.
   * @return string without leading spaces.
   */
  private static String trimStart(final String value) {
    int spaceDex = 0;
    while (spaceDex < value.length() && value.charAt(spaceDex) == ' ') {
      spaceDex++;
    }
    return value.substring(spaceDex);
  }

  /**
   * Returns the value of the specified request header.
   *
   * @param conn HTTP operation.
   * @param headerName header name.
   * @param defaultValue value returned if the header is not present.
   * @return header value or the default value.
   */
  private static String getHeaderValue(final AbfsHttpOperation conn,
      final String headerName,
      final String defaultValue) {
    final String headerValue = conn.getRequestProperty(headerName);
    return headerValue == null ? defaultValue : headerValue;
  }

  /**
   * Returns the current GMT time formatted using the RFC1123 pattern.
   *
   * @return current GMT timestamp.
   */
  static String getGMTTime() {
    return getGMTTime(new Date());
  }

  /**
   * Returns the specified date formatted as an RFC1123 GMT timestamp.
   *
   * @param date date to format.
   * @return formatted GMT timestamp.
   */
  static String getGMTTime(final Date date) {
    return RFC1123_GMT_DATE_TIME_FORMATTER.get().format(date);
  }
}