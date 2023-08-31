/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.util.functional.FunctionRaisingIOE;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_GET;
import static org.apache.hadoop.fs.azurebfs.services.AuthType.OAuth;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;

public final class AbfsClientTestUtil {

    private AbfsClientTestUtil() {

    }

    public static void addMockBehaviourToRestOpAndHttpOp(final AbfsRestOperation abfsRestOperation,
                                                         final AbfsHttpOperation httpOperation) throws IOException {
        HttpURLConnection httpURLConnection = Mockito.mock(HttpURLConnection.class);
        Mockito.doNothing().when(httpURLConnection)
                .setRequestProperty(nullable(String.class), nullable(String.class));
        Mockito.doReturn(httpURLConnection).when(httpOperation).getConnection();
        Mockito.doReturn("").when(abfsRestOperation).getClientLatency();
        Mockito.doReturn(httpOperation).when(abfsRestOperation).createHttpOperation();
    }

    public static void setMockAbfsRestOperationForListPathOperation(
            final AbfsClient spiedClient,
            FunctionRaisingIOE<AbfsHttpOperation, AbfsHttpOperation> functionRaisingIOE)
            throws Exception {
        ExponentialRetryPolicy retryPolicy = Mockito.mock(ExponentialRetryPolicy.class);
        AbfsHttpOperation httpOperation = Mockito.mock(AbfsHttpOperation.class);
        AbfsRestOperation abfsRestOperation = Mockito.spy(new AbfsRestOperation(
                AbfsRestOperationType.ListPaths,
                spiedClient,
                HTTP_METHOD_GET,
                null,
                new ArrayList<>()
        ));

        Mockito.doReturn(abfsRestOperation).when(spiedClient).listPath(any(), any(), any(), any(), any());

        addMockBehaviourToAbfsClient(spiedClient, retryPolicy);
        addMockBehaviourToRestOpAndHttpOp(abfsRestOperation, httpOperation);

        functionRaisingIOE.apply(httpOperation);
    }


    public static void addMockBehaviourToAbfsClient(final AbfsClient abfsClient,
                                                    final ExponentialRetryPolicy retryPolicy) throws IOException {
        Mockito.doReturn(OAuth).when(abfsClient).getAuthType();
        Mockito.doReturn("").when(abfsClient).getAccessToken();
        AbfsThrottlingIntercept intercept = Mockito.mock(
                AbfsThrottlingIntercept.class);
        Mockito.doReturn(intercept).when(abfsClient).getIntercept();
        Mockito.doNothing()
                .when(intercept)
                .sendingRequest(any(), nullable(AbfsCounters.class));
        Mockito.doNothing().when(intercept).updateMetrics(any(), any());

        Mockito.doReturn(retryPolicy).when(abfsClient).getRetryPolicy();
        Mockito.doReturn(true)
                .when(retryPolicy)
                .shouldRetry(nullable(Integer.class), nullable(Integer.class));
        Mockito.doReturn(false).when(retryPolicy).shouldRetry(0, HTTP_OK);
        Mockito.doReturn(false).when(retryPolicy).shouldRetry(1, HTTP_OK);
        Mockito.doReturn(false).when(retryPolicy).shouldRetry(2, HTTP_OK);
    }
}