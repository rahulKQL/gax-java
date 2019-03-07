/*
 * Copyright 2019 Google LLC
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google LLC nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.google.api.gax.batching.v2;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.InternalApi;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.batching.BatchingFlowController;
import com.google.api.gax.rpc.UnaryCallable;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

/**
 * Accumulates entries in {@link RequestBuilderV2} and returns a future to the client. these
 * future are resolved once #executeBatch has been triggered.
 */
@InternalApi
public class BatchAccumalator<EntryT, ResultT, RequestT, ResponseT> {

  private final UnaryCallable<RequestT, ResponseT> callable;
  private final List<SettableApiFuture<ResultT>> collectedResults;
  private final BatchingDescriptorV2<EntryT, ResultT, RequestT, ResponseT> descriptor;
  private final BatchingFlowController<EntryT> flowController;

  private RequestBuilderV2<EntryT, RequestT> requestBuilder;
  private ApiFuture<ResponseT> responseFuture;

  public BatchAccumalator(UnaryCallable<RequestT, ResponseT> callable,
      BatchingDescriptorV2<EntryT, ResultT, RequestT, ResponseT> descriptor,
      BatchingFlowController<EntryT> flowController) {
    this.callable = callable;
    this.descriptor = descriptor;
    this.flowController = flowController;
    this.requestBuilder = descriptor.getRequestBuilder();
    this.collectedResults = new LinkedList<>();
  }

  /**
   * Creates a future, and collects the entry object which is sent for batching.
   */
  public ApiFuture<ResultT> add(final EntryT entry) {
    SettableApiFuture<ResultT> resultFuture = SettableApiFuture.create();
    collectedResults.add(resultFuture);
    requestBuilder.add(entry);
    ApiFutures.addCallback(resultFuture, new ApiFutureCallback<ResultT>() {
      @Override
      public void onFailure(Throwable t) {
        flowController.release(entry);
      }

      @Override
      public void onSuccess(ResultT result) {
        flowController.release(entry);
      }
    }, directExecutor());

    return resultFuture;
  }

  /**
   * Executes the collected request in a batch using UnaryCallable. The response will be split
   * according the the state of the batch result.
   */
  public void executeBatch() {
    RequestT request = requestBuilder.build();
    if (request == null) {
      return;
    }
    responseFuture = callable.futureCall(request);
    ApiFutures.addCallback(responseFuture, new ApiFutureCallback<ResponseT>() {
      @Override
      public void onSuccess(ResponseT response) {
        descriptor.splitResponse(response, collectedResults);
      }

      @Override
      public void onFailure(Throwable throwable) {
        descriptor.splitException(throwable, collectedResults);
      }
    }, directExecutor());
  }
}
