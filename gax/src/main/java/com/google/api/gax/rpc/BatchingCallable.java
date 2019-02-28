/*
 * Copyright 2016 Google LLC
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
package com.google.api.gax.rpc;

import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.InternalApi;
import com.google.api.gax.batching.Batcher;
import com.google.api.gax.batching.PartitionKey;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

/**
 * A {@link UnaryCallable} which will batch requests based on the given BatchingDescriptor and
 * BatcherFactory. The BatcherFactory provides a distinct Batcher for each partition as specified by
 * the BatchingDescriptor. An example of a batching partition would be a pubsub topic.
 *
 * <p>This is public only for technical reasons, for advanced usage.
 */
@InternalApi("For use by transport-specific implementations")
public class BatchingCallable<EntryT, ResultT, RequestT, ResponseT> extends UnaryCallable<RequestT,
    ResponseT> {
  private final UnaryCallable<RequestT, ResponseT> callable;
  private final Map<PartitionKey, Batcher<EntryT, ResultT>> batchers = new ConcurrentHashMap<>();
  private final BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor;
  private final BatcherFactory<EntryT, ResultT, RequestT, ResponseT> batcherFactory;
  private final Object lock = new Object();

  public BatchingCallable(
      UnaryCallable<RequestT, ResponseT> callable,
      BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor,
      BatcherFactory<EntryT, ResultT, RequestT, ResponseT> batcherFactory) {
    this.callable = Preconditions.checkNotNull(callable);
    this.batchingDescriptor = Preconditions.checkNotNull(batchingDescriptor);
    this.batcherFactory = Preconditions.checkNotNull(batcherFactory);
  }

  @Override
  public ApiFuture<ResponseT> futureCall(RequestT request, ApiCallContext context) {
    PartitionKey key = batchingDescriptor.getPartitionKey(request);
    Batcher<EntryT, ResultT> batcher = getPartitionKey(key);
    List<ApiFuture<ResultT>> results = new ArrayList<>();
    for(EntryT entry : batchingDescriptor.extractEntries(request)) {
      results.add(batcher.add(entry));
    }

    return mergeResults(results);
  }

  private Batcher<EntryT, ResultT> getPartitionKey(PartitionKey partitionKey) {
    Batcher<EntryT, ResultT> batcher = batchers.get(partitionKey);
    if (batcher == null) {
      synchronized (lock) {
        batcher = batchers.get(partitionKey);
        if (batcher == null) {
          batcher = batcherFactory.createBatcher();
          batchers.put(partitionKey, batcher);
        }
      }
    }
    return batcher;
  }

  private ApiFuture<ResponseT> mergeResults(List<ApiFuture<ResultT>> results) {
    return ApiFutures
        .transform(ApiFutures.allAsList(results), new ApiFunction<List<ResultT>, ResponseT>() {
          @Override
          public ResponseT apply(List<ResultT> input) {
            return batchingDescriptor.mergeResults(input);
          }
        }, directExecutor());
  }
}
