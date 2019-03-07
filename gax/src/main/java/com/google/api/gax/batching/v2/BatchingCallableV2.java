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

import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingThreshold;
import com.google.api.gax.batching.PartitionKey;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.UnaryCallable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

/**
 * This class is to provide backward support to existing Batcher configuration.
 */
public class BatchingCallableV2<EntryT, ResultT, RequestT, ResponseT>
    extends UnaryCallable<RequestT, ResponseT> {

  private final List<BatchingThreshold<EntryT>> thresholds;
  private final Map<PartitionKey, Batcher<EntryT, ResultT>> batchers = new ConcurrentHashMap<>();
  private final BatchingDescriptorV2<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor;
  private final BatcherFactoryV2<EntryT, ResultT, RequestT, ResponseT> batcherFactory;
  private final Object lock = new Object();

  public BatchingCallableV2(
      BatchingDescriptorV2<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor,
      BatcherFactoryV2<EntryT, ResultT, RequestT, ResponseT> batcherFactory) {
    this.batchingDescriptor = batchingDescriptor;
    this.batcherFactory = batcherFactory;
    this.thresholds =
        new ArrayList<>(batcherFactory.getThresholds(batcherFactory.getBatchingSettings()));
  }

  @Override
  public ApiFuture<ResponseT> futureCall(RequestT request, ApiCallContext context) {
    Batcher<EntryT, ResultT> batcher = getBatcher(request);
    List<ApiFuture<ResultT>> results = new ArrayList<>();
    for(EntryT entry : batchingDescriptor.extractEntries(request)) {

      boolean anyThresholdReached = isAnyThresholdReached(entry);
      if(anyThresholdReached){
        batcher.flush();
        resetThresholds();
      }

      results.add(batcher.add(entry));
    }

    return mergeResults(results);
  }

  /**
   * Provides the Batcher corresponding to the partitionKey fetched from given request or
   * constructs one  if it doesn't exist yet. The implementation is thread-safe.
   */
  private Batcher<EntryT, ResultT> getBatcher(RequestT request) {
    PartitionKey partitionKey = batchingDescriptor.getPartitionKey(request);
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

  /** Merge the given List of results into a single response. */
  private ApiFuture<ResponseT> mergeResults(List<ApiFuture<ResultT>> results) {
    return ApiFutures
        .transform(ApiFutures.allAsList(results), new ApiFunction<List<ResultT>, ResponseT>() {
          @Override
          public ResponseT apply(List<ResultT> input) {
            return batchingDescriptor.mergeResults(input);
          }
        }, directExecutor());
  }

  private boolean isAnyThresholdReached(EntryT entry) {
    for (BatchingThreshold<EntryT> threshold : thresholds) {
      threshold.accumulate(entry);
      if (threshold.isThresholdReached()) {
        return true;
      }
    }
    return false;
  }

  private void resetThresholds() {
    for (int i = 0; i < thresholds.size(); i++) {
      thresholds.set(i, thresholds.get(i).copyWithZeroedValue());
    }
  }
}
