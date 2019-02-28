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

import com.google.api.core.InternalApi;
import com.google.api.gax.batching.Batcher;
import com.google.api.gax.batching.BatcherImpl;
import com.google.api.gax.batching.BatchingFlowController;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.BatchingThreshold;
import com.google.api.gax.batching.ElementCounter;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.batching.NumericThreshold;
import com.google.api.gax.batching.PartitionKey;
import com.google.api.gax.batching.ThresholdBatcher;
import com.google.api.gax.rpc.Batch.BatchByteCounter;
import com.google.api.gax.rpc.Batch.BatchElementCounter;
import com.google.api.gax.rpc.Batch.BatchMergerImpl;
import com.google.common.collect.ImmutableList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import javax.xml.transform.Result;

/**
 * A Factory class which, for each unique partitionKey, creates a trio including a ThresholdBatcher,
 * BatchExecutor, and ThresholdBatchingForwarder. The ThresholdBatchingForwarder pulls items from
 * the ThresholdBatcher and forwards them to the BatchExecutor for processing.
 *
 * <p>This is public only for technical reasons, for advanced usage.
 */
@InternalApi
public final class BatcherFactory<EntryT, ResultT, RequestT, ResponseT> {
  private final ScheduledExecutorService executor;
  private final BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor;
  private final FlowController flowController;
  private final BatchingSettings batchingSettings;
  private final UnaryCallable<RequestT, ResponseT> callable;
  private final Object lock = new Object();

  public BatcherFactory(
      BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor,
      BatchingSettings batchingSettings,
      ScheduledExecutorService executor,
      FlowController flowController,
      UnaryCallable<RequestT, ResponseT> callable) {
    this.batchingDescriptor = batchingDescriptor;
    this.batchingSettings = batchingSettings;
    this.executor = executor;
    this.flowController = flowController;
    this.callable = callable;
  }

  public Batcher<EntryT, ResultT> createBatcher() {
    BatchingFlowController<EntryT> batchingFlowController =
        new BatchingFlowController<>(flowController,
        new BatchElementCounter<>(batchingDescriptor),
        new BatchByteCounter<>(batchingDescriptor));
    return BatcherImpl.<EntryT, ResultT, RequestT, ResponseT>newBuilder()
        .setThresholds(getThresholds(batchingSettings))
        .setBatchingDescriptor(batchingDescriptor)
        .setFlowController(batchingFlowController)
        //TODO: change design?? so that we dont have to send Callable inside new Batcher.
        .setUnaryCallable(callable).build();
  }

  /**
   * Returns the BatchingSettings object that is associated with this factory.
   *
   * <p>This is public only for technical reasons, for advanced usage.
   */
  @InternalApi
  public BatchingSettings getBatchingSettings() {
    return batchingSettings;
  }

  /**
   *
   * <p>This is public only for technical reasons, for advanced usage.
   */
  @InternalApi
  public BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> getBatchingDescriptor(){
    return batchingDescriptor;
  }

  private ImmutableList<BatchingThreshold<EntryT>> getThresholds(BatchingSettings batchingSettings){
    ImmutableList.Builder<BatchingThreshold<EntryT>> listBuilder = ImmutableList.builder();

    if (batchingSettings.getElementCountThreshold() != null) {
      ElementCounter<EntryT> elementCounter =
          new BatchElementCounter<>(batchingDescriptor);

      BatchingThreshold<EntryT> countThreshold =
          new NumericThreshold<>(batchingSettings.getElementCountThreshold(), elementCounter);
      listBuilder.add(countThreshold);
    }

    if (batchingSettings.getRequestByteThreshold() != null) {
      ElementCounter<EntryT> requestByteCounter =
          new BatchByteCounter<>(batchingDescriptor);

      BatchingThreshold<EntryT> byteThreshold =
          new NumericThreshold<>(batchingSettings.getRequestByteThreshold(), requestByteCounter);
      listBuilder.add(byteThreshold);
    }

    return listBuilder.build();
  }

  static class BatchElementCounter<EntryT, ResultT, RequestT, ResponseT>
      implements ElementCounter<EntryT> {
    private final BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor;

    BatchElementCounter(BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor) {
      this.batchingDescriptor = batchingDescriptor;
    }

    @Override
    public long count(EntryT entry) {
      return batchingDescriptor.countElements(entry);
    }
  }

  //TODO: Is this correct??
  static class BatchByteCounter<EntryT, ResultT, RequestT, ResponseT> implements ElementCounter<EntryT> {
    private final BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor;

    BatchByteCounter(BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor) {
      this.batchingDescriptor = batchingDescriptor;
    }

    @Override
    public long count(EntryT element) {
      return batchingDescriptor.countBytes(element);
    }
  }
}
