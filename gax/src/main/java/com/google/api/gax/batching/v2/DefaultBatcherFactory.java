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

import com.google.api.core.BetaApi;
import com.google.api.gax.batching.BatchingFlowController;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.BatchingThreshold;
import com.google.api.gax.batching.ElementCounter;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.batching.NumericThreshold;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * This class creates a fresh {@link Batcher}, which have fresh set of thresholds as List of {@link
 * NumericThreshold}.
 *
 * <p>This is public only for technical reasons, for advanced usage.
 */
@BetaApi("The surface for batching is not stable yet and may change in the future.")
public class DefaultBatcherFactory<EntryT, ResultT, RequestT, ResponseT>
    implements BatcherFactory<EntryT, ResultT> {
  private final ScheduledExecutorService executor;
  private final BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor;
  private final BatchingSettings batchingSettings;
  private final UnaryCallable<RequestT, ResponseT> callable;

  public DefaultBatcherFactory(
      BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor,
      ScheduledExecutorService executor,
      BatchingSettings batchingSettings,
      UnaryCallable<RequestT, ResponseT> callable) {
    this.batchingDescriptor = batchingDescriptor;
    this.batchingSettings = batchingSettings;
    this.executor = executor;
    this.callable = callable;
  }

  /** {@inheritDoc} */
  @Override
  public Batcher<EntryT, ResultT> createBatcher() {
    FlowController flowController = new FlowController(batchingSettings.getFlowControlSettings());
    BatchingFlowController<EntryT> batchingFlowController =
        new BatchingFlowController<>(
            flowController,
            new EntryCountThreshold<EntryT>(),
            new EntryByteThreshold<>(batchingDescriptor));

    return BatcherImpl.<EntryT, ResultT, RequestT, ResponseT>newBuilder()
        .setThresholds(getThresholds(batchingSettings))
        .setExecutor(executor)
        .setMaxDelay(batchingSettings.getDelayThreshold())
        .setBatchingDescriptor(batchingDescriptor)
        .setFlowController(batchingFlowController)
        .setUnaryCallable(callable)
        .build();
  }

  /**
   * Returns {@link List} of different thresholds based on values present in {@link
   * BatchingSettings}.
   *
   * <p>This is public only for technical reasons, for advanced usage.
   */
  private List<BatchingThreshold<EntryT>> getThresholds(BatchingSettings batchingSettings) {
    ImmutableList.Builder<BatchingThreshold<EntryT>> listBuilder = ImmutableList.builder();

    final Long elementCount = batchingSettings.getElementCountThreshold();
    if (elementCount != null) {
      ElementCounter<EntryT> elementCounter = new EntryCountThreshold<>();

      BatchingThreshold<EntryT> countThreshold =
          new NumericThreshold<>(elementCount, elementCounter);
      listBuilder.add(countThreshold);
    }

    final Long byteCount = batchingSettings.getRequestByteThreshold();
    if (byteCount != null) {
      ElementCounter<EntryT> requestByte = new EntryByteThreshold<>(batchingDescriptor);

      BatchingThreshold<EntryT> byteThreshold = new NumericThreshold<>(byteCount, requestByte);
      listBuilder.add(byteThreshold);
    }
    return listBuilder.build();
  }

  /**
   * As each Entry object will be considered one single element in the set, so it returns 1 for each
   * count.
   *
   * <p>This is public only for technical reasons, for advanced usage.
   */
  private static class EntryCountThreshold<EntryT> implements ElementCounter<EntryT> {
    @Override
    public long count(EntryT element) {
      return 1;
    }
  }

  /**
   * Calculates bytes of an entry object sent for batching. Implementation of counting bytes are
   * client dependent as it internally usage {@link BatchingDescriptor#countBytes(Object)}.
   *
   * <p>This is public only for technical reasons, for advanced usage.
   */
  private static class EntryByteThreshold<EntryT, ResultT, RequestT, ResponseT>
      implements ElementCounter<EntryT> {

    private final BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> batchingDesc;

    EntryByteThreshold(BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> batchingDesc) {
      this.batchingDesc = batchingDesc;
    }

    @Override
    public long count(EntryT element) {
      return batchingDesc.countBytes(element);
    }
  }
}
