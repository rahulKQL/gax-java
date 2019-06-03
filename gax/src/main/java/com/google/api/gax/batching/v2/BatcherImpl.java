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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.BetaApi;
import com.google.api.core.InternalExtensionOnly;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.threeten.bp.Duration;

/**
 * Queues up the elements until {@link #flush()} is called, once batching is finished returned
 * future gets resolves.
 *
 * <p>This class is not thread-safe, and expects to be used from a single thread.
 */
@BetaApi("The surface for batching is not stable yet and may change in the future.")
@InternalExtensionOnly("For google-cloud-java client use only.")
public class BatcherImpl<ElementT, ElementResultT, RequestT, ResponseT>
    implements Batcher<ElementT, ElementResultT> {

  private final Runnable pushCurrentBatchRunnable =
      new Runnable() {
        @Override
        public void run() {
          sendBatch();
        }
      };

  /** The amount of time to wait before checking responses are received or not. */
  private final BatchingDescriptor<ElementT, ElementResultT, RequestT, ResponseT>
      batchingDescriptor;

  private final UnaryCallable<RequestT, ResponseT> callable;
  private final RequestT prototype;
  private final List<BatchingThreshold<ElementT>> thresholds;
  private final ScheduledExecutorService executor;
  private final Duration maxDelay;

  private final AtomicInteger numOfRpcs = new AtomicInteger(0);
  private final AtomicBoolean isFlushed = new AtomicBoolean(false);
  private final Semaphore semaphore = new Semaphore(0);
  private Batch<ElementT, ElementResultT, RequestT> currentOpenBatch;
  private boolean isClosed = false;

  private BatcherImpl(Builder<ElementT, ElementResultT, RequestT, ResponseT> builder) {
    this.prototype = checkNotNull(builder.prototype, "RequestPrototype cannot be null.");
    this.callable = checkNotNull(builder.unaryCallable, "UnaryCallable cannot be null.");
    this.batchingDescriptor =
        checkNotNull(builder.batchingDescriptor, "BatchingDescriptor cannot be null.");
    this.executor = checkNotNull(builder.executor, "Executor service cannot be null.");
    BatchingSettings batchingSettings =
        checkNotNull(builder.batchingSettings, "Batching Setting cannot be null.");
    this.maxDelay = batchingSettings.getDelayThreshold();

    List<BatchingThreshold<ElementT>> immutableThresholds =
        new BatchUtils<>(batchingDescriptor, batchingSettings).getThresholds();
    this.thresholds = new ArrayList<>(immutableThresholds);
  }

  /** Builder for a BatcherImpl. */
  public static class Builder<ElementT, ElementResultT, RequestT, ResponseT> {
    private BatchingDescriptor<ElementT, ElementResultT, RequestT, ResponseT> batchingDescriptor;
    private UnaryCallable<RequestT, ResponseT> unaryCallable;
    private RequestT prototype;
    private ScheduledExecutorService executor;
    private BatchingSettings batchingSettings;

    private Builder() {}

    public Builder<ElementT, ElementResultT, RequestT, ResponseT> setExecutor(
        ScheduledExecutorService executor) {
      this.executor = executor;
      return this;
    }

    public Builder<ElementT, ElementResultT, RequestT, ResponseT> setMaxDelay(Duration maxDelay) {
      this.batchingSettings = batchingSettings.toBuilder().setDelayThreshold(maxDelay).build();
      return this;
    }

    public Builder<ElementT, ElementResultT, RequestT, ResponseT> setBatchingSettings(
        BatchingSettings batchingSettings) {
      this.batchingSettings = batchingSettings;
      return this;
    }

    public BatchingSettings getBatchingSettings() {
      return batchingSettings;
    }

    public Builder<ElementT, ElementResultT, RequestT, ResponseT> setBatchingDescriptor(
        BatchingDescriptor<ElementT, ElementResultT, RequestT, ResponseT> batchingDescriptor) {
      this.batchingDescriptor = batchingDescriptor;
      return this;
    }

    public BatchingDescriptor<ElementT, ElementResultT, RequestT, ResponseT>
        getBatchingDescriptor() {
      return batchingDescriptor;
    }

    public Builder<ElementT, ElementResultT, RequestT, ResponseT> setUnaryCallable(
        UnaryCallable<RequestT, ResponseT> unaryCallable) {
      this.unaryCallable = unaryCallable;
      return this;
    }

    public Builder<ElementT, ElementResultT, RequestT, ResponseT> setPrototype(RequestT prototype) {
      this.prototype = prototype;
      return this;
    }

    public BatcherImpl<ElementT, ElementResultT, RequestT, ResponseT> build() {
      return new BatcherImpl<>(this);
    }
  }

  public static <EntryT, EntryResultT, RequestT, ResponseT>
      Builder<EntryT, EntryResultT, RequestT, ResponseT> newBuilder() {
    return new Builder<>();
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<ElementResultT> add(ElementT element) {
    Preconditions.checkState(!isClosed, "Cannot add elements on a closed batcher.");

    boolean anyThresholdReached = isAnyThresholdReached(element);

    if (currentOpenBatch == null) {
      currentOpenBatch = new Batch<>(batchingDescriptor.newRequestBuilder(prototype));
    }

    SettableApiFuture<ElementResultT> result = SettableApiFuture.create();
    currentOpenBatch.add(element, result);

    if (anyThresholdReached) {
      sendBatch();
    } else {
      executor.schedule(pushCurrentBatchRunnable, maxDelay.toMillis(), TimeUnit.MILLISECONDS);
    }

    return result;
  }

  /** {@inheritDoc} */
  @Override
  public void flush() throws InterruptedException {
    sendBatch();
    isFlushed.compareAndSet(false, true);
    if (numOfRpcs.get() > 0) {
      semaphore.acquire();
    }
  }

  /** Sends accumulated elements asynchronously for batching. */
  private void sendBatch() {
    if (currentOpenBatch == null) {
      return;
    }
    final Batch<ElementT, ElementResultT, RequestT> accumulatedBatch = currentOpenBatch;
    currentOpenBatch = null;
    numOfRpcs.incrementAndGet();

    final ApiFuture<ResponseT> batchResponse =
        callable.futureCall(accumulatedBatch.builder.build());

    ApiFutures.addCallback(
        batchResponse,
        new ApiFutureCallback<ResponseT>() {
          @Override
          public void onSuccess(ResponseT response) {
            try {
              batchingDescriptor.splitResponse(response, accumulatedBatch.results);
            } catch (Throwable ex) {
              for (SettableApiFuture<ElementResultT> result : accumulatedBatch.results) {
                result.setException(ex);
              }
            } finally {
              onCompletion();
            }
          }

          @Override
          public void onFailure(Throwable throwable) {
            try {
              batchingDescriptor.splitException(throwable, accumulatedBatch.results);
            } catch (Throwable ex) {
              for (SettableApiFuture<ElementResultT> result : accumulatedBatch.results) {
                result.setException(ex);
              }
            } finally {
              onCompletion();
            }
          }
        },
        directExecutor());

    resetThresholds();
  }

  private void onCompletion() {
    if (numOfRpcs.decrementAndGet() == 0 && isFlushed.get()) {
      semaphore.release();
      isFlushed.compareAndSet(true, false);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws InterruptedException {
    isClosed = true;
    flush();
  }

  private boolean isAnyThresholdReached(ElementT element) {
    for (BatchingThreshold<ElementT> threshold : thresholds) {
      threshold.accumulate(element);
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

  /**
   * This class represent one logical Batch. It accumulates all the elements and it's corresponding
   * future element results for one batch.
   */
  private static class Batch<ElementT, ElementResultT, RequestT> {
    private final RequestBuilder<ElementT, RequestT> builder;
    private final List<SettableApiFuture<ElementResultT>> results;

    private Batch(RequestBuilder<ElementT, RequestT> builder) {
      this.builder = builder;
      this.results = new ArrayList<>();
    }

    void add(ElementT element, SettableApiFuture<ElementResultT> result) {
      builder.add(element);
      results.add(result);
    }
  }
}
