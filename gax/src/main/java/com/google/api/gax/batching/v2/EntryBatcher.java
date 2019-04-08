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

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.BetaApi;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.batching.BatchingFlowController;
import com.google.api.gax.batching.BatchingThreshold;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.threeten.bp.Duration;

/**
 * Queues up elements until either a duration of time has passed or any threshold in a given set of
 * thresholds is breached, then returned future gets completed.
 */
@BetaApi("The surface for batching is not stable yet and may change in the future.")
public class EntryBatcher<EntryT, EntryResultT, RequestT, ResponseT>
    implements Batcher<EntryT, EntryResultT> {

  private static final long DEFAULT_FINISH_WAIT_MILLIS = 250;

  private final ArrayList<BatchingThreshold<EntryT>> thresholds;
  private final ScheduledExecutorService executor;
  private final Duration maxDelay;
  private final BatchingFlowController<EntryT> flowController;
  private final BatchingDescriptor<EntryT, EntryResultT, RequestT, ResponseT> batchingDescriptor;
  private final UnaryCallable<RequestT, ResponseT> callable;
  private final RequestT prototype;

  private final Object lock = new Object();

  private Batch batch;
  private boolean isClosed = false;

  //  private List<ApiFuture<ResponseT>> responseFutures = new ArrayList<>();
  private AtomicInteger numOfRpcs = new AtomicInteger();
  private long failedEntires;

  private final Runnable currentBatchRunnable =
      new Runnable() {
        @Override
        public void run() {
          sendBatch();
        }
      };

  EntryBatcher(
      List<BatchingThreshold<EntryT>> thresholds,
      ScheduledExecutorService executor,
      Duration maxDelay,
      BatchingFlowController<EntryT> flowController,
      UnaryCallable<RequestT, ResponseT> callable,
      BatchingDescriptor<EntryT, EntryResultT, RequestT, ResponseT> batchingDescriptor,
      RequestT prototype) {
    this.thresholds = new ArrayList<>(thresholds);
    this.executor = Preconditions.checkNotNull(executor);
    this.maxDelay = Preconditions.checkNotNull(maxDelay);
    this.flowController = Preconditions.checkNotNull(flowController);
    this.batchingDescriptor = Preconditions.checkNotNull(batchingDescriptor);
    this.callable = Preconditions.checkNotNull(callable);
    this.prototype = Preconditions.checkNotNull(prototype);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized ApiFuture<EntryResultT> add(final EntryT entry) {
    Preconditions.checkState(!isClosed, "Cannot perform batching on a closed connection");
    try {
      flowController.reserve(entry);
      boolean anyThresholdReached = isAnyThresholdReached(entry);
      if (batch == null) {
        batch = new Batch(batchingDescriptor.newRequestBuilder(prototype));

        // Scheduling a job with maxDelay, after each entries assignment.
        if (!anyThresholdReached) {
          executor.schedule(currentBatchRunnable, maxDelay.toMillis(), TimeUnit.MILLISECONDS);
        }
      }

      SettableApiFuture<EntryResultT> result = SettableApiFuture.create();
      batch.add(entry, result);
      ApiFutures.addCallback(
          result,
          new ApiFutureCallback<EntryResultT>() {
            @Override
            public void onFailure(Throwable t) {
              flowController.release(entry);
              failedEntires++;
            }

            @Override
            public void onSuccess(EntryResultT result) {
              flowController.release(entry);
            }
          },
          directExecutor());

      if (anyThresholdReached) {
        sendBatch();
      }
      return result;
    } catch (FlowController.FlowControlException e) {
      throw new BatchingException(failedEntires, "Some Message", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void flush() throws InterruptedException {
    while (numOfRpcs.get() > 0) {
      synchronized (lock) {
        if (numOfRpcs.get() > 0) {
          lock.wait(DEFAULT_FINISH_WAIT_MILLIS);
        }
      }
    }
  }

  private synchronized void sendBatch() {
    try {
      if (batch == null) {
        return;
      }
      final Batch accumulatedBatch = batch;
      batch = null;

      // Per Batch incrementing the AtomicInteger.
      numOfRpcs.incrementAndGet();

      final ApiFuture<ResponseT> batchResponse =
          callable.futureCall(accumulatedBatch.builder.build());
      ApiFutures.addCallback(
          batchResponse,
          new ApiFutureCallback<ResponseT>() {
            @Override
            public void onSuccess(ResponseT response) {
              batchingDescriptor.splitResponse(response, accumulatedBatch.results);
              onCompletion();
            }

            @Override
            public void onFailure(Throwable throwable) {
              batchingDescriptor.splitException(throwable, accumulatedBatch.results);
              onCompletion();
            }
          },
          directExecutor());
      resetThresholds();
    } catch (Exception e) {
      throw new BatchingException(failedEntires, "some message", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws InterruptedException {
    isClosed = true;
    flush();
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

  private void onCompletion() {
    if (numOfRpcs.decrementAndGet() == 0) {
      synchronized (lock) {
        lock.notifyAll();
      }
    }
  }

  /**
   * This class represent one logical Batch which holds all the request accumulated and it's
   *
   * <p>corresponding future result.
   */
  class Batch {
    private final RequestBuilder<EntryT, RequestT> builder;
    private final List<SettableApiFuture<EntryResultT>> results;

    private Batch(RequestBuilder<EntryT, RequestT> builder) {
      this.builder = builder;
      this.results = new ArrayList<>();
    }

    void add(EntryT entry, SettableApiFuture<EntryResultT> result) {
      builder.add(entry);
      results.add(result);
    }
  }
}
