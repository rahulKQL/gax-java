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
import com.google.api.gax.batching.FlowController.BatchingException;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.threeten.bp.Duration;

/**
 * Queues up elements until either a duration of time has passed or any threshold in a given set of
 * thresholds is breached, then returned future gets completed.
 */
@BetaApi("The surface for batching is not stable yet and may change in the future.")
public class BatcherImpl<EntryT, ResultT, RequestT, ResponseT> implements Batcher<EntryT, ResultT> {

  private final ArrayList<BatchingThreshold<EntryT>> thresholds;
  private final ScheduledExecutorService executor;
  private final Duration maxDelay;
  private final UnaryCallable<RequestT, ResponseT> callable;
  private final BatchingFlowController<EntryT> flowController;
  private final BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> descriptor;

  private final ReentrantLock lock = new ReentrantLock();
  private RequestBuilder<EntryT, RequestT> entries;
  private List<SettableApiFuture<ResultT>> resultFutures;
  private List<ApiFuture<ResponseT>> responses = new ArrayList<>();
  private boolean isShutDown = false;

  private final Runnable flushCurrentBatchRunnable =
      new Runnable() {
        @Override
        public void run() {
          flush();
        }
      };

  private BatcherImpl(Builder<EntryT, ResultT, RequestT, ResponseT> builder) {
    this.thresholds = new ArrayList<>(builder.thresholds);
    this.executor = Preconditions.checkNotNull(builder.executor);
    this.maxDelay = Preconditions.checkNotNull(builder.maxDelay);
    this.callable = Preconditions.checkNotNull(builder.callable);
    this.flowController = Preconditions.checkNotNull(builder.flowController);
    this.descriptor = Preconditions.checkNotNull(builder.descriptor);
  }

  public static class Builder<EntryT, ResultT, RequestT, ResponseT> {
    private Collection<BatchingThreshold<EntryT>> thresholds;
    private ScheduledExecutorService executor;
    private UnaryCallable<RequestT, ResponseT> callable;
    private BatchingFlowController<EntryT> flowController;
    private Duration maxDelay;
    private BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> descriptor;

    private Builder() {}

    public Builder<EntryT, ResultT, RequestT, ResponseT> setThresholds(
        List<BatchingThreshold<EntryT>> thresholds) {
      this.thresholds = thresholds;
      return this;
    }

    /** Set the executor for the ThresholdBatcher. */
    public Builder<EntryT, ResultT, RequestT, ResponseT> setExecutor(
        ScheduledExecutorService executor) {
      this.executor = executor;
      return this;
    }

    public Builder<EntryT, ResultT, RequestT, ResponseT> setUnaryCallable(
        UnaryCallable<RequestT, ResponseT> callable) {
      this.callable = callable;
      return this;
    }

    public Builder<EntryT, ResultT, RequestT, ResponseT> setFlowController(
        BatchingFlowController<EntryT> flowController) {
      this.flowController = flowController;
      return this;
    }

    /** Set the max delay for a batch. This is counted from the first item added to a batch. */
    public Builder<EntryT, ResultT, RequestT, ResponseT> setMaxDelay(Duration maxDelay) {
      this.maxDelay = maxDelay;
      return this;
    }

    public Builder<EntryT, ResultT, RequestT, ResponseT> setBatchingDescriptor(
        BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> descriptor) {
      this.descriptor = descriptor;
      return this;
    }

    /** Build the BatcherImpl */
    public BatcherImpl<EntryT, ResultT, RequestT, ResponseT> build() {
      return new BatcherImpl<>(this);
    }
  }

  public static <EntryT, ResultT, RequestT, ResponseT>
      Builder<EntryT, ResultT, RequestT, ResponseT> newBuilder() {
    return new Builder<>();
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<ResultT> add(final EntryT entry) {
    Preconditions.checkState(!isShutDown, "Cannot perform batching on a closed connection");
    lock.lock();
    try {
      flowController.reserve(entry);

      boolean anyThresholdReached = isAnyThresholdReached(entry);
      if (entries == null) {
        entries = descriptor.getRequestBuilder();
        resultFutures = new ArrayList<>();

        // Scheduling a job with maxDelay, after each entries assignment.
        if (!anyThresholdReached) {
          executor.schedule(flushCurrentBatchRunnable, maxDelay.toMillis(), TimeUnit.MILLISECONDS);
        }
      }
      entries.add(entry);
      SettableApiFuture<ResultT> result = SettableApiFuture.create();
      resultFutures.add(result);
      ApiFutures.addCallback(
          result,
          new ApiFutureCallback<ResultT>() {
            @Override
            public void onFailure(Throwable t) {
              flowController.release(entry);
            }

            @Override
            public void onSuccess(ResultT result) {
              flowController.release(entry);
            }
          },
          directExecutor());

      if (anyThresholdReached) {
        flush();
      }

      return result;
    } catch (FlowController.FlowControlException e) {
      throw BatchingException.fromFlowControlException(e);
    } finally {
      lock.unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void flush() {
    lock.lock();
    try {
      if (entries != null) {
        final RequestT request = entries.build();
        entries = null;

        final List<SettableApiFuture<ResultT>> results = resultFutures;
        resultFutures = null;

        final ApiFuture<ResponseT> responseFuture = callable.futureCall(request);
        responses.add(responseFuture);
        ApiFutures.addCallback(
            responseFuture,
            new ApiFutureCallback<ResponseT>() {
              @Override
              public void onSuccess(ResponseT response) {
                responses.remove(responseFuture);
                descriptor.splitResponse(response, results);
              }

              @Override
              public void onFailure(Throwable throwable) {
                responses.remove(responseFuture);
                descriptor.splitException(throwable, results);
              }
            },
            directExecutor());
        resetThresholds();
      }
    } finally {
      lock.unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    shutdown();
  }

  @Override
  public void shutdown() {
    isShutDown = true;
    flush();
    if (!responses.isEmpty()) {
      executor.shutdown();
      try {
        executor.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException ex) {
        throw BatchingException.fromException(ex);
      }
    }
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
