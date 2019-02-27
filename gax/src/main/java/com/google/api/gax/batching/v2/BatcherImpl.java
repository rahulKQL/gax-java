package com.google.api.gax.batching.v2;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.batching.BatchMerger;
import com.google.api.gax.batching.BatchingFlowController;
import com.google.api.gax.batching.BatchingThreshold;
import com.google.api.gax.batching.ThresholdBatcher;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.threeten.bp.Duration;


public final class BatcherImpl<EntryT, ResultT, RequestT, ResponseT> implements Batcher<EntryT,
    ResultT> {

  private final List<BatchingThreshold<EntryT>> thresholds;
  private final ScheduledExecutorService executor;
  private final UnaryCallable<RequestT, ResponseT> callable;
  private final Duration maxDelay;
  private final BatchingFlowController<EntryT> flowController;
  private RequestBuilder<EntryT, RequestT> requestBuilder;
  private List<ApiFuture<ResultT>> resultFutures;
  private final BatchMerger<EntryT> batchMerger;

  private List<EntryT> currentOpenBatch;
  private Future<?> currentAlarmFuture;

  private BatcherImpl(Builder<EntryT, ResultT, RequestT, ResponseT>  builder){
    this.thresholds = new ArrayList<>(builder.thresholds);
    this.executor = Preconditions.checkNotNull(builder.executor);
    this.maxDelay = Preconditions.checkNotNull(builder.maxDelay);
    // I dont think we should expect this from User.
    this.flowController = Preconditions.checkNotNull(builder.flowController);
    this.requestBuilder = Preconditions.checkNotNull(builder.requestBuilder);
    this.batchMerger = Preconditions.checkNotNull(builder.batchMerger);

    this.callable = null;

    resetThresholds();
  }

  public static class Builder<EntryT, ResultT, RequestT, ResponseT> {
    private Collection<BatchingThreshold<EntryT>> thresholds;
    private ScheduledExecutorService executor;
    private Duration maxDelay;
    private BatchingFlowController<EntryT> flowController;
    private RequestBuilder<EntryT, RequestT> requestBuilder;
    private BatchMerger<EntryT> batchMerger;

    private Builder(){}

    public Builder<EntryT, ResultT, RequestT, ResponseT> setThresholds(List<BatchingThreshold<EntryT>> thresholds) {
      this.thresholds = thresholds;
      return this;
    }

    public Builder<EntryT, ResultT, RequestT, ResponseT> setExecutor(ScheduledExecutorService executor) {
      this.executor = executor;
      return this;
    }

    /** Set the max delay for a batch. This is counted from the first item added to a batch. */
    public Builder<EntryT, ResultT, RequestT, ResponseT> setMaxDelay(Duration maxDelay) {
      this.maxDelay = maxDelay;
      return this;
    }

    public Builder<EntryT, ResultT, RequestT, ResponseT> setFlowController(BatchingFlowController<EntryT> flowController) {
      this.flowController = flowController;
      return this;
    }

    /** Set the batch merger for the ThresholdBatcher. */
    public Builder<EntryT, ResultT, RequestT, ResponseT> setBatchMerger(BatchMerger<EntryT> batchMerger) {
      this.batchMerger = batchMerger;
      return this;
    }

    /** Build the ThresholdBatcher. */
    public BatcherImpl<EntryT, ResultT, RequestT, ResponseT> build() {
      return new BatcherImpl<>(this);
    }
  }

  public static <EntryT, ResultT, RequestT, ResponseT> Builder<EntryT, ResultT, RequestT, ResponseT> newBuilder() {
    return new Builder<>();
  }

  //TODO(rahulkql): Everything is in progress.. would get clear picture once all classes connects.
  @Override
  public ApiFuture<ResultT> add(EntryT entry) {
    flowController.release(entry);

      boolean anyThresholdReached = isAnyThresholdReached(entry);
      if(currentOpenBatch == null){
        currentOpenBatch.add(entry);
        if (!anyThresholdReached) {
          currentAlarmFuture =
              executor.schedule(
                  pushCurrentBatchRunnable, maxDelay.toMillis(), TimeUnit.MILLISECONDS);
        }
      } else {
        //TODO(rahulkql): would this be enough to merge with current running batch?
        currentOpenBatch.add(entry);
      }
      if (anyThresholdReached) {
       flush();
      }
    //TODO: implement way in BatchingCallable to get EntryT ===> ResultT
    //TODO: create a new thread which processes EntryT object and returns ApiFuture<ResultT>
    //only that we can be send response to user and would process bunch of tasks in a batch.
    return null;
  }

  @Override
  public void flush() {
    //TODO: process everything which is present in CurrentOpenBatch.
    final SettableApiFuture<Void> retFuture = SettableApiFuture.create();

  }

  @Override
  public void close() {

  }

  private boolean isAnyThresholdReached(EntryT e) {
    for (BatchingThreshold<EntryT> threshold : thresholds) {
      threshold.accumulate(e);
      if (threshold.isThresholdReached()) {
        return true;
      }
    }
    return false;
  }

  private List<EntryT> removeBatch() {
    List<EntryT> batch = currentOpenBatch;
    currentOpenBatch = null;
    if (currentAlarmFuture != null) {
      currentAlarmFuture.cancel(false);
      currentAlarmFuture = null;
    }
    resetThresholds();
    return batch;
  }

  private void resetThresholds() {
    for (int i = 0; i < thresholds.size(); i++) {
      thresholds.set(i, thresholds.get(i).copyWithZeroedValue());
    }
  }

  private final Runnable pushCurrentBatchRunnable =
      new Runnable() {
        @Override
        public void run() {
          flush();
        }
      };
}
