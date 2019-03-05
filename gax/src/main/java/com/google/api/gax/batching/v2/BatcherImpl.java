package com.google.api.gax.batching.v2;

import com.google.api.core.ApiFuture;
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

public class BatcherImpl<EntryT, ResultT, RequestT, ResponseT> implements Batcher<EntryT,
    ResultT> {

  private final ArrayList<BatchingThreshold<EntryT>> thresholds;
  private final ScheduledExecutorService executor;
  private final Duration maxDelay;
  private final UnaryCallable<RequestT, ResponseT> callable;
  private final BatchingFlowController<EntryT> flowController;
  private final BatchingDescriptorV2<EntryT, ResultT, RequestT, ResponseT> descriptor;

  private final ReentrantLock lock = new ReentrantLock();
  private BatchAccumalator<EntryT, ResultT, RequestT, ResponseT> currentOpenBatch;

  private final Runnable pushCurrentBatchRunnable =
      new Runnable() {
        @Override
        public void run() {
          flush();
        }
      };

  private BatcherImpl(Builder<EntryT, ResultT, RequestT, ResponseT> builder){
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
    private BatchingDescriptorV2<EntryT, ResultT, RequestT, ResponseT> descriptor;

    private Builder(){}

    public Builder<EntryT, ResultT, RequestT, ResponseT> setThresholds(
        List<BatchingThreshold<EntryT>> thresholds) {
      this.thresholds = thresholds;
      return this;
    }

    /** Set the executor for the ThresholdBatcher. */
    public Builder<EntryT, ResultT, RequestT, ResponseT> setExecutor(ScheduledExecutorService executor) {
      this.executor = executor;
      return this;
    }

    public Builder<EntryT, ResultT, RequestT, ResponseT> setUnaryCallable(UnaryCallable<RequestT, ResponseT> callable) {
      this.callable = callable;
      return this;
    }

    public Builder<EntryT, ResultT, RequestT, ResponseT> setFlowController(BatchingFlowController<EntryT> flowController) {
      this.flowController = flowController;
      return this;
    }

    /** Set the max delay for a batch. This is counted from the first item added to a batch. */
    public Builder<EntryT, ResultT, RequestT, ResponseT> setMaxDelay(Duration maxDelay) {
      this.maxDelay = maxDelay;
      return this;
    }

    public Builder<EntryT, ResultT, RequestT, ResponseT> setBatchingDescriptor(BatchingDescriptorV2<EntryT, ResultT, RequestT, ResponseT> descriptor) {
      this.descriptor = descriptor;
      return this;
    }

    /** Build the */
    public BatcherImpl<EntryT, ResultT, RequestT, ResponseT> build() {
      return new BatcherImpl<>(this);
    }
  }

  public static <EntryT, ResultT, RequestT, ResponseT> Builder<EntryT, ResultT, RequestT, ResponseT> newBuilder() {
    return new Builder<>();
  }

  @Override
  public ApiFuture<ResultT> add(EntryT entry) {
    lock.lock();
    ApiFuture<ResultT> response = null;
    try {
      flowController.reserve(entry);
      if (currentOpenBatch == null) {
        currentOpenBatch = createAccumalator();
        response = currentOpenBatch.add(entry);
      } else {
        response = currentOpenBatch.add(entry);
      }

      boolean anyThresholdReached = isAnyThresholdReached(entry);
      if (!anyThresholdReached) {
        executor.schedule(pushCurrentBatchRunnable, maxDelay.toMillis(), TimeUnit.MILLISECONDS);
      }

      if (anyThresholdReached) {
        flush();
      }
    } catch (FlowController.FlowControlException e) {
      throw BatchingException.fromFlowControlException(e);
    } finally {
      lock.unlock();
    }
    return response;
  }

  @Override
  public void flush() {
    lock.lock();
    try{
      if(currentOpenBatch != null){
        System.out.println("flush");
        currentOpenBatch.executeBatch();
        currentOpenBatch = null;

        resetThresholds();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() {
    //TODO: Can not shutdown executor as batching might not have completed yet.
    //TODO: As awaitTermination will block all other task so simply adding shutdown.
    if(!executor.isShutdown()){
      executor.shutdown();
    }
  }

  private BatchAccumalator<EntryT,ResultT, RequestT, ResponseT> createAccumalator() {
    return new BatchAccumalator<>(callable, descriptor, flowController);
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
