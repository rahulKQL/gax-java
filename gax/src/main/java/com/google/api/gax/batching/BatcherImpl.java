package com.google.api.gax.batching;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.FlowController.BatchingException;
import com.google.api.gax.rpc.BatchingDescriptor;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class BatcherImpl<EntryT, ResultT, RequestT, ResponseT> implements Batcher<EntryT,
    ResultT> {
  private final ArrayList<BatchingThreshold<EntryT>> thresholds;
  private final UnaryCallable<RequestT, ResponseT> callable;
  private final BatchingFlowController<EntryT> flowController;
  private final BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> descriptor;

  private IBatch<EntryT, ResultT> currentOpenBatch;

  private BatcherImpl(Builder<EntryT, ResultT, RequestT, ResponseT> builder){
    this.thresholds = new ArrayList<>(builder.thresholds);
    this.callable = Preconditions.checkNotNull(builder.callable);
    this.flowController = Preconditions.checkNotNull(builder.flowController);
    this.descriptor = Preconditions.checkNotNull(builder.descriptor);

    resetThresholds();
  }

  public static class Builder<EntryT, ResultT, RequestT, ResponseT> {
    private Collection<BatchingThreshold<EntryT>> thresholds;
    private UnaryCallable<RequestT, ResponseT> callable;
    private BatchingFlowController<EntryT> flowController;
    private BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> descriptor;

    private Builder(){}

    public Builder<EntryT, ResultT, RequestT, ResponseT> setThresholds(
        List<BatchingThreshold<EntryT>> thresholds) {
      this.thresholds = thresholds;
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

    public Builder<EntryT, ResultT, RequestT, ResponseT> setBatchingDescriptor(BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> descriptor) {
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
    try {
      flowController.reserve(entry);
      if(currentOpenBatch == null){
        currentOpenBatch = createBatch();
      }
      boolean anyThresholdReached = isAnyThresholdReached(entry);
      if(anyThresholdReached){
        currentOpenBatch.executeBatch();
        resetThresholds();
      }

      return currentOpenBatch.add(entry);
    } catch (FlowController.FlowControlException e) {
      throw BatchingException.fromFlowControlException(e);
    }
  }

  private IBatch<EntryT,ResultT> createBatch() {
    return new BatcherHelper<>(callable, descriptor, flowController);
  }

  @Override
  public void flush() {
    if(currentOpenBatch != null){
      currentOpenBatch.executeBatch();
      resetThresholds();
    }
  }

  @Override
  public void close() {
    currentOpenBatch.executeBatch();
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
