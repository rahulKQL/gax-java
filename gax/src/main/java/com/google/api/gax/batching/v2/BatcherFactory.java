package com.google.api.gax.batching.v2;

import com.google.api.core.InternalApi;
import com.google.api.gax.batching.BatchingFlowController;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.BatchingThreshold;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.batching.PartitionKey;
import com.google.common.collect.ImmutableList;
import java.util.concurrent.ScheduledExecutorService;

public class BatcherFactory<EntryT, ResultT, RequestT, ResponseT> {

  private final ScheduledExecutorService executor;
  private final BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor;
  private final BatchingFlowController<EntryT> flowController;
  private final BatchingSettings batchingSettings;

  public BatcherFactory(
      BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor,
      BatchingSettings batchingSettings,
      ScheduledExecutorService executor,
      FlowController flowController){
    this.batchingDescriptor = batchingDescriptor;
    this.batchingSettings = batchingSettings;
    this.executor = executor;

    //TODO(rahulkql): remove unnecessary element counts
    this.flowController = new BatchingFlowController<>(flowController, null, null);
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


  public Batcher<EntryT, ResultT> createBatcher(PartitionKey partitionKey) {
  //TODO(rahulkql): Not sure about FlowController
    //    BatchingFlowController<Batcher<EntryT, ResultT>> batchingFlowController =
//        new BatchingFlowController<>(
//            flowController,
//            new BatcherImpl.BatchElementCounter<>(batchingDescriptor),
//            new BatcherImpl.BatchByteCounter<EntryT, ResultT>());


    return BatcherImpl.<EntryT, ResultT, RequestT, ResponseT>newBuilder()
        .setThresholds(getThresholds(batchingSettings))
        .setExecutor(executor)
        .setMaxDelay(batchingSettings.getDelayThreshold())
        .setFlowController(flowController)
        .build();
  }


  private ImmutableList<BatchingThreshold<EntryT>> getThresholds(
      BatchingSettings batchingSettings) {
    ImmutableList.Builder<BatchingThreshold<EntryT>> listBuilder =
        ImmutableList.builder();

    //TODO(rahulkql): I believe we dont need to calculate byteCount or element count as we are
    // receiving one element at a time.
//    if (batchingSettings.getElementCountThreshold() != null) {
//      ElementCounter<EntryT> elementCounter = null; // HERE we need proper element count
//
//      BatchingThreshold<EntryT> countThreshold =
//          new NumericThreshold<>(batchingSettings.getElementCountThreshold(), elementCounter);
//      listBuilder.add(countThreshold);
//    }
//
//    if (batchingSettings.getRequestByteThreshold() != null) {
//      ElementCounter<EntryT> requestByteCounter = null; //Same HERE
//
//      BatchingThreshold<EntryT> byteThreshold =
//          new NumericThreshold<>(batchingSettings.getRequestByteThreshold(), requestByteCounter);
//      listBuilder.add(byteThreshold);
//    }

    return listBuilder.build();
  }

}

