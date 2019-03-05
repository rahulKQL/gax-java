package com.google.api.gax.batching.v2;

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

public class BatcherFactoryV2<EntryT, ResultT, RequestT, ResponseT> {

  //Keeping default count, bcoz if entry count raise to 2 then start the execution.
  private static final long MIN_ELEMENT_COUNT = 2;
  private static final long MIN_BYTE_COUNT = 2;

  private final ScheduledExecutorService executor;
  private final BatchingDescriptorV2<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor;
  private final FlowController flowController;
  private final BatchingSettings batchingSettings;
  private final UnaryCallable<RequestT, ResponseT> callable;

  public BatcherFactoryV2(
      BatchingDescriptorV2<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor,
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
            new EntryCountThreshold<EntryT>(),
            new EntryByteThreshold<>(batchingDescriptor)
        );

    return BatcherImpl.<EntryT, ResultT, RequestT, ResponseT>newBuilder()
        .setThresholds(getThresholds(batchingSettings))
        .setExecutor(executor)
        .setMaxDelay(batchingSettings.getDelayThreshold())
        .setBatchingDescriptor(batchingDescriptor)
        .setFlowController(batchingFlowController)
        .setUnaryCallable(callable)
        .build();
  }

  private List<BatchingThreshold<EntryT>> getThresholds(BatchingSettings batchingSettings) {
    ImmutableList.Builder<BatchingThreshold<EntryT>> listBuilder = ImmutableList.builder();

    final Long elementCount = batchingSettings.getElementCountThreshold();
    final Long byteCount = batchingSettings.getRequestByteThreshold();
    if (elementCount != null && elementCount > MIN_ELEMENT_COUNT) {
      ElementCounter<EntryT> elementCounter =
          new EntryCountThreshold<>();

      BatchingThreshold<EntryT> countThreshold =
          new NumericThreshold<>(elementCount, elementCounter);
      listBuilder.add(countThreshold);
    }

    if (byteCount != null && byteCount > MIN_BYTE_COUNT) {
      ElementCounter<EntryT> requestByte =
          new EntryByteThreshold<>(batchingDescriptor);

      BatchingThreshold<EntryT> byteThreshold = new NumericThreshold<>(byteCount, requestByte);
      listBuilder.add(byteThreshold);
    }
    return listBuilder.build();
  }

  private class EntryCountThreshold<EntryT> implements ElementCounter<EntryT> {
    @Override
    public long count(EntryT element) {
      return 1;
    }
  }

  private class EntryByteThreshold<EntryT, ResultT, RequestT, ResponseT> implements ElementCounter<EntryT>{

    private final BatchingDescriptorV2<EntryT, ResultT, RequestT, ResponseT> batchingDesc;
    EntryByteThreshold(BatchingDescriptorV2<EntryT, ResultT, RequestT, ResponseT> batchingDesc){
      this.batchingDesc = batchingDesc;
    }

    @Override
    public long count(EntryT element) {
      return batchingDesc.countByteEntry(element);
    }
  }
}
