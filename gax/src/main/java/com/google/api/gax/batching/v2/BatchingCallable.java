package com.google.api.gax.batching.v2;

import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.PartitionKey;
import com.google.api.gax.batching.ThresholdBatcher;
import com.google.api.gax.rpc.ApiCallContext;

import com.google.api.gax.rpc.Batch;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BatchingCallable<EntryT, ResultT, RequestT, ResponseT> extends UnaryCallable<RequestT, ResponseT> {
  private final Map<PartitionKey, Batcher<EntryT, ResultT>> batchers =  new ConcurrentHashMap<>();
  private final BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> descriptor;
  private final BatcherFactory<EntryT, ResultT, RequestT, ResponseT>  batcherFactory;

  public BatchingCallable(
      BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> descriptor,
      BatcherFactory<EntryT, ResultT, RequestT, ResponseT> batcherFactory) {
    this.descriptor = Preconditions.checkNotNull(descriptor);
    this.batcherFactory = Preconditions.checkNotNull(batcherFactory);
  }

  @Override
  public ApiFuture<ResponseT> futureCall(RequestT request, ApiCallContext context) {
    if (batcherFactory.getBatchingSettings().getIsEnabled()) {
      PartitionKey key = descriptor.getBatchPartitionKey(request);

      Batcher<EntryT, ResultT> batcher = getPushingBatcher(key);
      List<ApiFuture<ResultT>> results = new ArrayList<>();
      for (EntryT entry : descriptor.extractEntries(request)) {
        results.add(batcher.add(entry));
      }
      return mergeResults(results);
    } else {

      //TODO(rahulkql): Identify what will be the out if batch is disabled?
      return null;
    }
  }

  private ApiFuture<ResponseT> mergeResults(List<ApiFuture<ResultT>> resultFutures){
      return ApiFutures.transform(ApiFutures.allAsList(resultFutures), new ApiFunction<List<ResultT>, ResponseT>() {
        @Override
        public ResponseT apply(List<ResultT> input) {
          return null;
        }
      }, MoreExecutors.directExecutor());
  }

  /**
   * Provides the ThresholdBatcher corresponding to the given partitionKey, or constructs one if it
   * doesn't exist yet. The implementation is thread-safe.
   */
  public Batcher<EntryT, ResultT> getPushingBatcher(PartitionKey partitionKey) {
    Batcher<EntryT, ResultT> batcher = batchers.get(partitionKey);
    if (batcher == null) {
        batcher = batchers.get(partitionKey);
        if (batcher == null) {
          batcher = batcherFactory.createBatcher(partitionKey);
          batchers.put(partitionKey, batcher);
        }
      }
    return batcher;
  }
} 
