package com.google.api.gax.batching.v2;

import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.PartitionKey;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.UnaryCallable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public class BatchingCallableV2<EntryT, ResultT, RequestT, ResponseT>
    extends UnaryCallable<RequestT, ResponseT> {

  private final UnaryCallable<RequestT, ResponseT> callable;
  private final Map<PartitionKey, Batcher<EntryT, ResultT>> batchers = new ConcurrentHashMap<>();
  private final BatchingDescriptorV2<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor;
  private final BatcherFactoryV2<EntryT, ResultT, RequestT, ResponseT> batcherFactory;
  private final Object lock = new Object();

  public BatchingCallableV2(
      UnaryCallable<RequestT, ResponseT> callable,
      BatchingDescriptorV2<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor,
      BatcherFactoryV2<EntryT, ResultT, RequestT, ResponseT> batcherFactory) {
    this.callable = callable;
    this.batchingDescriptor = batchingDescriptor;
    this.batcherFactory = batcherFactory;
  }

  @Override
  public ApiFuture<ResponseT> futureCall(RequestT request, ApiCallContext context) {
    PartitionKey key = batchingDescriptor.getPartitionKey(request);
    Batcher<EntryT, ResultT> batcher = getPartitionKey(key);
    List<ApiFuture<ResultT>> results = new ArrayList<>();
    for(EntryT entry : batchingDescriptor.extractEntries(request)) {
      System.out.println("Printing entry: " +  entry);
      results.add(batcher.add(entry));
    }

    return mergeResults(results);
  }

  private Batcher<EntryT, ResultT> getPartitionKey(PartitionKey partitionKey) {
    Batcher<EntryT, ResultT> batcher = batchers.get(partitionKey);
    if (batcher == null) {
      synchronized (lock) {
        batcher = batchers.get(partitionKey);
        if (batcher == null) {
          batcher = batcherFactory.createBatcher();
          batchers.put(partitionKey, batcher);
        }
      }
    }
    return batcher;
  }

  private ApiFuture<ResponseT> mergeResults(List<ApiFuture<ResultT>> results) {
    return ApiFutures
        .transform(ApiFutures.allAsList(results), new ApiFunction<List<ResultT>, ResponseT>() {
          @Override
          public ResponseT apply(List<ResultT> input) {
            return batchingDescriptor.mergeResults(input);
          }
        }, directExecutor());
  }
}
