package com.google.api.gax.batching.v2;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.batching.BatchingFlowController;
import com.google.api.gax.rpc.UnaryCallable;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

/**
 * Work as a BatcherHelper
 */
public class BatchAccumalator<EntryT, ResultT, RequestT, ResponseT> {

  private final UnaryCallable<RequestT, ResponseT> callable;
  private final List<SettableApiFuture<ResultT>> resultFutures;
  private final BatchingDescriptorV2<EntryT, ResultT, RequestT, ResponseT> descriptor;
  private BatchingFlowController<EntryT> flowController;

  private RequestBuilderV2<EntryT, RequestT> requestBuilder;
  private ApiFuture<ResponseT> responseFuture;

  public BatchAccumalator(UnaryCallable<RequestT, ResponseT> callable,
      BatchingDescriptorV2<EntryT, ResultT, RequestT, ResponseT> descriptor,
      BatchingFlowController<EntryT> flowController) {
    this.callable = callable;
    this.descriptor = descriptor;
    this.flowController = flowController;
    this.requestBuilder = descriptor.getRequestBuilder();
    this.resultFutures = new LinkedList<>();
  }

  public ApiFuture<ResultT> add(final EntryT entry) {
    SettableApiFuture<ResultT> resultFuture = SettableApiFuture.create();
    resultFutures.add(resultFuture);
    requestBuilder.add(entry);
    ApiFutures.addCallback(resultFuture, new ApiFutureCallback<ResultT>() {
      @Override
      public void onFailure(Throwable t) {
        flowController.release(entry);
      }

      @Override
      public void onSuccess(ResultT result) {
        flowController.release(entry);
      }
    }, directExecutor());

    return resultFuture;
  }

  public void executeBatch() {
    RequestT request = requestBuilder.build();
    if (request == null) {
      return;
    }
    responseFuture = callable.futureCall(request);
    ApiFutures.addCallback(responseFuture, new ApiFutureCallback<ResponseT>() {
      @Override
      public void onSuccess(ResponseT response) {
        descriptor.splitResponse(response, resultFutures);
      }

      @Override
      public void onFailure(Throwable throwable) {
        descriptor.splitException(throwable, resultFutures);
      }
    }, directExecutor());
  }

  public void cancel() {
    if (responseFuture != null) {
      // Cancelling the batch
      responseFuture.cancel(false);
    }
  }
}
