package com.google.api.gax.batching;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.BatchingDescriptor;
import com.google.api.gax.rpc.UnaryCallable;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public class BatcherHelper<EntryT, ResultT, RequestT, ResponseT> implements IBatch<EntryT, ResultT> {

  private final UnaryCallable<RequestT, ResponseT> callable;
  private final List<SettableApiFuture<ResultT>> resultFutures;
  private final BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> descriptor;
  private BatchingFlowController<EntryT> flowController;

  private RequestBuilder<EntryT, RequestT> requestBuilder;
  private ApiFuture<ResponseT> responseFuture;

  public BatcherHelper(UnaryCallable<RequestT, ResponseT> callable,
      BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> descriptor,
      BatchingFlowController<EntryT> flowController) {
    this.callable = callable;
    this.descriptor = descriptor;
    this.flowController = flowController;
    this.requestBuilder = descriptor.getRequestBuilder();
    this.resultFutures = new LinkedList<>();
  }

  @Override
  public ApiFuture<ResultT> add(final EntryT entry) {
    SettableApiFuture<ResultT> apiFuture = SettableApiFuture.create();
    resultFutures.add(apiFuture);
    requestBuilder.add(entry);
    ApiFutures.addCallback(apiFuture, new ApiFutureCallback<ResultT>() {
      @Override
      public void onFailure(Throwable t) {
        flowController.release(entry);
      }

      @Override
      public void onSuccess(ResultT result) {
        flowController.release(entry);
      }
    }, directExecutor());

    executeBatch();
    return apiFuture;
  }

  @Override
  public void executeBatch() {
    RequestT request = requestBuilder.build();
    if(request == null){
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

  //TODO: verify this.
  @Override
  public void cancel() {
    if(responseFuture != null){
      // Cancelling the batch
      responseFuture.cancel(false);
    }
  }
}
