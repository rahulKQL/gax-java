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
  private final List<ApiFuture<ResultT>> resultFutures;
  private final BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> descriptor;
  private final RequestBuilder<EntryT, RequestT> requestBuilder;
  private BatchingFlowController<EntryT> flowController;

  private ApiFuture<ResponseT> responseFuture;

  public BatcherHelper(UnaryCallable<RequestT, ResponseT> callable,
      RequestBuilder<EntryT, RequestT> requestBuilder,
      BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> descriptor,
      BatchingFlowController<EntryT> flowController) {
    this.callable = callable;
    this.requestBuilder = requestBuilder;
    this.descriptor = descriptor;
    this.flowController = flowController;
    this.resultFutures = new LinkedList<>();
  }


  @Override
  public ApiFuture<ResultT> add(EntryT entry) {
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

    return apiFuture;
  }

  @Override
  public void executeBatch() {
    responseFuture = callable.futureCall(requestBuilder.build());
    if(responseFuture == null){
      return;
    }
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
