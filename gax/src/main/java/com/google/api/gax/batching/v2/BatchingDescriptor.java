package com.google.api.gax.batching.v2;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.PartitionKey;
import java.util.List;

public interface BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> {

  /** Returns the value of the partition key for the given request. */
  @Deprecated
  PartitionKey getBatchPartitionKey(RequestT request);

  /** Get the Builder object for the request type RequestT. */
  RequestBuilder<EntryT, RequestT> newRequestBuilder(RequestT base);

  /**
   * Splits the result from a batched call into an individual setResponse call on each
   * RequestIssuer.
   */
  void splitResponse(ResponseT response, List<ApiFuture<ResultT>> results);

  /**
   * Splits the exception that resulted from a batched call into an individual setException call on
   * each RequestIssuer.
   */
  void splitException(Throwable throwable, List<ApiFuture<ResultT>> results);

  @Deprecated
  ResponseT mergeResults(List<ResultT> results);

  @Deprecated
  List<EntryT> extractEntries(RequestT request);

  /** Returns the number of elements contained in this request. */
  long countElements(RequestT request);

  /** Returns the size in bytes of this request. */
  long countBytes(RequestT request);
}
