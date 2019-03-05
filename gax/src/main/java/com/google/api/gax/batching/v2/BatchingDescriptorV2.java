package com.google.api.gax.batching.v2;

import com.google.api.core.SettableApiFuture;
import com.google.api.gax.batching.PartitionKey;
import java.util.Collection;
import java.util.List;

public interface BatchingDescriptorV2 <EntryT, ResultT, RequestT, ResponseT> {

  /** Get the Builder object for the request type RequestT. */
  RequestBuilderV2<EntryT, RequestT> getRequestBuilder();

  /** Get the Builder object for the request type RequestT. */
  RequestBuilderV2<EntryT, RequestT> newRequestBuilder(RequestT base);

  /**
   * Splits the result from a batched call into an individual setResponse call on each
   * RequestIssuer.
   */
  void splitResponse(ResponseT batchResponse, Collection<SettableApiFuture<ResultT>> batch);

  /**
   * Splits the exception that resulted from a batched call into an individual setException call on
   * each RequestIssuer.
   */
  void splitException(Throwable throwable, Collection<SettableApiFuture<ResultT>> batch);

  @Deprecated
  PartitionKey getPartitionKey(RequestT request);

  @Deprecated
  ResponseT mergeResults(List<ResultT> results);

  @Deprecated
  List<EntryT> extractEntries(RequestT request);

  /** Returns the number of elements contained in this request. */
  long countElements(RequestT request);

  /** Returns the size in bytes of this request. */
  long countBytes(RequestT request);

  /** Returns Counts bytes of a single entry object **/
  long countByteEntry(EntryT entry);
}
