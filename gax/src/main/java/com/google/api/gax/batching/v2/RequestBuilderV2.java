package com.google.api.gax.batching.v2;

public interface RequestBuilderV2<EntryT, RequestT> {

  void add(EntryT request);

  RequestT build();
}
