package com.google.api.gax.batching.v2;

public interface RequestBuilder<EntryT, RequestT>{

  RequestT build();

  void add(EntryT entryT);
}
