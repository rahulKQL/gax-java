package com.google.api.gax.batching.v2;

import com.google.api.core.ApiFuture;

public interface Batcher<EntryT, ResultT> extends AutoCloseable{

  ApiFuture<ResultT> add(EntryT entry);

  void flush();

  void close();
}
