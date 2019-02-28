package com.google.api.gax.batching;

import com.google.api.core.ApiFuture;
import javax.xml.ws.Response;

public interface IBatch<EntryT, ResultT> {

  ApiFuture<ResultT> add(EntryT entryT);

  void executeBatch();

  void cancel();
}
