package com.google.api.gax.batching.v2;

import com.google.common.annotations.VisibleForTesting;

public class BatchingException extends RuntimeException {
  private final long failedEntires;

  @VisibleForTesting
  BatchingException(long failedEntires, String message, Throwable cause){
    super(message, cause);
    this.failedEntires = failedEntires;
  }

  public long getFailedEntries(){
    return failedEntires;
  }
}
