package com.google.api.gax.batching.v2;

import com.google.api.gax.batching.BatchingThreshold;

public class ElementCountThreshold<E> implements BatchingThreshold<E> {
  @Override
  public void accumulate(E e) {
  }

  @Override
  public boolean isThresholdReached() {
    return false;
  }

  @Override
  public BatchingThreshold<E> copyWithZeroedValue() {
    return null;
  }
}
