/*
 * Copyright 2019 Google LLC
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google LLC nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.google.api.gax.batching.v2;

import com.google.api.core.InternalApi;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

@InternalApi("Public for technical reasons only")
class BatchUtils<ElementT, ElementResultT, RequestT, ResponseT> {

  private final BatchingDescriptor<ElementT, ElementResultT, RequestT, ResponseT>
      batchingDescriptor;
  private final BatchingSettings batchingSettings;

  @VisibleForTesting
  BatchUtils(
      BatchingDescriptor<ElementT, ElementResultT, RequestT, ResponseT> batchingDescriptor,
      BatchingSettings batchingSettings) {
    this.batchingDescriptor = batchingDescriptor;
    this.batchingSettings = batchingSettings;
  }

  ImmutableList<BatchingThreshold<ElementT>> getThresholds() {
    ImmutableList.Builder<BatchingThreshold<ElementT>> listBuilder = ImmutableList.builder();

    if (batchingSettings.getElementCountThreshold() != null) {
      ElementCounter<ElementT> elementCounter =
          new ElementCounter<ElementT>() {
            @Override
            public long count(ElementT element) {
              return 1;
            }
          };

      BatchingThreshold<ElementT> countThreshold =
          new NumericThreshold<>(batchingSettings.getElementCountThreshold(), elementCounter);
      listBuilder.add(countThreshold);
    }

    if (batchingSettings.getRequestByteThreshold() != null) {
      ElementCounter<ElementT> requestByteCounter =
          new ElementCounter<ElementT>() {
            @Override
            public long count(ElementT element) {
              return batchingDescriptor.countBytes(element);
            }
          };

      BatchingThreshold<ElementT> byteThreshold =
          new NumericThreshold<>(batchingSettings.getRequestByteThreshold(), requestByteCounter);
      listBuilder.add(byteThreshold);
    }

    return listBuilder.build();
  }
}
