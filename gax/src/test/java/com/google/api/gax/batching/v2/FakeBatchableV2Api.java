/*
 * Copyright 2017 Google LLC
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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.InternalApi;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.batching.PartitionKey;
import com.google.api.gax.batching.RequestBuilder;
import com.google.api.gax.batching.v2.BatchingDescriptorV2;
import com.google.api.gax.batching.v2.RequestBuilderV2;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.BatchedRequestIssuer;
import com.google.api.gax.rpc.BatchingDescriptor;
import com.google.api.gax.rpc.UnaryCallable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@InternalApi("for testing")
public class FakeBatchableV2Api {

  public static class LabeledIntList {
    public String label;
    public List<Integer> ints;

    public LabeledIntList(String label, Integer... numbers) {
      this(label, new ArrayList<>(Arrays.asList(numbers)));
    }

    public LabeledIntList(String label, List<Integer> ints) {
      this.label = label;
      this.ints = ints;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      LabeledIntList that = (LabeledIntList) o;

      if (!label.equals(that.label)) {
        return false;
      }
      return ints.equals(that.ints);
    }

    @Override
    public int hashCode() {
      int result = label.hashCode();
      result = 31 * result + ints.hashCode();
      return result;
    }
  }

  public static LabeledIntSquarerCallable callLabeledIntSquarer = new LabeledIntSquarerCallable();

  public static class LabeledIntSquarerCallable
      extends UnaryCallable<LabeledIntList, List<Integer>> {

    @Override
    public ApiFuture<List<Integer>> futureCall(LabeledIntList request, ApiCallContext context) {
      List<Integer> result = new ArrayList<>();
      for (Integer i : request.ints) {
        result.add(i * i);
      }
      return ApiFutures.immediateFuture(result);
    }
  }

  public static SquarerBatchingDescriptorV2 SQUARER_BATCHING_DESC_V2 =
      new SquarerBatchingDescriptorV2();

  public static class SquarerBatchingDescriptorV2
      implements BatchingDescriptorV2<Integer, Integer, LabeledIntList, List<Integer>> {

    @Override
    public RequestBuilderV2<Integer, LabeledIntList> getRequestBuilder() {

      return new RequestBuilderV2<Integer, LabeledIntList>() {

        LabeledIntList list;

        @Override
        public void add(Integer request) {
          if (list == null) {
            list = new LabeledIntList("", request);
          } else {
            list.ints.add(request);
          }
        }

        @Override
        public LabeledIntList build() {
          return list;
        }
      };
    }

    @Override
    public RequestBuilderV2<Integer, LabeledIntList> newRequestBuilder(LabeledIntList base) {
      final LabeledIntList newReq = new LabeledIntList(base.label, base.ints);
      return new RequestBuilderV2<Integer, LabeledIntList>() {
        @Override
        public void add(Integer request) {
          newReq.ints.add(request);
        }

        @Override
        public LabeledIntList build() {
          return newReq;
        }
      };
    }

    @Override
    public void splitResponse(List<Integer> batchResponse,
        Collection<SettableApiFuture<Integer>> batch) {
      int index = 0;
      for(SettableApiFuture<Integer> response : batch){
        response.set(batchResponse.get(index));
        index++;
      }
    }

    @Override
    public void splitException(Throwable throwable, Collection<SettableApiFuture<Integer>> batch) {
      for (SettableApiFuture<Integer> responder : batch) {
        responder.setException(throwable);
      }
    }

    @Override
    public PartitionKey getPartitionKey(LabeledIntList request) {
      return new PartitionKey(request.label);
    }

    @Override
    public List<Integer> mergeResults(List<Integer> results) {
      return results;
    }

    @Override
    public List<Integer> extractEntries(LabeledIntList request) {
      return request.ints;
    }

    @Override
    public long countElements(LabeledIntList request) {
      return 0;
    }

    @Override
    public long countBytes(LabeledIntList request) {
      long counter = 0;
      for (Integer i : request.ints) {
        counter += i;
      }
      // Limit the byte size to simulate merged messages having smaller serialized size that the
      // sum of their components
      return Math.min(counter, 5);
    }

    @Override
    public long countByteEntry(Integer entry) {
      // Limit the byte size to simulate merged messages having smaller serialized size that the
      // sum of their components
      byte[] bytes = String.valueOf(entry).getBytes();
      return bytes.length;
    }
  }
}
