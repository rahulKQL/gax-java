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
package com.google.api.gax.batching;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.common.base.MoreObjects;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * Keeps the statistics about failed operations(both at RPC and ElementT) in {@link Batcher}. This
 * provides the count of individual exception failure and count of each failed {@link Code} occurred
 * in the batching process.
 */
class BatcherStats {

  private final Map<Class, Integer> requestExceptionCounts = new HashMap<>();
  private final Map<Code, Integer> requestStatusCounts = new HashMap<>();
  private final Map<Class, Integer> entryExceptionCounts = new HashMap<>();
  private final Map<Code, Integer> entryStatusCounts = new HashMap<>();
  private final Object lock = new Object();
  private int partialBatchFailures;

  /**
   * Records the count of the exception and it's type when complete batch is failed to apply.
   *
   * <p>Note: This method aggregates all the subclasses of {@link ApiException} under ApiException
   * using the {@link Code status codes} and its number of occurrences.
   */
  void recordBatchFailure(Throwable throwable) {
    Class exceptionClass = throwable.getClass();

    synchronized (lock) {
      if (throwable instanceof ApiException) {
        Code code = ((ApiException) throwable).getStatusCode().getCode();
        exceptionClass = ApiException.class;

        int oldCount = MoreObjects.firstNonNull(requestStatusCounts.get(code), 0);
        requestStatusCounts.put(code, oldCount + 1);
      }

      int oldExCount = MoreObjects.firstNonNull(requestExceptionCounts.get(exceptionClass), 0);
      requestExceptionCounts.put(exceptionClass, oldExCount + 1);
    }
  }

  /**
   * Records partial failure occurred within per batch. For any exception within a batch, the {@link
   * #partialBatchFailures} is incremented once. It also keeps the records of the count and type of
   * each entry failure as well.
   *
   * <p>Note: This method aggregates all the subclasses of {@link ApiException} under ApiException
   * using the {@link Code status codes} and its number of occurrences.
   */
  <T extends ApiFuture> void recordBatchElementsCompletion(List<T> batchElementResultFutures) {
    final AtomicBoolean elementResultFailed = new AtomicBoolean();
    for (final ApiFuture elementResult : batchElementResultFutures) {

      ApiFutures.addCallback(
          elementResult,
          new ApiFutureCallback() {

            @Override
            public void onFailure(Throwable throwable) {
              synchronized (lock) {
                if (elementResultFailed.compareAndSet(false, true)) {
                  partialBatchFailures++;
                }

                Class exceptionClass = throwable.getClass();

                if (throwable instanceof ApiException) {
                  Code code = ((ApiException) throwable).getStatusCode().getCode();
                  exceptionClass = ApiException.class;

                  int statusOldCount = MoreObjects.firstNonNull(entryStatusCounts.get(code), 0);
                  entryStatusCounts.put(code, statusOldCount + 1);
                }

                int oldCount =
                    MoreObjects.firstNonNull(entryExceptionCounts.get(exceptionClass), 0);
                entryExceptionCounts.put(exceptionClass, oldCount + 1);
              }
            }

            @Override
            public void onSuccess(Object result) {}
          },
          directExecutor());
    }
  }

  /** Calculates and formats the message with request and entry failure count. */
  @Nullable
  BatchingException asException() {
    synchronized (lock) {
      if (requestExceptionCounts.isEmpty() && partialBatchFailures == 0) {
        return null;
      }

      StringBuilder sb = new StringBuilder();
      sb.append("Batching finished with ");

      if (!requestExceptionCounts.isEmpty()) {
        sb.append(
                String.format("%d batches failed to apply due to: ", requestExceptionCounts.size()))
            .append(printKeyValue(requestExceptionCounts, requestStatusCounts))
            .append(" and ");
      }

      sb.append(String.format("%d partial failures.", partialBatchFailures));
      if (partialBatchFailures > 0) {
        int totalEntriesCount = 0;
        for (Integer count : entryExceptionCounts.values()) {
          totalEntriesCount += count;
        }

        sb.append(
                String.format(
                    " The %d partial failures contained %d entries that failed with: ",
                    partialBatchFailures, totalEntriesCount))
            .append(printKeyValue(entryExceptionCounts, entryStatusCounts))
            .append(".");
      }
      return new BatchingException(sb.toString());
    }
  }

  /**
   * Prints the class name and it's count along with {@link Code status code} and it's counts.
   *
   * <p>Example: "1 IllegalStateException, 1 ApiException(1 UNAVAILABLE, 1 ALREADY_EXISTS)".
   */
  private String printKeyValue(
      Map<Class, Integer> exceptionCounts, Map<Code, Integer> statusCounts) {
    StringBuilder keyValue = new StringBuilder();
    Iterator<Map.Entry<Class, Integer>> iterator = exceptionCounts.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<Class, Integer> request = iterator.next();
      keyValue.append(String.format("%d %s", request.getValue(), request.getKey().getSimpleName()));

      if (ApiException.class.equals(request.getKey())) {
        keyValue.append("(");
        Iterator<Map.Entry<Code, Integer>> statusItrator = statusCounts.entrySet().iterator();
        while (statusItrator.hasNext()) {
          Map.Entry<Code, Integer> statusCode = statusItrator.next();
          keyValue.append(String.format("%d %s", statusCode.getValue(), statusCode.getKey()));
          if (statusItrator.hasNext()) {
            keyValue.append(", ");
          }
        }
        keyValue.append(")");
      }
      if (iterator.hasNext()) {
        keyValue.append(", ");
      }
    }

    return keyValue.toString();
  }
}
