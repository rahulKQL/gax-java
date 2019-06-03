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

import static com.google.api.gax.rpc.testing.FakeBatchableApi.SQUARER_BATCHING_DESC_V2;
import static com.google.api.gax.rpc.testing.FakeBatchableApi.callLabeledIntSquarer;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.api.gax.rpc.testing.FakeBatchableApi.LabeledIntList;
import com.google.api.gax.rpc.testing.FakeBatchableApi.SquarerBatchingDescriptorV2;
import com.google.common.truth.Truth;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class BatcherImplTest {

  private static final ScheduledExecutorService EXECUTOR =
      Executors.newSingleThreadScheduledExecutor();

  @Rule public MockitoRule rule = MockitoJUnit.rule();
  @Mock private UnaryCallable<LabeledIntList, List<Integer>> mockUnaryCallable;
  @Mock private BatchingDescriptor<Integer, Integer, LabeledIntList, List<Integer>> mockDescriptor;

  private Batcher<Integer, Integer> underTest;
  private LabeledIntList labeledIntList = new LabeledIntList("Default");
  private BatchingSettings batchingSettings =
      BatchingSettings.newBuilder()
          .setDelayThreshold(Duration.ofMinutes(1))
          .setRequestByteThreshold(1000L)
          .setElementCountThreshold(1000L)
          .build();

  @Before
  public void setUp() {}

  /** Tests accumulated element are resolved when {@link Batcher#flush()} is called. */
  @Test
  public void testResultsAreResolvedAfterFlush() throws Exception {
    underTest = createNewBatcherBuilder().build();
    Future<Integer> result = underTest.add(4);
    assertThat(result.isDone()).isFalse();
    underTest.flush();
    assertThat(result.isDone()).isTrue();
    assertThat(result.get()).isEqualTo(16);

    Future<Integer> anotherResult = underTest.add(5);
    assertThat(anotherResult.isDone()).isFalse();
  }

  /** Element results are resolved after batch is closed. */
  @Test
  public void testWhenBatcherIsClose() throws Exception {
    Future<Integer> result;
    try (Batcher<Integer, Integer> batcher = createNewBatcherBuilder().build()) {
      result = batcher.add(5);
    }
    assertThat(result.isDone()).isTrue();
    assertThat(result.get()).isEqualTo(25);
  }

  /** Validates exception when batch is called after {@link Batcher#close()}. */
  @Test
  public void testNoElementAdditionAfterClose() throws Exception {
    underTest = createNewBatcherBuilder().build();
    underTest.close();
    Throwable actualError = null;
    try {
      underTest.add(1);
    } catch (Exception ex) {
      actualError = ex;
    }
    assertThat(actualError).isInstanceOf(IllegalStateException.class);
    assertThat(actualError.getMessage()).matches("Cannot add elements on a closed batcher.");
  }

  /** Verifies unaryCallable is being called with a batch. */
  @Test
  public void testResultsAfterRPCSucceed() throws Exception {
    underTest = createNewBatcherBuilder().setUnaryCallable(mockUnaryCallable).build();
    when(mockUnaryCallable.futureCall(any(LabeledIntList.class)))
        .thenReturn(ApiFutures.immediateFuture(Arrays.asList(16, 25)));

    Future<Integer> result = underTest.add(4);
    Future<Integer> anotherResult = underTest.add(5);
    underTest.flush();

    assertThat(result.isDone()).isTrue();
    assertThat(result.get()).isEqualTo(16);
    assertThat(anotherResult.get()).isEqualTo(25);
    verify(mockUnaryCallable, times(1)).futureCall(any(LabeledIntList.class));
  }

  /** Verifies exception occurred at RPC is propagated to element results */
  @Test
  public void testResultFailureAfterRPCFailure() throws Exception {
    underTest = createNewBatcherBuilder().setUnaryCallable(mockUnaryCallable).build();
    final Exception fakeError = new RuntimeException();

    when(mockUnaryCallable.futureCall(any(LabeledIntList.class)))
        .thenReturn(ApiFutures.<List<Integer>>immediateFailedFuture(fakeError));

    Future<Integer> failedResult = underTest.add(5);
    underTest.flush();
    assertThat(failedResult.isDone()).isTrue();
    Throwable actualError = null;
    try {
      failedResult.get();
    } catch (InterruptedException | ExecutionException ex) {
      actualError = ex;
    }

    assertThat(actualError.getCause()).isSameAs(fakeError);
    verify(mockUnaryCallable, times(1)).futureCall(any(LabeledIntList.class));
  }

  /** Tests results are resolves when {@link BatchingDescriptor#splitResponse} throws exception. */
  @Test
  public void testExceptionDescriptorResultHandling() throws InterruptedException {
    underTest = createNewBatcherBuilder().setBatchingDescriptor(mockDescriptor).build();

    final RuntimeException fakeError = new RuntimeException("internal exception");
    when(mockDescriptor.newRequestBuilder(any(LabeledIntList.class)))
        .thenReturn(SQUARER_BATCHING_DESC_V2.newRequestBuilder(labeledIntList));
    doThrow(fakeError)
        .when(mockDescriptor)
        .splitResponse(Mockito.<Integer>anyList(), Mockito.<SettableApiFuture<Integer>>anyList());

    Future<Integer> result = underTest.add(2);
    underTest.flush();
    Throwable actualError = null;
    try {
      result.get();
    } catch (ExecutionException ex) {
      actualError = ex;
    }

    assertThat(actualError.getCause()).isSameAs(fakeError);
    verify(mockDescriptor)
        .splitResponse(Mockito.<Integer>anyList(), Mockito.<SettableApiFuture<Integer>>anyList());
  }

  /** Tests results are resolves when {@link BatchingDescriptor#splitException} throws exception. */
  @Test
  public void testExceptionInDescriptorErrorHandling() throws InterruptedException {
    underTest =
        createNewBatcherBuilder()
            .setBatchingDescriptor(mockDescriptor)
            .setUnaryCallable(mockUnaryCallable)
            .build();

    final RuntimeException fakeRpcError = new RuntimeException("RPC error");
    final RuntimeException fakeError = new RuntimeException("internal exception");
    when(mockUnaryCallable.futureCall(any(LabeledIntList.class)))
        .thenReturn(ApiFutures.<List<Integer>>immediateFailedFuture(fakeRpcError));
    when(mockDescriptor.newRequestBuilder(any(LabeledIntList.class)))
        .thenReturn(SQUARER_BATCHING_DESC_V2.newRequestBuilder(labeledIntList));
    doThrow(fakeError)
        .when(mockDescriptor)
        .splitException(any(Throwable.class), Mockito.<SettableApiFuture<Integer>>anyList());

    Future<Integer> result = underTest.add(2);
    underTest.flush();
    Throwable actualError = null;
    try {
      result.get();
    } catch (ExecutionException ex) {
      actualError = ex;
    }

    assertThat(actualError.getCause()).isSameAs(fakeError);
    verify(mockDescriptor)
        .splitException(any(Throwable.class), Mockito.<SettableApiFuture<Integer>>anyList());
  }

  /** Tests accumulated element are resolved when {@link Batcher#flush()} is called. */
  @Test
  public void testBatchingWithCallable() throws Exception {
    BatchingSettings batchSet =
        BatchingSettings.newBuilder()
            .setElementCountThreshold(101L)
            .setRequestByteThreshold(3000L)
            .setDelayThreshold(Duration.ofMillis(10000))
            .build();
    underTest = createNewBatcherBuilder().setBatchingSettings(batchSet).build();
    int limit = 100;
    int batch = 10;
    List<ApiFuture<Integer>> resultList = new ArrayList<>(limit);
    for (int i = 0; i <= limit; i++) {
      resultList.add(underTest.add(i));
      if (i % batch == 0) {
        underTest.flush();
        for (int j = i - batch; j >= 0 && j < i; j++) {
          Truth.assertThat(resultList.get(j).isDone()).isTrue();
          Truth.assertThat(resultList.get(j).get()).isEqualTo(j * j);
        }
      }
    }
  }

  @Test
  public void testBatchingWhenThresholdExceeds() throws ExecutionException, InterruptedException {
    BatchingSettings settings = batchingSettings.toBuilder().setElementCountThreshold(2L).build();
    underTest = createNewBatcherBuilder().setBatchingSettings(settings).build();
    Future result = underTest.add(2);
    assertThat(result.isDone()).isFalse();
    // After this element is added, Batch will be trigger sendBatch().
    Future anotherResult = underTest.add(3);
    // Both the element should be resolved now.
    assertThat(result.isDone()).isTrue();
    assertThat(result.get()).isEqualTo(4);
    assertThat(anotherResult.isDone()).isTrue();

    settings = batchingSettings.toBuilder().setRequestByteThreshold(2L).build();
    underTest = createNewBatcherBuilder().setBatchingSettings(settings).build();
    result = underTest.add(4);
    assertThat(result.isDone()).isFalse();
    // After this element is added, Batch will be trigger sendBatch().
    anotherResult = underTest.add(5);
    // Both the element should be resolved now.
    assertThat(result.isDone()).isTrue();
    assertThat(result.get()).isEqualTo(16);
    assertThat(anotherResult.isDone()).isTrue();

    underTest = createNewBatcherBuilder().setMaxDelay(Duration.ofMillis(200)).build();
    result = underTest.add(6);
    assertThat(result.isDone()).isFalse();
    // Give time for the delay to trigger and push the batch
    Thread.sleep(300);
    assertThat(result.isDone()).isTrue();
    assertThat(result.get()).isEqualTo(36);
  }

  @Test
  public void testSynchronousFlush() throws Exception {
    final long expectedTimeToSleep = 300L;
    BatchingDescriptor<Integer, Integer, LabeledIntList, List<Integer>> desc =
        new SquarerBatchingDescriptorV2() {
          @Override
          public void splitResponse(
              List<Integer> batchResponse, List<SettableApiFuture<Integer>> batch) {
            try {
              Thread.sleep(expectedTimeToSleep);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            super.splitResponse(batchResponse, batch);
          }
        };
    underTest =
        createNewBatcherBuilder()
            .setBatchingDescriptor(desc)
            .setMaxDelay(Duration.ofMillis(50))
            .build();
    long startTime = System.currentTimeMillis();
    Future result = underTest.add(2);
    // Give time for the delay to trigger and push the batch through executor.
    Thread.sleep(50);
    underTest.flush();
    long totalTime = System.currentTimeMillis() - startTime;
    assertThat(totalTime).isAtLeast(expectedTimeToSleep + 50);
    assertThat(result.isDone()).isTrue();
  }

  @Test
  public void testBatcherImplBuilderGetters() {
    BatcherImpl.Builder<Integer, Integer, LabeledIntList, List<Integer>> builder =
        createNewBatcherBuilder();
    assertThat(builder.getBatchingDescriptor()).isSameAs(SQUARER_BATCHING_DESC_V2);
    assertThat(builder.getBatchingSettings()).isSameAs(batchingSettings);
    Duration maxDelay = Duration.ofMillis(100);
    builder.setMaxDelay(maxDelay);
    BatchingSettings settings = batchingSettings.toBuilder().setDelayThreshold(maxDelay).build();
    assertThat(builder.getBatchingSettings()).isEqualTo(settings);
  }

  private BatcherImpl.Builder<Integer, Integer, LabeledIntList, List<Integer>>
      createNewBatcherBuilder() {
    return BatcherImpl.<Integer, Integer, LabeledIntList, List<Integer>>newBuilder()
        .setPrototype(labeledIntList)
        .setUnaryCallable(callLabeledIntSquarer)
        .setBatchingDescriptor(SQUARER_BATCHING_DESC_V2)
        .setExecutor(EXECUTOR)
        .setBatchingSettings(batchingSettings);
  }
}
