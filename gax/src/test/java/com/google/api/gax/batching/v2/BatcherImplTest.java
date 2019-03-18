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

import static com.google.api.gax.batching.v2.FakeBatchableApiV2.SQUARER_BATCHING_DESC_V2;
import static com.google.api.gax.batching.v2.FakeBatchableApiV2.callLabeledIntSquarer;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.rpc.ClientContext;
import com.google.api.gax.rpc.testing.FakeCallContext;
import com.google.api.gax.rpc.testing.FakeCallableFactory;
import com.google.api.gax.rpc.testing.FakeChannel;
import com.google.api.gax.rpc.testing.FakeTransportChannel;
import com.google.common.truth.Truth;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class BatcherImplTest {

  private ScheduledExecutorService batchingExecutor;
  private ClientContext clientContext;

  @Before
  public void setUp() {
    batchingExecutor = new ScheduledThreadPoolExecutor(1);
    clientContext =
        ClientContext.newBuilder()
            .setExecutor(batchingExecutor)
            .setDefaultCallContext(FakeCallContext.createDefault())
            .setTransportChannel(FakeTransportChannel.create(new FakeChannel()))
            .build();
  }

  @After
  public void teardown() {
    batchingExecutor.shutdownNow();
  }

  /**
   * To test single entry object with new Batcher interface. It also verifies the functionality for
   * DelayThreshold.
   */
  @Test
  public void bachingWithDelayThreshold() throws Exception {
    long waitTime = 1L;
    BatchingSettings batchingSettings =
        BatchingSettings.newBuilder()
            .setDelayThreshold(Duration.ofSeconds(waitTime))
            .setRequestByteThreshold(null)
            .setElementCountThreshold(11L)
            .build();
    try (Batcher<Integer, Integer> batcher =
        FakeCallableFactory.createBatcher(
            callLabeledIntSquarer, SQUARER_BATCHING_DESC_V2, batchingSettings, clientContext)) {

      // Running batch with a single entry object.
      ApiFuture<Integer> singleResult = batcher.add(10);
      // Before waiting for 2 sec, result will not be completed.
      Truth.assertThat(singleResult.isDone()).isFalse();

      // Allowing scheduler to trigger the flushing.
      TimeUnit.SECONDS.sleep(2L);

      // result should be in completed state now.
      Truth.assertThat(singleResult.isDone()).isTrue();
      Truth.assertThat(singleResult.get()).isEqualTo(100);
    }
  }

  /** Tests ElementCounterThreshold */
  @Test
  public void bacherWithElementThreshold() throws Exception {
    BatchingSettings batchingSettings =
        BatchingSettings.newBuilder()
            .setDelayThreshold(Duration.ofSeconds(10))
            .setRequestByteThreshold(null)
            .setElementCountThreshold(5L)
            .build();
    try (Batcher<Integer, Integer> batcher =
        FakeCallableFactory.createBatcher(
            callLabeledIntSquarer, SQUARER_BATCHING_DESC_V2, batchingSettings, clientContext)) {

      List<ApiFuture<Integer>> results = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        results.add(batcher.add(i));
      }
      //As we have not crossed the element count threshold i.e. 5.
      ApiFuture<List<Integer>> unCompleteResult = ApiFutures.allAsList(results);
      Truth.assertThat(unCompleteResult.isDone()).isFalse();

      // After this addition to batch, element count threshold will be reached.
      results.add(batcher.add(10));
      ApiFuture<List<Integer>> completedResult = ApiFutures.allAsList(results);
      Truth.assertThat(completedResult.isDone()).isTrue();
      Truth.assertThat(completedResult.get()).isEqualTo(Arrays.asList(0, 1, 4, 9, 100));
    }
  }
}
