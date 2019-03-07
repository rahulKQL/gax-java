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

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.BatchingThreshold;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.common.truth.Truth;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

import static com.google.api.gax.batching.v2.FakeBatchableApiV2.callLabeledIntSquarer;

@RunWith(JUnit4.class)
public class BatcherFactoryV2Test {

  private ScheduledExecutorService batchingExecutor;

  @Before
  public void setUp() {
    batchingExecutor = new ScheduledThreadPoolExecutor(1);
  }

  @After
  public void tearDown() {
    batchingExecutor.shutdownNow();
  }

  @Test
  public void testBatcher() {
    BatchingSettings batchingSettings =
        BatchingSettings.newBuilder()
            .setDelayThreshold(Duration.ofSeconds(1))
            .setElementCountThreshold(2L)
            .setRequestByteThreshold(1010L)
            .build();
    FlowControlSettings flowControlSettings =
        FlowControlSettings.newBuilder()
            .setLimitExceededBehavior(FlowController.LimitExceededBehavior.Ignore)
            .build();
    FlowController flowController = new FlowController(flowControlSettings);
    BatcherFactoryV2 batcherFactory =
        new BatcherFactoryV2<>(
            new FakeBatchableApiV2.SquarerBatchingDescriptorV2(), batchingSettings, batchingExecutor
            , flowController, callLabeledIntSquarer);
    Truth.assertThat(batcherFactory.getBatchingSettings()).isSameAs(batchingSettings);
  }

  @Test
  public void testThresholdPresences() {
    BatchingSettings batchingSettings =
        BatchingSettings.newBuilder()
            .setDelayThreshold(Duration.ofSeconds(1))
            .setElementCountThreshold(null)
            .setRequestByteThreshold(null)
            .build();
    FlowControlSettings flowControlSettings =
        FlowControlSettings.newBuilder()
            .setLimitExceededBehavior(FlowController.LimitExceededBehavior.Ignore)
            .build();
    FlowController flowController = new FlowController(flowControlSettings);
    BatcherFactoryV2 batcherFactory =
        new BatcherFactoryV2<>(
            new FakeBatchableApiV2.SquarerBatchingDescriptorV2(), batchingSettings, batchingExecutor
            , flowController, callLabeledIntSquarer);

    // There should not be any thresholds in case of null in batchingSettings.
    List<BatchingThreshold<Integer>> emptyThreshold =
        batcherFactory.getThresholds(batchingSettings);
    Truth.assertThat(emptyThreshold).isEmpty();


    BatchingSettings elementThresholdSettings =
        BatchingSettings.newBuilder()
            .setDelayThreshold(Duration.ofSeconds(1))
            .setElementCountThreshold(1L)
            .setRequestByteThreshold(2L)
            .build();
    // Both of the threshold should be present
    List<BatchingThreshold<Integer>> elementCountThreshold =
        batcherFactory.getThresholds(elementThresholdSettings);
    Truth.assertThat(elementCountThreshold.size()).isEqualTo(2);
  }
}
