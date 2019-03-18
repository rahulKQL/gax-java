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
import com.google.api.gax.batching.BatchingFlowController;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.common.truth.Truth;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Junit for {@link BatchAccumalator}. */
@RunWith(JUnit4.class)
public class BatchAccumalatorTest {

  @Test
  public void addAndExecuteBatch() throws Exception {
    FlowControlSettings flowControlSettings =
        FlowControlSettings.newBuilder()
            .setLimitExceededBehavior(FlowController.LimitExceededBehavior.Ignore)
            .build();
    BatchingFlowController<Integer> batchingFlowController =
        new BatchingFlowController<>(
            new FlowController(flowControlSettings),
            new FakeBatchableApiV2.RequestCounter<Integer>(),
            new FakeBatchableApiV2.RequestByteCounter<>(SQUARER_BATCHING_DESC_V2));

    BatchAccumalator<Integer, Integer, FakeBatchableApiV2.LabeledIntList, List<Integer>>
        batchAccumalator =
            new BatchAccumalator<>(
                callLabeledIntSquarer, SQUARER_BATCHING_DESC_V2, batchingFlowController);

    //Testing add operation for single entry
    ApiFuture<Integer> result_one = batchAccumalator.add(5);
    ApiFuture<Integer> result_two = batchAccumalator.add(10);

    //Future should not be resolved yet.
    Truth.assertThat(result_one.isDone()).isFalse();
    Truth.assertThat(result_two.isDone()).isFalse();

    //Triggering the execution of current batch.
    batchAccumalator.executeBatch();

    //Both the result should be completed by this time.
    Truth.assertThat(result_one.isDone()).isTrue();
    Truth.assertThat(result_two.isDone()).isTrue();

    //Fetching the expected result.
    Truth.assertThat(result_one.get()).isSameAs(25);
    Truth.assertThat(result_two.get()).isSameAs(100);
  }

  @Test
  public void testWhenNoRequestAccumulated() throws Exception {
    FlowControlSettings flowControlSettings =
        FlowControlSettings.newBuilder()
            .setLimitExceededBehavior(FlowController.LimitExceededBehavior.Ignore)
            .build();
    BatchingFlowController<Integer> batchingFlowController =
        new BatchingFlowController<>(
            new FlowController(flowControlSettings),
            new FakeBatchableApiV2.RequestCounter<Integer>(),
            new FakeBatchableApiV2.RequestByteCounter<>(SQUARER_BATCHING_DESC_V2));

    BatchAccumalator<Integer, Integer, FakeBatchableApiV2.LabeledIntList, List<Integer>>
        batchAccumalator =
            new BatchAccumalator<>(
                callLabeledIntSquarer, SQUARER_BATCHING_DESC_V2, batchingFlowController);

    //Testing add operation for single entry
    ApiFuture<Integer> result = batchAccumalator.add(4);
    //Executing all queued up entries
    batchAccumalator.executeBatch();
    // Checking the result
    Truth.assertThat(result.get()).isEqualTo(16);
    try {
      // This should not throw any exception.
      batchAccumalator.executeBatch();
    } catch (Exception e) {
      Assert.fail("Should not have thrown any exception");
    }
  }
}
