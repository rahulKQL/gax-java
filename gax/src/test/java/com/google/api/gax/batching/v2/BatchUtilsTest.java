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
import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.rpc.testing.FakeBatchableApi;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BatchUtilsTest {

  @Test
  public void testGetThresholds() {
    BatchingSettings settings =
        BatchingSettings.newBuilder()
            .setElementCountThreshold(2L)
            .setRequestByteThreshold(2L)
            .build();
    List<BatchingThreshold<Integer>> thresholds = createBatchUtil(settings).getThresholds();
    verifyThresholdLevel(thresholds, 2, 0);
    verifyThresholdLevel(thresholds, 2, 1);
  }

  @Test
  public void testGetThresholdsWithOneThreshold() {
    BatchingSettings settings =
        BatchingSettings.newBuilder()
            .setElementCountThreshold(2L)
            .setRequestByteThreshold(null)
            .build();
    List<BatchingThreshold<Integer>> thresholds = createBatchUtil(settings).getThresholds();
    assertThat(thresholds.size()).isEqualTo(1);
    verifyThresholdLevel(thresholds, 1, 0);

    settings =
        settings.toBuilder().setRequestByteThreshold(2L).setElementCountThreshold(null).build();
    thresholds = createBatchUtil(settings).getThresholds();
    verifyThresholdLevel(thresholds, 1, 0);
  }

  private void verifyThresholdLevel(
      List<BatchingThreshold<Integer>> thresholds, int size, int index) {
    assertThat(thresholds.size()).isEqualTo(size);
    BatchingThreshold<Integer> elementCountThreshold = thresholds.get(index);
    elementCountThreshold.accumulate(4);
    assertThat(elementCountThreshold.isThresholdReached()).isFalse();
    elementCountThreshold.accumulate(5);
    assertThat(elementCountThreshold.isThresholdReached()).isTrue();
  }

  private BatchUtils<Integer, Integer, FakeBatchableApi.LabeledIntList, List<Integer>>
      createBatchUtil(BatchingSettings batchingSettings) {
    return new BatchUtils<>(SQUARER_BATCHING_DESC_V2, batchingSettings);
  }
}
