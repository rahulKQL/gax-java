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
import com.google.api.gax.batching.BatchingSettings;
import com.google.common.truth.Truth;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class DefaultBatcherFactoryTest {

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
  public void testCreateFactory() throws Exception {
    // Setting long duration for DelayThreshold, so that it doesn't execute it before test finishes.
    BatchingSettings batchingSettings =
        BatchingSettings.newBuilder()
            .setDelayThreshold(Duration.ofSeconds(1000))
            .setElementCountThreshold(2L)
            .setRequestByteThreshold(1010L)
            .build();
    BatcherFactory<Integer, Integer> batcherFactory =
        new DefaultBatcherFactory<>(
            SQUARER_BATCHING_DESC_V2, batchingExecutor, batchingSettings, callLabeledIntSquarer);

    try (Batcher<Integer, Integer> batcher = batcherFactory.createBatcher()) {
      // Running batch with a single entry object.
      ApiFuture<Integer> result = batcher.add(10);
      batcher.flush();
      // Result should be completed now.
      Truth.assertThat(result.isDone()).isTrue();
      Truth.assertThat(result.get()).isEqualTo(100);
    }
  }
}
