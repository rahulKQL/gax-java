package com.google.api.gax.batching;

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.BatchingCallSettings;
import com.google.api.gax.rpc.ClientContext;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.api.gax.rpc.testing.FakeBatchableApi;
import com.google.api.gax.rpc.testing.FakeCallContext;
import com.google.api.gax.rpc.testing.FakeCallableFactory;
import com.google.api.gax.rpc.testing.FakeChannel;
import com.google.api.gax.rpc.testing.FakeTransportChannel;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

import static com.google.api.gax.rpc.testing.FakeBatchableApi.SQUARER_BATCHING_DESC;
import static com.google.api.gax.rpc.testing.FakeBatchableApi.callLabeledIntSquarer;

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


  @Test
  public void batchingFirst() throws Exception {
    BatchingSettings batchingSettings =
        BatchingSettings.newBuilder()
            .setDelayThreshold(Duration.ofSeconds(1))
            .setRequestByteThreshold(50L)
            .setElementCountThreshold(30L)
            .build();
    BatchingCallSettings<Integer, Integer, FakeBatchableApi.LabeledIntList, List<Integer>>
        batchingCallSettings =
        BatchingCallSettings.newBuilder(SQUARER_BATCHING_DESC)
            .setBatchingSettings(batchingSettings)
            .build();
    Batcher<Integer, Integer> batcher =
        FakeCallableFactory.createrBatcher(
            callLabeledIntSquarer, batchingCallSettings, clientContext);

    ApiFuture<Integer> value = batcher.add(5);
    batcher.flush();
    System.out.println(value.get());
  }
}
