/*
 * Copyright 2016 Google LLC
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
package com.google.api.gax.rpc;

import com.google.api.core.BetaApi;
import com.google.api.core.InternalExtensionOnly;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.retrying.RetrySettings;
import com.google.common.base.Preconditions;
import java.util.Set;

/**
 * A settings class to configure a {@link UnaryCallable} for calls to an API method that supports
 * batching. The settings are provided using an instance of {@link BatchingSettings}.
 */
@BetaApi("The surface for batching is not stable yet and may change in the future.")
@InternalExtensionOnly
public final class BatchingCallSettings<EntryT, ResultT, RequestT, ResponseT>
    extends UnaryCallSettings<RequestT, ResponseT> {
  private final BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor;
  private final BatchingSettings batchingSettings;
  private final FlowController flowController;

  public BatchingDescriptor<EntryT, ResultT,RequestT, ResponseT> getBatchingDescriptor() {
    return batchingDescriptor;
  }

  public BatchingSettings getBatchingSettings() {
    return batchingSettings;
  }

  public FlowController getFlowController() {
    return flowController;
  }

  private BatchingCallSettings(Builder<EntryT, ResultT, RequestT, ResponseT> builder) {
    super(builder);
    this.batchingDescriptor = builder.batchingDescriptor;
    this.batchingSettings = Preconditions.checkNotNull(builder.batchingSettings);
    FlowController flowControllerToUse = builder.flowController;
    if (flowControllerToUse == null) {
      flowControllerToUse = new FlowController(batchingSettings.getFlowControlSettings());
    }
    this.flowController = flowControllerToUse;
  }

  public static <EntryT, ResultT, RequestT, ResponseT> Builder<EntryT, ResultT, RequestT,
      ResponseT> newBuilder(
      BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor) {
    return new Builder<>(batchingDescriptor);
  }

  @Override
  public final Builder<EntryT, ResultT, RequestT, ResponseT> toBuilder() {
    return new Builder<>(this);
  }

  public static class Builder<EntryT, ResultT, RequestT, ResponseT>
      extends UnaryCallSettings.Builder<RequestT, ResponseT> {

    private BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor;
    private BatchingSettings batchingSettings;
    private FlowController flowController;

    public Builder(BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> batchingDescriptor) {
      this.batchingDescriptor = batchingDescriptor;
    }

    public Builder(BatchingCallSettings<EntryT, ResultT, RequestT, ResponseT> settings) {
      super(settings);
      this.batchingDescriptor = settings.batchingDescriptor;
      this.batchingSettings = settings.batchingSettings;
      // TODO decide if a copy should be made
      this.flowController = settings.flowController;
    }

    public BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> getBatchingDescriptor() {
      return batchingDescriptor;
    }

    public Builder<EntryT, ResultT, RequestT, ResponseT> setBatchingSettings(BatchingSettings batchingSettings) {
      this.batchingSettings = batchingSettings;
      return this;
    }

    public BatchingSettings getBatchingSettings() {
      return batchingSettings;
    }

    public Builder<EntryT, ResultT, RequestT, ResponseT> setFlowController(FlowController flowController) {
      this.flowController = flowController;
      return this;
    }

    public FlowController getFlowController() {
      return flowController;
    }

    @Override
    public Builder<EntryT, ResultT, RequestT, ResponseT> setRetryableCodes(Set<StatusCode.Code> retryableCodes) {
      super.setRetryableCodes(retryableCodes);
      return this;
    }

    @Override
    public Builder<EntryT, ResultT, RequestT, ResponseT> setRetryableCodes(StatusCode.Code... codes) {
      super.setRetryableCodes(codes);
      return this;
    }

    @Override
    public Builder<EntryT, ResultT, RequestT, ResponseT> setRetrySettings(RetrySettings retrySettings) {
      super.setRetrySettings(retrySettings);
      return this;
    }

    @Override
    public BatchingCallSettings<EntryT, ResultT, RequestT, ResponseT> build() {
      return new BatchingCallSettings<>(this);
    }
  }
}
