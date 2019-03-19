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

import com.google.api.core.BetaApi;
import com.google.api.core.SettableApiFuture;
import java.util.Collection;

/**
 * Interface which represents an object that transforms entry/result OR request/response data for
 * the purposes of batching.
 *
 * <p>Implementations of BatchingDescriptorV2 must guarantee that all methods are stateless and
 * thread safe.
 *
 * <p>This class is designed to be used by generated code.
 */
@BetaApi("The surface for batching is not stable yet and may change in the future.")
public interface BatchingDescriptor<EntryT, ResultT, RequestT, ResponseT> {

  /** Get the Builder object for the request type RequestT. */
  RequestBuilder<EntryT, RequestT> getRequestBuilder();

  /**
   * Splits the result from a batched call into an individual setResponse call on each
   * RequestIssuer.
   */
  void splitResponse(ResponseT batchResponse, Collection<SettableApiFuture<ResultT>> batch);

  /**
   * Splits the exception that resulted from a batched call into an individual setException call on
   * each RequestIssuer.
   */
  void splitException(Throwable throwable, Collection<SettableApiFuture<ResultT>> batch);

  /** Returns total bytes of a single entry object * */
  long countBytes(EntryT entry);
}
