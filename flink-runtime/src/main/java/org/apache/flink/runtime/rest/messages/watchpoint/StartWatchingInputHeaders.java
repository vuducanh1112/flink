/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages.watchpoint;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationTriggerMessageHeaders;
import org.apache.flink.runtime.rest.handler.job.savepoints.SavepointDisposalHandlers;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * {@link AsynchronousOperationTriggerMessageHeaders} for the {@link SavepointDisposalHandlers.SavepointDisposalTriggerHandler}.
 */
public class StartWatchingInputHeaders extends AsynchronousOperationTriggerMessageHeaders<StartWatchingInputRequest, StartWatchingInputMessageParameters> {

	private static final StartWatchingInputHeaders INSTANCE = new StartWatchingInputHeaders();

	private static final String URL = String.format(
		"/jobs/:%s/watchpoints",
		JobIDPathParameter.KEY);

	private StartWatchingInputHeaders() {}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public Class<StartWatchingInputRequest> getRequestClass() {
		return StartWatchingInputRequest.class;
	}

	@Override
	public StartWatchingInputMessageParameters getUnresolvedMessageParameters() {
		return new StartWatchingInputMessageParameters();
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.POST;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	public static StartWatchingInputHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	protected String getAsyncOperationDescription() {
		return "Start watching Input of each task.";
	}
}
