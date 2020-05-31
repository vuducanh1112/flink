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

package org.apache.flink.runtime.rest.handler.job.watchpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.AbstractAsynchronousOperationHandlers;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationInfo;
import org.apache.flink.runtime.rest.handler.async.OperationKey;
import org.apache.flink.runtime.rest.messages.*;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalRequest;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalStatusHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalTriggerHeaders;
import org.apache.flink.runtime.rest.messages.watchpoint.*;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.watchpoint.WatchpointTarget;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.util.SerializedThrowable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Handlers to trigger operations regarding watchpoints.
 */
public class WatchpointHandlers extends AbstractAsynchronousOperationHandlers<OperationKey, Acknowledge> {

	/**
	 * {@link TriggerHandler} implementation for starting to watch inputs.
	 */
	public class StartWatchingInputTriggerHandler extends TriggerHandler<RestfulGateway, StartWatchingInputRequest, StartWatchingInputMessageParameters> {

		public StartWatchingInputTriggerHandler(
				GatewayRetriever<? extends RestfulGateway> leaderRetriever,
				Time timeout,
				Map<String, String> responseHeaders) {
			super(
				leaderRetriever,
				timeout,
				responseHeaders,
				StartWatchingInputHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<Acknowledge> triggerOperation(HandlerRequest<StartWatchingInputRequest, StartWatchingInputMessageParameters> request, RestfulGateway gateway) throws RestHandlerException {
			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);

			return gateway.startWatchingInput(jobId);
		}

		@Override
		protected OperationKey createOperationKey(HandlerRequest<StartWatchingInputRequest, StartWatchingInputMessageParameters> request) {
			return new OperationKey(new TriggerId());
		}
	}

	/**
	 * {@link TriggerHandler} implementation for starting to watch inputs.
	 */
	public class StopWatchingInputTriggerHandler extends TriggerHandler<RestfulGateway, StartWatchingInputRequest, StartWatchingInputMessageParameters> {

		public StopWatchingInputTriggerHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders) {
			super(
				leaderRetriever,
				timeout,
				responseHeaders,
				StartWatchingInputHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<Acknowledge> triggerOperation(HandlerRequest<StartWatchingInputRequest, StartWatchingInputMessageParameters> request, RestfulGateway gateway) throws RestHandlerException {
			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);

			return gateway.stopWatchingInput(jobId);
		}

		@Override
		protected OperationKey createOperationKey(HandlerRequest<StartWatchingInputRequest, StartWatchingInputMessageParameters> request) {
			return new OperationKey(new TriggerId());
		}
	}

	/**
	 * {@link TriggerHandler} implementation for starting to watch inputs.
	 */
	public class WatchpointOperationTriggerHandler extends TriggerHandler<RestfulGateway, WatchpointRequest, WatchpointMessageParameters> {

		public WatchpointOperationTriggerHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders) {
			super(
				leaderRetriever,
				timeout,
				responseHeaders,
				WatchpointHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<Acknowledge> triggerOperation(HandlerRequest<WatchpointRequest, WatchpointMessageParameters> request, RestfulGateway gateway) throws RestHandlerException {
			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			String action = request.getPathParameter(WatchpointActionParameter.class);
			String whatToWatch = request.getPathParameter(WatchpointTargetParameter.class);

			return gateway.operateWatchpoints(jobId, action, new WatchpointTarget(whatToWatch, jobId));
		}

		@Override
		protected OperationKey createOperationKey(HandlerRequest<WatchpointRequest, WatchpointMessageParameters> request) {
			return new OperationKey(new TriggerId());
		}
	}

}
