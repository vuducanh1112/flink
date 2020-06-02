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

import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/**
 * Request body to operate watchpoints.
 */
public class WatchpointRequest implements RequestBody {

	public static final String FIELD_NAME_WATCHPOINT_ACTION = "action";

	private static final String FIELD_NAME_WATCHPOINT_TARGET = "target";

	private static final String FIELD_NAME_WATCHPOINT_GUARDCLASSNAME = "guardClassName";

	@JsonProperty(FIELD_NAME_WATCHPOINT_ACTION)
	@Nullable
	private final String action;

	@JsonProperty(FIELD_NAME_WATCHPOINT_TARGET)
	private final String target;

	@JsonProperty(FIELD_NAME_WATCHPOINT_GUARDCLASSNAME)
	private final String guardClassName;

	@JsonCreator
	public WatchpointRequest(
		@Nullable @JsonProperty(FIELD_NAME_WATCHPOINT_ACTION) final String action,
		@Nullable @JsonProperty(FIELD_NAME_WATCHPOINT_TARGET) final String target,
		@Nullable @JsonProperty(FIELD_NAME_WATCHPOINT_GUARDCLASSNAME) final String guardClassName) {
		this.action = action;
		this.target = target;
		this.guardClassName = guardClassName;
	}


	public String getAction() {
		return action;
	}

	public String getTarget() {
		return target;
	}

	public String getGuardClassName() { return guardClassName; }

}
