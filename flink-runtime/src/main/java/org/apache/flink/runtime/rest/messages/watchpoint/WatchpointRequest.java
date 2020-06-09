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

	private static final String FIELD_NAME_TASK_ID = "taskId";

	private static final String FIELD_NAME_SUBTASK_INDEX = "subtaskIndex";

	private static final String FIELD_NAME_WATCHPOINT_GUARD1CLASSNAME = "guard1ClassName";

	private static final String FIELD_NAME_WATCHPOINT_GUARD2CLASSNAME = "guard2ClassName";

	@JsonProperty(FIELD_NAME_WATCHPOINT_ACTION)
	private final String action;

	@JsonProperty(FIELD_NAME_WATCHPOINT_TARGET)
	private final String target;

	@JsonProperty(FIELD_NAME_TASK_ID)
	@Nullable
	private final String taskId;

	@JsonProperty(FIELD_NAME_SUBTASK_INDEX)
	@Nullable
	private final String subtaskIndex;

	@JsonProperty(FIELD_NAME_WATCHPOINT_GUARD1CLASSNAME)
	private final String guard1ClassName;

	@JsonProperty(FIELD_NAME_WATCHPOINT_GUARD2CLASSNAME)
	private final String guard2ClassName;

	@JsonCreator
	public WatchpointRequest(
		@Nullable @JsonProperty(FIELD_NAME_WATCHPOINT_ACTION) final String action,
		@Nullable @JsonProperty(FIELD_NAME_WATCHPOINT_TARGET) final String target,
		@Nullable @JsonProperty(FIELD_NAME_TASK_ID) final String taskId,
		@Nullable @JsonProperty(FIELD_NAME_SUBTASK_INDEX) final String subtaskIndex,
		@Nullable @JsonProperty(FIELD_NAME_WATCHPOINT_GUARD1CLASSNAME) final String guard1ClassName,
		@Nullable @JsonProperty(FIELD_NAME_WATCHPOINT_GUARD2CLASSNAME) final String guard2ClassName) {
		this.action = action;
		this.target = target;
		this.taskId = taskId;
		this.subtaskIndex = subtaskIndex;
		this.guard1ClassName = guard1ClassName;
		this.guard2ClassName = guard2ClassName;
	}


	public String getAction() {
		return action;
	}

	public String getTarget() {
		return target;
	}

	public String getTaskId() { return taskId; }

	public String getSubtaskIndex() { return subtaskIndex; }

	public String getGuard1ClassName() { return guard1ClassName; }

	public String getGuard2ClassName() { return guard2ClassName; }

}
