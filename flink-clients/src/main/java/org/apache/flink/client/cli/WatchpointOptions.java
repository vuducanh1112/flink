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

package org.apache.flink.client.cli;

import org.apache.commons.cli.CommandLine;

import static org.apache.flink.client.cli.CliFrontendParser.*;

/**
 * Command line options for the WATCHPOINT command.
 */
public class WatchpointOptions extends CommandLineOptions {

	private final String[] args;

	private String action;
	private String target;
	private String jobId;
	private String operatorId;
	private String taskId;
	private String subtaskIndex;
	private String guard1;
	private String guard2;

	private String jarFile;

	public WatchpointOptions(CommandLine line) {
		super(line);
		args = line.getArgs();
		action = line.getOptionValue(WATCHPOINT_ACTION.getOpt());
		target = line.getOptionValue(WATCHPOINT_ACTION_TARGET.getOpt());
		jobId = line.getOptionValue(WATCHPOINT_JOB_ID.getOpt());
		operatorId = line.getOptionValue(WATCHPOINT_OPERATOR_ID.getOpt());
		taskId = line.getOptionValue(WATCHPOINT_TASK_ID.getOpt());
		subtaskIndex = line.getOptionValue(WATCHPOINT_SUBTASK_INDEX.getOpt());
		guard1 = line.getOptionValue(WATCHPOINT_GUARD1.getOpt());
		guard2 = line.getOptionValue(WATCHPOINT_GUARD2.getOpt());


		jarFile = line.getOptionValue(JAR_OPTION.getOpt());
	}

	public String[] getArgs() {
		return args == null ? new String[0] : args;
	}

	public String getAction() {
		return action;
	}

	public String getTarget() {
		return target;
	}

	public String getJobId() {
		return jobId;
	}

	public String getOperatorId() {
		return operatorId;
	}

	public String getTaskId() {
		return taskId;
	}

	public String getSubtaskIndex() {
		return subtaskIndex;
	}

	public String getGuard1() { return guard1; }

	public String getGuard2() { return guard2; }
}
