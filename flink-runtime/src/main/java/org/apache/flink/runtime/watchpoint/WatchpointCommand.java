package org.apache.flink.runtime.watchpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class WatchpointCommand {

	private String action;

	private String whatToWatch;

	private JobID jobId;

	private OperatorID operatorID;

	private JobVertexID jobVertexID;

	private ExecutionVertexID executionVertexID;

	private String guardClassName;

	//----------------------------------------------------------------------------------------------
	// Constructors
	//----------------------------------------------------------------------------------------------

	public WatchpointCommand(String action, String whatToWatch, JobID jobId) {
		this.action = checkNotNull(action);
		this.whatToWatch = checkNotNull(whatToWatch);
		this.jobId = checkNotNull(jobId);
		this.guardClassName = "";
	}

	public WatchpointCommand(String action, String whatToWatch, JobID jobId, String guardClassName) {
		this.action = checkNotNull(action);
		this.whatToWatch = checkNotNull(whatToWatch);
		this.jobId = checkNotNull(jobId);
		this.guardClassName = checkNotNull(guardClassName);
	}

	//----------------------------------------------------------------------------------------------
	// Getters
	//----------------------------------------------------------------------------------------------

	public String getAction() { return action; }

	public String getWhatToWatch() {
		return whatToWatch;
	}

	public JobID getJobId() {
		return jobId;
	}

	public OperatorID getOperatorID() {
		return operatorID;
	}

	public JobVertexID getJobVertexID() {
		return jobVertexID;
	}

	public ExecutionVertexID getExecutionVertexID() {
		return executionVertexID;
	}

	public String getGuardClassName() { return guardClassName; }
}
