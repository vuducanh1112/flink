package org.apache.flink.runtime.watchpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class WatchpointTarget {

	private String whatToWatch;

	private JobID jobId;

	private OperatorID operatorID;

	private JobVertexID jobVertexID;

	private ExecutionVertexID executionVertexID;

	//----------------------------------------------------------------------------------------------
	// Constructors
	//----------------------------------------------------------------------------------------------

	public WatchpointTarget(String whatToWatch, JobID jobId) {
		this.whatToWatch = checkNotNull(whatToWatch);
		this.jobId = checkNotNull(jobId);
	}

	//----------------------------------------------------------------------------------------------
	// Getters
	//----------------------------------------------------------------------------------------------

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
}
