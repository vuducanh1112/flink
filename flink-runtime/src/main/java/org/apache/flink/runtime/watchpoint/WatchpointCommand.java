package org.apache.flink.runtime.watchpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class WatchpointCommand implements java.io.Serializable {

	private String action;

	private String whatToWatch;

	private JobID jobId;

	private OperatorID operatorId;

	private boolean hasOperatorId = false;

	private JobVertexID taskId;

	private boolean hasTaskId = false;

	private Integer subtaskIndex;

	private boolean hasSubTaskIndex = false;

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


	public WatchpointCommand(
		String action,
		String whatToWatch,
		JobID jobId,
		JobVertexID taskId,
		Integer subtaskIndex,
		OperatorID operatorId,
		String guardClassName) {

		this(action, whatToWatch, jobId);

		if(guardClassName == null) {
			this.guardClassName = "";
		}

		if(taskId != null){
			this.taskId = taskId;
			this.hasTaskId = true;
		}

		if(subtaskIndex != null){
			this.subtaskIndex = subtaskIndex;
			this.hasSubTaskIndex = true;
		}

		if(operatorId != null){
			this.operatorId = operatorId;
			this.hasOperatorId = true;
		}

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

	public OperatorID getOperatorId() {
		return operatorId;
	}

	public JobVertexID getTaskId() {
		return taskId;
	}

	public Integer getSubtaskIndex() { return subtaskIndex; }

	public String getGuardClassName() { return guardClassName; }

	public boolean hasOperatorId() { return hasOperatorId; }

	public boolean hasTaskId() { return hasTaskId; }

	public boolean hasSubTaskIndex() { return hasSubTaskIndex; }

}
