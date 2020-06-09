package org.apache.flink.runtime.watchpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;

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

	private String guard1ClassName;

	private String guard2ClassName;

	//----------------------------------------------------------------------------------------------
	// Constructors
	//----------------------------------------------------------------------------------------------

	public WatchpointCommand(String action, String whatToWatch, JobID jobId) {
		this.action = checkNotNull(action);
		this.whatToWatch = checkNotNull(whatToWatch);
		this.jobId = checkNotNull(jobId);
		this.guard1ClassName = "";
		this.guard2ClassName = "";
	}

	public WatchpointCommand(
		String action,
		String whatToWatch,
		JobID jobId,
		JobVertexID taskId,
		Integer subtaskIndex,
		OperatorID operatorId,
		String guard1ClassName,
		String guard2ClassName) {

		this(action, whatToWatch, jobId);

		if(guard1ClassName != null) {
			this.guard1ClassName = guard1ClassName;
		}

		if(guard2ClassName != null) {
			this.guard1ClassName = guard2ClassName;
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

	public String getGuard1ClassName() { return guard1ClassName; }

	public String getGuard2ClassName() { return guard2ClassName; }

	public boolean hasOperatorId() { return hasOperatorId; }

	public boolean hasTaskId() { return hasTaskId; }

	public boolean hasSubTaskIndex() { return hasSubTaskIndex; }

}
