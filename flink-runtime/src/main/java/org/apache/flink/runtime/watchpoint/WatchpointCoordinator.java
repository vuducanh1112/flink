package org.apache.flink.runtime.watchpoint;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class WatchpointCoordinator {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(WatchpointCoordinator.class);

	/** The job whose checkpoint this coordinator coordinates. */
	private final JobID job;

	/** The executor used for asynchronous calls, like potentially blocking I/O. */
	private final Executor executor;

	/** */
	private final Map<JobVertexID, ExecutionJobVertex> tasks;

	//----------------------------------------------------------------------------------------------
	// Constructors
	//----------------------------------------------------------------------------------------------

	public WatchpointCoordinator(
		JobID job,
		Executor executor,
		Map<JobVertexID, ExecutionJobVertex> tasks) {

		this.job = checkNotNull(job);
		this.executor = checkNotNull(executor);
		this.tasks = checkNotNull(tasks);

	}

	//----------------------------------------------------------------------------------------------
	// Watchpoint operations
	//----------------------------------------------------------------------------------------------

	public void operateWatchpoint(WatchpointCommand watchpointCommand) {

		LOG.info("Operating watchpoint with command:\n" + watchpointCommand);

		Collection<ExecutionVertex> subtasks = new ArrayList<>();

		if(watchpointCommand.hasTaskId()) {

			ExecutionJobVertex task = tasks.get(watchpointCommand.getTaskId());

			if(task == null) {
				LOG.warn("No task " + watchpointCommand.getTaskId() + " exists. Abort operating watchpoint.");
				return;
			}

			if(watchpointCommand.hasSubTaskIndex()){

				try{
					ExecutionVertex subtask = task.getTaskVertices()[watchpointCommand.getSubtaskIndex()];
					subtasks.add(subtask);
				}catch(IndexOutOfBoundsException e){
					LOG.warn("task " + watchpointCommand.getTaskId() + " has no subtask " + watchpointCommand.getSubtaskIndex());
				}

			}else{
				for(ExecutionVertex subtask : task.getTaskVertices()){
					subtasks.add(subtask);
				}
			}
		} else {

			//no task specified -> operate on all tasks and their subtasks
			for(ExecutionJobVertex task : tasks.values()){
				for(ExecutionVertex subtask : task.getTaskVertices()){
					subtasks.add(subtask);
				}
			}
		}

		for(ExecutionVertex subtask : subtasks){
			subtask.getCurrentExecutionAttempt().operateWatchpoint(watchpointCommand);
		}

	}

	//----------------------------------------------------------------------------------------------
	// Helper
	//----------------------------------------------------------------------------------------------



}
