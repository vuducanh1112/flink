package org.apache.flink.runtime.watchpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class WatchpointCoordinator {

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
		switch(watchpointCommand.getAction()){
			case "startWatching":
				switch(watchpointCommand.getWhatToWatch()){
					case "input":
						startWatchingInput(watchpointCommand.getGuardClassName());
						break;
					case "output":
						startWatchingOutput(watchpointCommand.getGuardClassName());
						break;
				}
				break;
			case "stopWatching":
				switch(watchpointCommand.getWhatToWatch()){
					case "input":
						stopWatchingInput();
						break;
					case "output":
						stopWatchingOutput();
						break;
				}
				break;
			default:
				throw new UnsupportedOperationException("action " + watchpointCommand.getAction() + " is not supported for watchpoints. Use 'stopWatching' or 'startWatching'");
		}
	}

	public void startWatchingInput(String guardClassName) {

		LOG.info("Start watching input of tasks");

		for(ExecutionJobVertex executionJobVertex : tasks.values()){
			for(ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()){
				executionVertex.getCurrentExecutionAttempt().startWatchingInput(guardClassName);
			}
		}
	}

	public void stopWatchingInput() {

		LOG.info("Stop watching input of tasks");

		for(ExecutionJobVertex executionJobVertex : tasks.values()){
			for(ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()){
				executionVertex.getCurrentExecutionAttempt().stopWatchingInput();
			}
		}
	}

	public void startWatchingOutput(String guardClassName) {

		LOG.info("Start watching output of tasks");

		for(ExecutionJobVertex executionJobVertex : tasks.values()){
			for(ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()){
				executionVertex.getCurrentExecutionAttempt().startWatchingOutput(guardClassName);
			}
		}
	}

	public void stopWatchingOutput() {

		LOG.info("Stop watching output of tasks");

		for(ExecutionJobVertex executionJobVertex : tasks.values()){
			for(ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()){
				executionVertex.getCurrentExecutionAttempt().stopWatchingOutput();
			}
		}
	}


	public void operateWatchpoint(JobVertexID taskId, WatchpointCommand watchpointCommand) {

		ExecutionJobVertex task = tasks.get(taskId);
		if (task == null) {
			throw new IllegalStateException(
				String.format("No task with id %s.", taskId));
		}



	}

	//----------------------------------------------------------------------------------------------
	// Helper
	//----------------------------------------------------------------------------------------------



}
