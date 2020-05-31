package org.apache.flink.runtime.watchpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class WatchpointCoordinator {

	private static final Logger LOG = LoggerFactory.getLogger(WatchpointCoordinator.class);

	/** The job whose checkpoint this coordinator coordinates. */
	private final JobID job;

	/** The executor used for asynchronous calls, like potentially blocking I/O. */
	private final Executor executor;

	/** */
	private final Map<JobVertexID, ExecutionJobVertex> tasks;

	public WatchpointCoordinator(
		JobID job,
		Executor executor,
		Map<JobVertexID, ExecutionJobVertex> tasks) {

		this.job = checkNotNull(job);
		this.executor = checkNotNull(executor);
		this.tasks = checkNotNull(tasks);

	}

	public void operateWatchpoint(String action, WatchpointTarget target) {
		switch(action){
			case "startWatching":
				switch(target.getWhatToWatch()){
					case "input":
						startWatchingInput();
						break;
					case "output":
						startWatchingOutput();
						break;
				}
				break;
			case "stopWatching":
				switch(target.getWhatToWatch()){
					case "input":
						stopWatchingInput();
						break;
					case "output":
						stopWatchingOutput();
						break;
				}
				break;
			default:
				throw new UnsupportedOperationException("action " + action + " is not supported for watchpoints. Use 'stopWatching' or 'startWatching'");
		}
	}

	public void startWatchingInput() {

		LOG.info("Start watching input of tasks");

		for(ExecutionJobVertex executionJobVertex : tasks.values()){
			for(ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()){
				executionVertex.getCurrentExecutionAttempt().startWatchingInput();
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

	public void startWatchingOutput() {

		LOG.info("Start watching output of tasks");

		for(ExecutionJobVertex executionJobVertex : tasks.values()){
			for(ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()){
				executionVertex.getCurrentExecutionAttempt().startWatchingOutput();
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

}
