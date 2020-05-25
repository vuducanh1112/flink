package org.apache.flink.runtime.watchpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Map;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class WatchpointCoordinator {

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

	public void startWatchingInput() {
		for(ExecutionJobVertex executionJobVertex : tasks.values()){
			for(ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()){
				executionVertex.getCurrentExecutionAttempt().startWatchingInput();
			}
		}
	}

}
