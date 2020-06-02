package org.apache.flink.runtime.watchpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
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

	public void operateWatchpoint(String action, WatchpointCommand target) {
		switch(action){
			case "startWatching":
				FilterFunction guard;
				try{
					guard = loadFilterFunction(target.getGuardClassName());
				}catch(Exception e){
					LOG.warn("filter function " + target.getGuardClassName() + " could not be loaded");
					guard = (x) -> true;
				}

				switch(target.getWhatToWatch()){
					case "input":
						startWatchingInput(guard);
						break;
					case "output":
						startWatchingOutput(guard);
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

	public void startWatchingInput(FilterFunction guard) {

		LOG.info("Start watching input of tasks");

		for(ExecutionJobVertex executionJobVertex : tasks.values()){
			for(ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()){
				executionVertex.getCurrentExecutionAttempt().startWatchingInput(guard);
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

	public void startWatchingOutput(FilterFunction guard) {

		LOG.info("Start watching output of tasks");

		for(ExecutionJobVertex executionJobVertex : tasks.values()){
			for(ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()){
				executionVertex.getCurrentExecutionAttempt().startWatchingOutput(guard);
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

	//----------------------------------------------------------------------------------------------
	// Helper
	//----------------------------------------------------------------------------------------------

	private FilterFunction loadFilterFunction(String className) throws Exception{

		if(className == ""){
			return (x) -> true;
		}


		ClassLoader classLoader = FilterFunction.class.getClassLoader();
		final Class<? extends FilterFunction> filterFunctionClass;
		try {
			filterFunctionClass = Class.forName(className, true, classLoader)
				.asSubclass(FilterFunction.class);
		} catch (Throwable t) {
			throw new Exception("Could not load the filter function " + className + ".", t);
		}

		Constructor<? extends FilterFunction> statelessCtor;

		try {
			statelessCtor = filterFunctionClass.getConstructor();
		} catch (NoSuchMethodException ee) {
			throw new FlinkException("Task misses proper constructor", ee);
		}

		// instantiate the class
		try {
			//noinspection ConstantConditions  --> cannot happen
			FilterFunction filterFunction =  statelessCtor.newInstance();
			System.out.println("start test");
			if(filterFunction.filter("hello")){
				System.out.println("hello");
			}
			if(filterFunction.filter("no")){
				System.out.println("no");
			}
			return filterFunction;
		} catch (Exception e) {
			throw new FlinkException("Could not instantiate the filter function " + className + ".", e);
		}

	}

}
