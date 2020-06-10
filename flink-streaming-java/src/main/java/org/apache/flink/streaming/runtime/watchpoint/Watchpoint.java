package org.apache.flink.streaming.runtime.watchpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.watchpoint.WatchpointCommand;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.sql.Timestamp;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class Watchpoint {

	protected static final Logger LOG = LoggerFactory.getLogger(Watchpoint.class);

	private AbstractStreamOperator operator;

	private FilterFunction guardIN1;

	private FilterFunction guardIN2;

	private FilterFunction guardOUT;

	private OutputStream outputStream;

	private SocketClientSink socketClientSink;

	private SerializationSchema serializationSchema;

	private boolean isWatchingInput1;

	private boolean isWatchingInput2;

	private boolean isWatchingOutput;

	private String identifier; //adopted from flink metric identifiers

	// ------------------------------------------------------------------------
	//  Constructors
	// ------------------------------------------------------------------------

	public Watchpoint(AbstractStreamOperator operator){
		this.operator = checkNotNull(operator);

		//initialize no filter i.e. any record passes
		this.guardIN1 = (x) -> true;
		this.guardIN2 = (x) -> true;
		this.guardOUT = (x) -> true;

		this.outputStream = System.out;
		this.serializationSchema = new SimpleStringSchema();

		this.isWatchingInput1 = false;
		this.isWatchingInput2 = false;
		this.isWatchingOutput = false;

	}

	// ------------------------------------------------------------------------
	//  Operate
	// ------------------------------------------------------------------------

	public void operateWatchpoint(WatchpointCommand watchpointCommand) {

		switch(watchpointCommand.getAction()){
			case "startWatching":
				startWatching(watchpointCommand.getWhatToWatch(), watchpointCommand.getGuard1ClassName(), watchpointCommand.getGuard2ClassName());
				break;
			case "stopWatching":
				stopWatching(watchpointCommand.getWhatToWatch());
				break;
			default:
				throw new UnsupportedOperationException("action " + watchpointCommand.getAction() + " is not supported for watchpoints. Use 'stopWatching' or 'startWatching'");
		}
	}

	private void startWatching(String target, String guard1ClassName, String guard2ClassName) {

		FilterFunction guard1;
		try{
			guard1 = loadFilterFunction(guard1ClassName);
		}catch(Exception e){
			LOG.warn("Could not load guard1. Using no guard instead");
			guard1 = (x) -> true;
		}

		FilterFunction guard2;
		try{
			guard2 = loadFilterFunction(guard2ClassName);
		}catch(Exception e){
			LOG.warn("Could not load guard2. Using no guard instead");
			guard2 = (x) -> true;
		}

		switch(target){
			case "input":
				setGuardIN1(guard1);
				this.isWatchingInput1 = true;
				setGuardIN2(guard2);
				this.isWatchingInput2 = true;
				break;
			case "input1":
				setGuardIN1(guard1);
				this.isWatchingInput1 = true;
				break;
			case "input2":
				setGuardIN2(guard2);
				this.isWatchingInput2 = true;
				break;
			case "output":
				setGuardOUT(guard1);
				this.isWatchingOutput = true;
				break;
			default:
				throw new UnsupportedOperationException("target for watchpoint action must be either input, input1, input2 or output");
		}
	}

	private void stopWatching(String target) {
		switch(target){
			case "input":
				this.isWatchingInput1 = false;
				this.isWatchingInput2 = false;
				break;
			case "input1":
				this.isWatchingInput1 = false;
				break;
			case "input2":
				this.isWatchingInput2 = false;
				break;
			case "output":
				this.isWatchingOutput = false;
				break;
			default:
				throw new UnsupportedOperationException("target for watchpoint action must be either input, input1, input2 or output");
		}
	}

	// ------------------------------------------------------------------------
	//  Watch methods
	// ------------------------------------------------------------------------

	public <IN1> void watchInput1(StreamRecord<IN1> inStreamRecord){
		if(isWatchingInput1){
			try{
				if(guardIN1.filter(inStreamRecord.getValue())){
					outputStream.write(serializationSchema.serialize((new Timestamp(System.currentTimeMillis())).toString() + " " + identifier + ": " + inStreamRecord.toString() + "\n"));
				}
			}catch(Exception e){
				e.printStackTrace(System.err);
			}
		}
	}

	public <IN2> void watchInput2(StreamRecord<IN2> inStreamRecord){
		if(isWatchingInput2){
			try{
				if(guardIN2.filter(inStreamRecord.getValue())){
					outputStream.write(serializationSchema.serialize((new Timestamp(System.currentTimeMillis())).toString() + " " + identifier + ": " + inStreamRecord.toString() + "\n"));
				}
			}catch(Exception e){
				e.printStackTrace(System.err);
			}
		}
	}

	public <OUT> void watchOutput(StreamRecord<OUT> outStreamRecord){
		if(isWatchingOutput){
			try{
				if(guardOUT.filter(outStreamRecord.getValue())){
					outputStream.write(serializationSchema.serialize((new Timestamp(System.currentTimeMillis())).toString() + " " + identifier + ": " + outStreamRecord.toString() + "\n"));
				}
			}catch(Exception e){
				e.printStackTrace(System.err);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Utility
	// ------------------------------------------------------------------------

	public void setIdentifier() {

		JobID job = operator.getContainingTask().getEnvironment().getJobID();
		JobVertexID task = operator.getContainingTask().getEnvironment().getJobVertexId();
		String subtask = operator.getContainingTask().getName();
		OperatorID operatorID = operator.getOperatorID();
		String operaterName = operator.getOperatorConfig().getOperatorName();

		StringBuilder builder = new StringBuilder();

		builder.append("job").append(job.toHexString()).append(".");
		builder.append("task").append(task.toHexString()).append(".");
		builder.append(subtask).append(".");
		builder.append(operaterName).append(".");
		builder.append(operatorID.toHexString());

		//this.identifier = operator.getMetricGroup().getMetricIdentifier("watchpoint");
		this.identifier = builder.toString();

	}

	private FilterFunction loadFilterFunction(String className) throws Exception{

		if(className == ""){
			return (x) -> true;
		}

		//ClassLoader classLoader = FilterFunction.class.getClassLoader();
		ClassLoader classLoader = operator.getUserCodeClassloader();
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
			return filterFunction;
		} catch (Exception e) {
			throw new FlinkException("Could not instantiate the filter function " + className + ".", e);
		}

	}

	// ------------------------------------------------------------------------
	//  Setter and Getter
	// ------------------------------------------------------------------------

	public void setGuardIN1(FilterFunction guardIN1) {

		try{
			this.guardIN1 = checkNotNull(guardIN1);
		}catch (Exception e){
			this.guardIN1 = (x) -> true;
		}

	}

	public void setGuardIN2(FilterFunction guardIN) {

		try{
			this.guardIN2 = checkNotNull(guardIN);
		}catch (Exception e){
			this.guardIN2 = (x) -> true;
		}

	}

	public void setGuardOUT(FilterFunction guardOUT) {

		try{
			this.guardOUT = checkNotNull(guardOUT);
		}catch (Exception e){
			this.guardOUT = (x) -> true;
		}

	}

}
