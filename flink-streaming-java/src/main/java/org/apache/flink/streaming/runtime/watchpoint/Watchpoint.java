package org.apache.flink.streaming.runtime.watchpoint;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.watchpoint.WatchpointCommand;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializableObject;

import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.net.Socket;
import java.sql.Timestamp;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class Watchpoint {

	private AbstractStreamOperator operator;

	private FilterFunction guardIN;

	private FilterFunction guardOUT;

	private OutputStream outputStream;

	private SocketClientSink socketClientSink;

	private SerializationSchema serializationSchema;

	private boolean isWatchingInput;

	private boolean isWatchingOutput;

	private String identifier; //adopted from flink metric identifiers

	// ------------------------------------------------------------------------
	//  Constructors
	// ------------------------------------------------------------------------

	public Watchpoint(AbstractStreamOperator operator){
		this.operator = checkNotNull(operator);

		//initialize no filter i.e. any record passes
		this.guardIN = (x) -> true;
		this.guardOUT = (x) -> true;

		this.outputStream = System.out;
		this.serializationSchema = new SimpleStringSchema();

		this.isWatchingInput = false;
		this.isWatchingOutput = false;

	}

	// ------------------------------------------------------------------------
	//  Operate
	// ------------------------------------------------------------------------

	public void operateWatchpoint(WatchpointCommand watchpointCommand) {

		switch(watchpointCommand.getAction()){
			case "startWatching":
				startWatching(watchpointCommand.getWhatToWatch(), watchpointCommand.getGuardClassName());
				break;
			case "stopWatching":
				stopWatching(watchpointCommand.getWhatToWatch());
				break;
			default:
				throw new UnsupportedOperationException("action " + watchpointCommand.getAction() + " is not supported for watchpoints. Use 'stopWatching' or 'startWatching'");
		}

	}

	private void startWatching(String target, String guardClassName) {

		FilterFunction guard;
		try{
			guard = loadFilterFunction(guardClassName);
		}catch(Exception e){
			e.printStackTrace();
			guard = (x) -> true;
		}

		switch(target){
			case "input":
				setGuardIN(guard);
				this.isWatchingInput = true;
				break;
			case "output":
				setGuardOUT(guard);
				this.isWatchingOutput = true;
				break;
			default:
				throw new UnsupportedOperationException("target for watchpoint action must be input or output");
		}
	}

	private void stopWatching(String target) {
		switch(target){
			case "input":
				this.isWatchingInput = false;
				break;
			case "output":
				this.isWatchingOutput = false;
				break;
			default:
				throw new UnsupportedOperationException("target for watchpoint action must be input or output");
		}
	}

	// ------------------------------------------------------------------------
	//  Watch methods
	// ------------------------------------------------------------------------

	public <IN> void watchInput(StreamRecord<IN> inStreamRecord){
		if(isWatchingInput){
			try{
				if(guardIN.filter(inStreamRecord.getValue())){
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
		this.identifier = operator.getMetricGroup().getMetricIdentifier("watchpoint");
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

	public void setGuardIN(FilterFunction guardIN) {

		try{
			this.guardIN = checkNotNull(guardIN);
		}catch (Exception e){
			this.guardIN = (x) -> true;
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
