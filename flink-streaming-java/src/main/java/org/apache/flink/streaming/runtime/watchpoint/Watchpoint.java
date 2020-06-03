package org.apache.flink.streaming.runtime.watchpoint;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.FlinkException;

import java.io.OutputStream;
import java.lang.reflect.Constructor;
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
					outputStream.write(serializationSchema.serialize(outStreamRecord.toString()));
				}
			}catch(Exception e){
				e.printStackTrace(System.err);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Utility
	// ------------------------------------------------------------------------

	public void startWatchingInput(String guardClassName) {
		FilterFunction guard;
		try{
			guard = loadFilterFunction(guardClassName);
		}catch(Exception e){
			guard = (x) -> true;
		}
		setGuardIN(guard);
		this.isWatchingInput = true;
	}

	public void stopWatchingInput() {
		this.isWatchingInput = false;
	}

	public void startWatchingOutput(String guardClassName) {
		FilterFunction guard;
		try{
			guard = loadFilterFunction(guardClassName);
		}catch(Exception e){
			guard = (x) -> true;
		}
		setGuardOUT(guard);
		this.isWatchingOutput = true;
	}

	public void stopWatchingOutput() {
		this.isWatchingOutput = false;
	}

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
