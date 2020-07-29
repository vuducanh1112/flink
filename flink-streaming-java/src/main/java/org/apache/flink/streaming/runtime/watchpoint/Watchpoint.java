package org.apache.flink.streaming.runtime.watchpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.watchpoint.WatchpointCommand;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
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

	private String dir;

	private OutputStream outputStream;

	private FSDataOutputStream input1Records;

	private FSDataOutputStream input2Records;

	private FSDataOutputStream outputRecords;

	private SerializationSchema input1SerializationSchema;

	private SerializationSchema input2SerializationSchema;

	private SerializationSchema outputSerializationSchema;

	private volatile boolean isWatchingInput1;

	private volatile boolean isWatchingInput2;

	private volatile boolean isWatchingOutput;

	private String identifier;

	public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024; // 1 MB

	private byte[] input1Buffer;

	private byte[] input2Buffer;

	private byte[] outputBuffer;

	private int currentInput1BufferPos;

	private int currentInput2BufferPos;

	private int currentOutputBufferPos;

	private Object lock = new Object();

	// ------------------------------------------------------------------------
	//  Constructors and Initializers
	// ------------------------------------------------------------------------

	public Watchpoint(AbstractStreamOperator operator){
		this.operator = checkNotNull(operator);
		//this.setIdentifier();

		this.dir = "tmp/" +
			this.operator.getContainingTask().getEnvironment().getJobID() + "/" +
			this.operator.getContainingTask().getEnvironment().getJobVertexId() + "/" +
			this.operator.getOperatorID() + "_" +
			this.operator.getOperatorConfig().getOperatorName() + "_" +
			this.operator.getContainingTask().getEnvironment().getTaskInfo().getIndexOfThisSubtask() +
			"/";

		if((this.operator instanceof StreamSource) == false){
			initInput1Watching();
		}

		if(this.operator instanceof TwoInputStreamOperator){
			initInput2Watching();
		}

		if((this.operator instanceof StreamSink) == false){
			initOutputWatching();
		}

	}

	private void initInput1Watching(){
		input1Buffer = new byte[DEFAULT_BUFFER_SIZE];
		currentInput1BufferPos = 0;
		this.guardIN1 = (x) -> true;
		this.isWatchingInput1 = false;
		this.input1SerializationSchema = new SimpleStringSchema();
		startWatchingInput1("");
	}

	private void initInput2Watching(){
		input2Buffer = new byte[DEFAULT_BUFFER_SIZE];
		currentInput2BufferPos = 0;
		this.guardIN2 = (x) -> true;
		this.isWatchingInput2 = false;
		this.input2SerializationSchema = new SimpleStringSchema();
		startWatchingInput2("");
	}

	private void initOutputWatching(){
		outputBuffer = new byte[DEFAULT_BUFFER_SIZE];
		currentOutputBufferPos = 0;
		this.guardOUT = (x) -> true;
		this.isWatchingOutput = false;
		this.outputSerializationSchema = new SimpleStringSchema();
		startWatchingOutput("");
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

	// ------------------------------------------------------------------------
	//  Start Watching
	// ------------------------------------------------------------------------

	private void startWatching(String target, String guard1ClassName, String guard2ClassName) {

		switch(target){
			case "input":
				startWatchingInput1(guard1ClassName);
				startWatchingInput2(guard2ClassName);
				break;
			case "input1":
				startWatchingInput1(guard1ClassName);
				break;
			case "input2":
				startWatchingInput2(guard2ClassName);
				break;
			case "output":
				startWatchingOutput(guard1ClassName);
				break;
			default:
				throw new UnsupportedOperationException("target for watchpoint action must be either input, input1, input2 or output");
		}
	}

	private void startWatchingInput1(String guardClassName) {

		setGuardIN1(guardClassName);

		Path input1File = new Path(this.dir + "input1.records");
		this.operator.getContainingTask().getTaskRecorder().startRecording(operator.getOperatorID(), input1File, TaskRecorder.Command.RECORD_INPUT1);

		synchronized (lock) {
			this.isWatchingInput1 = true;
		}

	}

	private void startWatchingInput2(String guardClassName) {

		setGuardIN2(guardClassName);

		Path input2File = new Path(this.dir + "input2.records");
		this.operator.getContainingTask().getTaskRecorder().startRecording(operator.getOperatorID(), input2File, TaskRecorder.Command.RECORD_INPUT2);

		synchronized (lock) {
			this.isWatchingInput2 = true;
		}

	}

	private void startWatchingOutput(String guardClassName) {

		setGuardOUT(guardClassName);

		Path outputFile = new Path(this.dir + "output.records");
		this.operator.getContainingTask().getTaskRecorder().startRecording(operator.getOperatorID(), outputFile, TaskRecorder.Command.RECORD_OUTPUT);

		synchronized (lock) {
			this.isWatchingOutput = true;
		}

	}

	// ------------------------------------------------------------------------
	//  Stop Watching
	// ------------------------------------------------------------------------

	private void stopWatching(String target) {
		switch(target){
			case "input":
				closeInput1Watcher();
				closeInput2Watcher();
				break;
			case "input1":
				closeInput1Watcher();
				break;
			case "input2":
				closeInput2Watcher();
				break;
			case "output":
				closeOutputWatcher();
				break;
			default:
				throw new UnsupportedOperationException("target for watchpoint action must be either input, input1, input2 or output");
		}
	}

	// ------------------------------------------------------------------------
	//  Watch methods
	// ------------------------------------------------------------------------

	public <IN1> void watchInput1(StreamRecord<IN1> inStreamRecord) {

		synchronized (lock) {

			try{
				if(isWatchingInput1 && guardIN1.filter(inStreamRecord.getValue())){

					byte[] toWrite = input1SerializationSchema.serialize(
						(new Timestamp(System.currentTimeMillis())).toString() + " " +
//					identifier + ".input1" +  ": " +
							inStreamRecord.toString() +
							"\n");

					Tuple4<OperatorID, byte[], Integer, TaskRecorder.Command> writeRequest;

					if(currentInput1BufferPos + toWrite.length >= input1Buffer.length){

						if(currentInput1BufferPos == 0){ //record is larger than the buffer
							writeRequest = new Tuple4<>(operator.getOperatorID(), toWrite, toWrite.length, TaskRecorder.Command.RECORD_INPUT1);
						}else{
							writeRequest = new Tuple4<>(operator.getOperatorID(), input1Buffer, currentInput1BufferPos, TaskRecorder.Command.RECORD_INPUT1);
						}

						this.operator.getContainingTask().getTaskRecorder().getRequestQueue().put(writeRequest);
						currentInput1BufferPos = 0;

					}else{
						System.arraycopy(toWrite, 0, input1Buffer, currentInput1BufferPos, toWrite.length);
						currentInput1BufferPos = currentInput1BufferPos + toWrite.length;
					}
				}
			}catch(Exception e){
				LOG.error(e.getMessage());
			}
		}
	}

	public <IN2> void watchInput2(StreamRecord<IN2> inStreamRecord) {
		synchronized (lock) {

			try{
				if(isWatchingInput2 && guardIN2.filter(inStreamRecord.getValue())){

					byte[] toWrite = input2SerializationSchema.serialize(
						(new Timestamp(System.currentTimeMillis())).toString() + " " +
								inStreamRecord.toString() +
								"\n");

					Tuple4<OperatorID, byte[], Integer, TaskRecorder.Command> writeRequest;

					if(currentInput2BufferPos + toWrite.length >= input2Buffer.length){

						if(currentInput2BufferPos == 0){ //record is larger than the buffer
							writeRequest = new Tuple4<>(operator.getOperatorID(), toWrite, toWrite.length, TaskRecorder.Command.RECORD_INPUT2);
						}else{
							writeRequest = new Tuple4<>(operator.getOperatorID(), input2Buffer, currentInput2BufferPos, TaskRecorder.Command.RECORD_INPUT2);
						}

						this.operator.getContainingTask().getTaskRecorder().getRequestQueue().put(writeRequest);
						currentInput2BufferPos = 0;

					}else{
						System.arraycopy(toWrite, 0, input2Buffer, currentInput2BufferPos, toWrite.length);
						currentInput2BufferPos = currentInput2BufferPos + toWrite.length;
					}
				}
			}catch(Exception e){
				LOG.error(e.getMessage());
			}
		}
	}

	public <OUT> void watchOutput(StreamRecord<OUT> outStreamRecord) {
		synchronized (lock) {

			try{
				if(isWatchingOutput && guardOUT.filter(outStreamRecord.getValue())){

					byte[] toWrite = outputSerializationSchema.serialize(
						(new Timestamp(System.currentTimeMillis())).toString() + " " +
//					identifier + ".input1" +  ": " +
							outStreamRecord.toString() +
							"\n");

					Tuple4<OperatorID, byte[], Integer, TaskRecorder.Command> writeRequest;

					if(currentOutputBufferPos + toWrite.length >= outputBuffer.length){

						if(currentOutputBufferPos == 0){ //record is larger than the buffer
							writeRequest = new Tuple4<>(operator.getOperatorID(), toWrite, toWrite.length, TaskRecorder.Command.RECORD_OUTPUT);
						}else{
							writeRequest = new Tuple4<>(operator.getOperatorID(), outputBuffer, currentOutputBufferPos, TaskRecorder.Command.RECORD_OUTPUT);
						}

						this.operator.getContainingTask().getTaskRecorder().getRequestQueue().put(writeRequest);
						currentOutputBufferPos = 0;

					}else{
						System.arraycopy(toWrite, 0, outputBuffer, currentOutputBufferPos, toWrite.length);
						currentOutputBufferPos = currentOutputBufferPos + toWrite.length;
					}
				}
			}catch(Exception e){
				LOG.error(e.getMessage());
			}
		}
	}

	public void reportData(){

	}

	// ------------------------------------------------------------------------
	//  Close
	// ------------------------------------------------------------------------

	public void close() {
		if((this.operator instanceof StreamSource) == false){
			closeInput1Watcher();
		}

		if(this.operator instanceof TwoInputStreamOperator){
			closeInput2Watcher();
		}

		if((this.operator instanceof StreamSink) == false){
			closeOutputWatcher();
		}
	}

	private void closeInput1Watcher() {

		synchronized (lock) {
			flushInput1Buffer();

			Tuple4<OperatorID, byte[], Integer, TaskRecorder.Command> writeRequest = new Tuple4<>(operator.getOperatorID(), null, -1, TaskRecorder.Command.STOP_RECORDING_INPUT1);
			try{
				this.operator.getContainingTask().getTaskRecorder().getRequestQueue().put(writeRequest);
			}catch(InterruptedException e){
				LOG.error(e.getMessage());
			}
			isWatchingInput1 = false;
		}

	}

	private void closeInput2Watcher() {
		synchronized (lock) {
			flushInput2Buffer();

			Tuple4<OperatorID, byte[], Integer, TaskRecorder.Command> writeRequest = new Tuple4<>(operator.getOperatorID(), null, -1, TaskRecorder.Command.STOP_RECORDING_INPUT2);
			try{
				this.operator.getContainingTask().getTaskRecorder().getRequestQueue().put(writeRequest);
			}catch(InterruptedException e){
				LOG.error(e.getMessage());
			}
			isWatchingInput2 = false;
		}
	}

	private void closeOutputWatcher() {
		synchronized (lock) {
			flushOutputBuffer();

			Tuple4<OperatorID, byte[], Integer, TaskRecorder.Command> writeRequest = new Tuple4<>(operator.getOperatorID(), null, -1, TaskRecorder.Command.STOP_RECORDING_OUTPUT);
			try{
				this.operator.getContainingTask().getTaskRecorder().getRequestQueue().put(writeRequest);
			}catch(InterruptedException e){
				LOG.error(e.getMessage());
			}
			isWatchingOutput = false;
		}
	}

	public void flushInput1Buffer() {
		synchronized (lock){
			if(currentInput1BufferPos > 0){
				Tuple4<OperatorID, byte[], Integer, TaskRecorder.Command> writeRequest = new Tuple4<>(operator.getOperatorID(), input1Buffer, currentInput1BufferPos, TaskRecorder.Command.RECORD_INPUT1);
				try{
					this.operator.getContainingTask().getTaskRecorder().getRequestQueue().put(writeRequest);
					currentInput1BufferPos = 0;
				}catch(InterruptedException e){
					LOG.error(e.getMessage());
				}
			}
		}
	}

	public void flushInput2Buffer() {
		synchronized (lock){
			if(currentInput2BufferPos > 0){
				Tuple4<OperatorID, byte[], Integer, TaskRecorder.Command> writeRequest = new Tuple4<>(operator.getOperatorID(), input2Buffer, currentInput2BufferPos, TaskRecorder.Command.RECORD_INPUT2);
				try{
					this.operator.getContainingTask().getTaskRecorder().getRequestQueue().put(writeRequest);
					currentInput2BufferPos = 0;
				}catch(InterruptedException e){
					LOG.error(e.getMessage());
				}
			}
		}
	}

	public void flushOutputBuffer() {
		synchronized (lock){
			if(currentOutputBufferPos > 0){
				Tuple4<OperatorID, byte[], Integer, TaskRecorder.Command> writeRequest = new Tuple4<>(operator.getOperatorID(), outputBuffer, currentOutputBufferPos, TaskRecorder.Command.RECORD_OUTPUT);
				try{
					this.operator.getContainingTask().getTaskRecorder().getRequestQueue().put(writeRequest);
					currentOutputBufferPos = 0;
				}catch(InterruptedException e){
					LOG.error(e.getMessage());
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Guards
	// ------------------------------------------------------------------------

	public void setGuardIN1(String guardClassName) {

		try{
			this.guardIN1 = loadFilterFunction(guardClassName);
		}catch(Exception e){
			LOG.warn("Could not load guard1. Using no guard instead");
			this.guardIN1 = (x) -> true;
		}

	}

	public void setGuardIN2(String guardClassName) {

		try{
			this.guardIN2 = loadFilterFunction(guardClassName);
		}catch(Exception e){
			LOG.warn("Could not load guard2. Using no guard instead");
			this.guardIN2 = (x) -> true;
		}

	}

	public void setGuardOUT(String guardClassName) {

		try{
			this.guardOUT = loadFilterFunction(guardClassName);
		}catch(Exception e){
			LOG.warn("Could not load guard2. Using no guard instead");
			this.guardOUT = (x) -> true;
		}

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
	//  MISC
	// ------------------------------------------------------------------------

	public void setIdentifier() {

		JobID job = operator.getContainingTask().getEnvironment().getJobID();
		JobVertexID task = operator.getContainingTask().getEnvironment().getJobVertexId();
		String subtask = operator.getContainingTask().getName();
		OperatorID operatorID = operator.getOperatorID();
		String operaterName = operator.getOperatorConfig().getOperatorName();

		StringBuilder builder = new StringBuilder();

		builder.append("job.").append(job.toHexString()).append(".");
		builder.append("task.").append(task.toHexString()).append(".");
		builder.append(subtask).append(".");
		builder.append(operaterName).append(".");
		builder.append(operatorID.toHexString());

		//this.identifier = operator.getMetricGroup().getMetricIdentifier("watchpoint");
		this.identifier = builder.toString();

	}

}
