package org.apache.flink.streaming.runtime.watchpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalDataOutputStream;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.watchpoint.WatchpointCommand;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

	private SerializationSchema serializationSchema;

	private boolean isWatchingInput1;

	private boolean isWatchingInput2;

	private boolean isWatchingOutput;

	private String identifier;

	public static final int BUFFER_SIZE = 10 * 1024 * 1024;

	private byte[] buffer;

	private int currentBufferPos;

	private Object lock = new Object();

	// ------------------------------------------------------------------------
	//  Constructors
	// ------------------------------------------------------------------------

	public Watchpoint(AbstractStreamOperator operator){
		this.operator = checkNotNull(operator);
		this.setIdentifier();

		buffer = new byte[BUFFER_SIZE];
		currentBufferPos = 0;

		//initialize no filter i.e. any record passes
		this.guardIN1 = (x) -> true;
		this.guardIN2 = (x) -> true;
		this.guardOUT = (x) -> true;

		this.isWatchingInput1 = false;
		this.isWatchingInput2 = false;
		this.isWatchingOutput = false;

		this.dir = "tmp/" +
			this.operator.getContainingTask().getEnvironment().getJobID() + "/" +
			this.operator.getContainingTask().getEnvironment().getJobVertexId() + "/" +
			this.operator.getOperatorID() + "_" +
			this.operator.getOperatorConfig().getOperatorName() + "_" +
			this.operator.getContainingTask().getEnvironment().getTaskInfo().getIndexOfThisSubtask() +
			"/";

		this.outputStream = System.out;
		this.serializationSchema = new SimpleStringSchema();

		startWatchingInput1("");
		//startWatchingInput2("");

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

		FilterFunction guard1;
		try{
			guard1 = loadFilterFunction(guardClassName);
		}catch(Exception e){
			LOG.warn("Could not load guard1. Using no guard instead");
			guard1 = (x) -> true;
		}

		synchronized (lock) {
			Path input1File = new Path(this.dir + "input1.records");
			this.operator.getContainingTask().getTaskRecorder().startRecording(operator.getOperatorID(), input1File);
			this.isWatchingInput1 = true;
		}

		/*
		try{
			Path input1File = new Path(this.dir + "input1.records");
			FileSystem fs = input1File.getFileSystem();

			fs.mkdirs(input1File.getParent());

			input1Records = fs.create(input1File, FileSystem.WriteMode.NO_OVERWRITE);

			setGuardIN1(guard1);
			this.isWatchingInput1 = true;
		}catch(IOException e){
			LOG.error("IO exception when trying to access file for writing records. " + e.getMessage());
		}
		*/
	}

	private void startWatchingInput2(String guardClassName) {

		FilterFunction guard2;
		try{
			guard2 = loadFilterFunction(guardClassName);
		}catch(Exception e){
			LOG.warn("Could not load guard2. Using no guard instead");
			guard2 = (x) -> true;
		}

		try{
			Path input2File = new Path(this.dir + "input2.records");
			FileSystem fs = input2File.getFileSystem();

			fs.mkdirs(input2File.getParent());

			input2Records = fs.create(input2File, FileSystem.WriteMode.NO_OVERWRITE);

			setGuardIN2(guard2);
			this.isWatchingInput2 = true;
		}catch(IOException e){
			LOG.error("IO exception when trying to access file for writing records. " + e.getMessage());
		}

	}

	private void startWatchingOutput(String guardClassName) {

		FilterFunction guard;
		try{
			guard = loadFilterFunction(guardClassName);
		}catch(Exception e){
			LOG.warn("Could not load guard1. Using no guard instead");
			guard = (x) -> true;
		}

		try{
			Path outputFile = new Path(this.dir + "output.records");
			FileSystem fs = outputFile.getFileSystem();

			fs.mkdirs(outputFile.getParent());

			outputRecords = fs.create(outputFile, FileSystem.WriteMode.NO_OVERWRITE);

			setGuardOUT(guard);
			this.isWatchingOutput = true;
		}catch(IOException e){
			LOG.error("IO exception when trying to access file for writing records. " + e.getMessage());
		}

	}

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

	public <IN1> void watchInput1(StreamRecord<IN1> inStreamRecord){

		synchronized (lock) {
			if(isWatchingInput1){

				byte[] toWrite = serializationSchema.serialize(
					(new Timestamp(System.currentTimeMillis())).toString() + " " +
//					identifier + ".input1" +  ": " +
						inStreamRecord.toString() +
						"\n");

				Tuple3<OperatorID, byte[], TaskRecorder.Command> writeRequest;


				if(currentBufferPos + toWrite.length >= buffer.length){

					if(currentBufferPos == 0){ //record is larger than the buffer
						writeRequest = new Tuple3<>(operator.getOperatorID(), toWrite, TaskRecorder.Command.WRITE);
					}else{
						writeRequest = new Tuple3<>(operator.getOperatorID(), buffer, TaskRecorder.Command.WRITE);
					}

					try{
						this.operator.getContainingTask().getTaskRecorder().getRecordsToWriteQueue().put(writeRequest);
						currentBufferPos = 0;
					}catch(InterruptedException e){

					}

				}else{
					System.arraycopy(toWrite, 0, buffer, currentBufferPos, toWrite.length);
					currentBufferPos = currentBufferPos + toWrite.length;
					return;
				}


			/*
			try{
				if(guardIN1.filter(inStreamRecord.getValue())){
					input1Records.write(serializationSchema.serialize(
						(new Timestamp(System.currentTimeMillis())).toString() + " " +
							identifier + ".input1" +  ": " +
							inStreamRecord.toString() +
							"\n"));
				}
			}catch(Exception e){
				e.printStackTrace(System.err);
			}
			 */
			}
		}
	}

	public <IN2> void watchInput2(StreamRecord<IN2> inStreamRecord){
		if(isWatchingInput2){
			try{
				if(guardIN2.filter(inStreamRecord.getValue())){
					input2Records.write(serializationSchema.serialize(
						(new Timestamp(System.currentTimeMillis())).toString() + " " +
							identifier + ".input2" +  ": " +
							inStreamRecord.toString() +
							"\n"));
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
					outputRecords.write(serializationSchema.serialize(
						(new Timestamp(System.currentTimeMillis())).toString() + " " +
							identifier + ".output" + ": " +
							outStreamRecord.toString() +
							"\n"));
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

		builder.append("job.").append(job.toHexString()).append(".");
		builder.append("task.").append(task.toHexString()).append(".");
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

	public void flush() {
		synchronized (lock){
			if(currentBufferPos > 0){
				Tuple3<OperatorID, byte[], TaskRecorder.Command> writeRequest = new Tuple3<>(operator.getOperatorID(), buffer, TaskRecorder.Command.WRITE);
				try{
					this.operator.getContainingTask().getTaskRecorder().getRecordsToWriteQueue().put(writeRequest);
					currentBufferPos = 0;
				}catch(InterruptedException e){

				}
			}
		}
	}

	public void close() {
		closeInput1Watcher();
		closeInput2Watcher();
		closeOutputWatcher();
	}

	private void closeInput1Watcher() {

		synchronized (lock) {
			flush();

			Tuple3<OperatorID, byte[], TaskRecorder.Command> writeRequest = new Tuple3<>(operator.getOperatorID(), null, TaskRecorder.Command.STOP);
			try{
				this.operator.getContainingTask().getTaskRecorder().getRecordsToWriteQueue().put(writeRequest);
			}catch(InterruptedException e){

			}

			isWatchingInput1 = false;
		}



		//this.operator.getContainingTask().getTaskRecorder().stopRecording(operator.getOperatorID());
		/*
		try{

			if(input1Records != null) {
				input1Records.close();
			}
		}catch(IOException e){

		}
		*/
	}

	private void closeInput2Watcher() {
		try{
			if(input2Records != null) {
				input2Records.close();
			}
		}catch(IOException e){

		}
	}

	private void closeOutputWatcher() {
		try{
			if(outputRecords != null) {
				outputRecords.close();
			}
		}catch(IOException e){

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
