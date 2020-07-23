package org.apache.flink.streaming.runtime.watchpoint;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TaskRecorder implements Runnable {

	public enum Command{

		RECORD_INPUT1(0, true),

		RECORD_INPUT2(1, true),

		RECORD_OUTPUT(2, true),

		STOP_RECORDING_INPUT1(0, false),

		STOP_RECORDING_INPUT2(1, false),

		STOP_RECORDING_OUTPUT(2, false);

		private int streamIndex;

		private boolean record;

		private Command(int streamIndex, boolean record){
			this.streamIndex = streamIndex;
			this.record = record;
		}

		public int getStreamIndex() {
			return this.streamIndex;
		}

		public boolean isRecord() {
			return record;
		}
	}

	private Map<OperatorID, FileOutputStream[]> watchpointRecordFiles;

	private LinkedBlockingQueue<Tuple4<OperatorID,byte[],Integer, Command>> requestQueue;

	private volatile boolean alive;

	private int QUEUE_CAPACITY_PER_OPERATOR = 10;

	private Object lock = new Object();

	public TaskRecorder() {

		watchpointRecordFiles = new HashMap();

		requestQueue = new LinkedBlockingQueue<>(10 * QUEUE_CAPACITY_PER_OPERATOR);

	}

	@Override
	public void run() {

		alive = true;

		while (this.alive) {

			Tuple4<OperatorID,byte[],Integer,TaskRecorder.Command> request = null;

			// get the next buffer. ignore interrupts that are not due to a shutdown.
			while (alive && request == null) {
				try {
					request = requestQueue.take();
					handleRequest(request);
				}
				catch (InterruptedException e) {
					if (!this.alive) {
						return;
					} else {
						//IOManagerAsync.LOG.warn(Thread.currentThread() + " was interrupted without shutdown.");
					}
				}
			}
		}
	}

	private void handleRequest(Tuple4<OperatorID,byte[],Integer,TaskRecorder.Command> request){
		try {
			synchronized (lock){
				FileOutputStream outputStream = watchpointRecordFiles.get(request.f0)[request.f3.getStreamIndex()];
				if(outputStream != null){
					if(request.f3.isRecord()){
						outputStream.write(request.f1, 0, request.f2);
					}else{
						stopRecording(request.f0, request.f3);
					}
				}
			}
		}
		catch (IOException e) {

		}
		catch (Throwable t) {

		}
	}

	public void stop() {
		alive = false;
	}

	public void startRecording(OperatorID operatorID, Path recordsFile, Command command) {

		synchronized (lock){
			try{
				FileSystem fs = recordsFile.getFileSystem();
				FileOutputStream recordsFileOutputStream;

				fs.mkdirs(recordsFile.getParent());

				recordsFileOutputStream = new FileOutputStream(new File(recordsFile.makeQualified(fs).getPath()), true);

				if(watchpointRecordFiles.containsKey(operatorID)){
					watchpointRecordFiles.get(operatorID)[command.getStreamIndex()] = recordsFileOutputStream;
				}else{
					//each operator has three file output streams: input1, input2, output
					watchpointRecordFiles.put(operatorID, new FileOutputStream[3]);
				}

			}catch(IOException e){

			}
		}

	}

	public void stopRecording(OperatorID operatorID, Command command) {

		synchronized (lock) {
			FileOutputStream outputStream = watchpointRecordFiles.get(operatorID)[command.getStreamIndex()];
			if (outputStream != null) {
				try {
					outputStream.flush();
					outputStream.close();
					watchpointRecordFiles.get(operatorID)[command.getStreamIndex()] = null;
				} catch (IOException e) {

				}
			}
		}
	}

	public void close(){

		flush();

		synchronized (lock) {
			for(OperatorID operatorID : watchpointRecordFiles.keySet()){
				stopRecording(operatorID, Command.STOP_RECORDING_INPUT1);
				stopRecording(operatorID, Command.STOP_RECORDING_INPUT2);
				stopRecording(operatorID, Command.STOP_RECORDING_OUTPUT);
			}
		}

	}

	public void flush(){
		synchronized (lock){
			try{
				while(requestQueue.isEmpty() == false){
					handleRequest(requestQueue.take());
				}
			}catch(InterruptedException e){

			}
		}
	}

	public LinkedBlockingQueue<Tuple4<OperatorID,byte[],Integer,TaskRecorder.Command>> getRequestQueue(){
		return this.requestQueue;
	}

}
