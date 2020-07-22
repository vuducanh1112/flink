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

		WRITE,

		STOP

	}

	private Map<OperatorID, FileOutputStream> watchpointRecordFiles;

	private LinkedBlockingQueue<Tuple4<OperatorID,byte[],Integer, Command>> recordsToWriteQueue;

	private volatile boolean alive;

	private int QUEUE_CAPACITY_PER_OPERATOR = 10;

	private Object lock = new Object();

	public TaskRecorder() {

		watchpointRecordFiles = new HashMap();

		recordsToWriteQueue = new LinkedBlockingQueue<>(10 * QUEUE_CAPACITY_PER_OPERATOR);

		alive = true;

	}

	@Override
	public void run() {

		while (this.alive) {

			Tuple4<OperatorID,byte[],Integer,TaskRecorder.Command> request = null;

			// get the next buffer. ignore interrupts that are not due to a shutdown.
			while (alive && request == null) {
				try {
					request = recordsToWriteQueue.take();
				}
				catch (InterruptedException e) {
					if (!this.alive) {
						return;
					} else {
						//IOManagerAsync.LOG.warn(Thread.currentThread() + " was interrupted without shutdown.");
					}
				}
			}

			handleRequest(request);

		}
	}

	private void handleRequest(Tuple4<OperatorID,byte[],Integer,TaskRecorder.Command> request){
		try {
			synchronized (lock){
				FileOutputStream outputStream = watchpointRecordFiles.get(request.f0);
				if(outputStream != null){
					switch(request.f3){
						case WRITE:
							outputStream.write(request.f1, 0, request.f2);
							break;
						case STOP:
							stopRecording(request.f0);
							break;
						default:
							throw new Exception();
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

	public void startRecording(OperatorID operatorID, Path recordsFile) {

		synchronized (lock){
			try{
				FileSystem fs = recordsFile.getFileSystem();
				FileOutputStream recordsFileOutputStream;

				fs.mkdirs(recordsFile.getParent());

				recordsFileOutputStream = new FileOutputStream(new File(recordsFile.makeQualified(fs).getPath()), true);

				watchpointRecordFiles.put(operatorID, recordsFileOutputStream);

			}catch(IOException e){

			}
		}

	}

	public void stopRecording(OperatorID operatorID) {

		synchronized (lock) {
			FileOutputStream outputStream = watchpointRecordFiles.get(operatorID);
			if (outputStream != null) {
				try {
					outputStream.flush();
					outputStream.close();
					watchpointRecordFiles.put(operatorID, null);
				} catch (IOException e) {

				}
			}
		}
	}

	public void close(){

		flush();

		synchronized (lock) {
			for(OperatorID operatorID : watchpointRecordFiles.keySet()){
				stopRecording(operatorID);
			}
		}

	}

	public void flush(){
		synchronized (lock){
			try{
				while(recordsToWriteQueue.isEmpty() == false){
					handleRequest(recordsToWriteQueue.take());
				}
			}catch(InterruptedException e){

			}
		}
	}

	public LinkedBlockingQueue<Tuple4<OperatorID,byte[],Integer,TaskRecorder.Command>> getRecordsToWriteQueue(){
		return this.recordsToWriteQueue;
	}

}
