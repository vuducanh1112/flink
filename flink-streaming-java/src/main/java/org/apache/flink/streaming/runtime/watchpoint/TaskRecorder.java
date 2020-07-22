package org.apache.flink.streaming.runtime.watchpoint;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalDataOutputStream;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TaskRecorder implements Runnable {

	public enum Command{

		WRITE,

		STOP

	}

	private Map<OperatorID, FileOutputStream> watchpointRecordFiles;

	private LinkedBlockingQueue<Tuple3<OperatorID,byte[],TaskRecorder.Command>> recordsToWriteQueue;

	private volatile boolean alive;

	private int QUEUE_CAPACITY_PER_OPERATOR = 10;

	private Object lock = new Object();

	public TaskRecorder() {

		watchpointRecordFiles = new HashMap();
		watchpointRecordFiles = Collections.synchronizedMap(watchpointRecordFiles);

		recordsToWriteQueue = new LinkedBlockingQueue<>(10 * QUEUE_CAPACITY_PER_OPERATOR);

		alive = true;

	}

	@Override
	public void run() {

		while (this.alive) {

			Tuple3<OperatorID,byte[],TaskRecorder.Command> request = null;

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

			// remember any IO exception that occurs, so it can be reported to the writer
			IOException ioex = null;

			try {
				// write buffer to the specified channel
				synchronized (lock){
					FileOutputStream outputStream = watchpointRecordFiles.get(request.f0);
					if(outputStream != null){
						switch(request.f2){
							case WRITE:
								outputStream.write(request.f1);
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
				ioex = e;
			}
			catch (Throwable t) {
				ioex = new IOException("The buffer could not be written: " + t.getMessage(), t);
				//IOManagerAsync.LOG.error("I/O writing thread encountered an error" + (t.getMessage() == null ? "." : ": " + t.getMessage()), t);
			}

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

		synchronized (lock) {
			for(OperatorID operatorID : watchpointRecordFiles.keySet()){
				stopRecording(operatorID);
			}
		}

	}

	public LinkedBlockingQueue<Tuple3<OperatorID,byte[],TaskRecorder.Command>> getRecordsToWriteQueue(){
		return this.recordsToWriteQueue;
	}

}
