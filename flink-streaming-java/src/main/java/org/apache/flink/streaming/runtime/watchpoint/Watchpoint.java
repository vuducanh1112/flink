package org.apache.flink.streaming.runtime.watchpoint;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.OutputStream;

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

		this.isWatchingInput = true;
		this.isWatchingOutput = true;
	}

	// ------------------------------------------------------------------------
	//  Watch methods
	// ------------------------------------------------------------------------

	public <IN> void watchInput(StreamRecord<IN> inStreamRecord){
		if(isWatchingInput){
			try{
				if(guardIN.filter(inStreamRecord.getValue())){
					outputStream.write(serializationSchema.serialize(inStreamRecord.toString()));
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

	public void startWatchingInput(){
		this.isWatchingInput = true;
	}

	public void stopWatchingInput(){
		this.isWatchingInput = false;
	}

	public void startWatchingOutput(){
		this.isWatchingOutput = true;
	}

	public void stopWatchingOutput(){
		this.isWatchingOutput = false;
	}

	// ------------------------------------------------------------------------
	//  Setter and Getter
	// ------------------------------------------------------------------------

	public void setGuardIN(FilterFunction guardIN){
		this.guardIN = checkNotNull(guardIN);
	}

	public void setGuardOUT(FilterFunction guardOUT){
		this.guardOUT = checkNotNull(guardOUT);
	}

}
