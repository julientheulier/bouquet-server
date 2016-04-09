package com.squid.kraken.v4.writers;

import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.export.ExportSourceWriter;
import com.squid.kraken.v4.export.ExportSourceWriterVelocity;

public class ExportQueryWriter extends QueryWriter {

	OutputStream out; 
	ExportSourceWriter writer ;
	long linesWritten;
	String jobID;
	
	static final Logger logger = LoggerFactory
			.getLogger(QueryWriter.class);
	
	public ExportQueryWriter(ExportSourceWriter w, OutputStream out, String jobId){
		this.out = out;
		this.writer = w ;
		this.jobID = jobId;
	}

	@Override
	public void write() throws ComputingException{
		long startExport = System.currentTimeMillis();
		if (writer instanceof ExportSourceWriterVelocity){
			((ExportSourceWriterVelocity) writer).setQueryMapper(this.mapper);
		}
		try{
		
			if (val instanceof RawMatrix ){
				this.linesWritten =	writer.write((RawMatrix) val, out) ;
			}
 
			if (val instanceof RedisCacheValuesList){
				this.linesWritten =	writer.write( (RedisCacheValuesList) val, out );			
			}			
			
			long stopExport = System.currentTimeMillis();
			logger.info("task="+this.getClass().getName()+" method=compute.writeData"+" jobid="+jobID +" lineWritten="+linesWritten+" duration="+ (stopExport-startExport)+" error=false status=done");

		} catch (Throwable e) {
			logger.error("failed to export jobId="+jobID +":", e);
			throw e;
		}		
	}
	
	public long getLinesWritten(){
		return this.linesWritten;
	}
	
}
