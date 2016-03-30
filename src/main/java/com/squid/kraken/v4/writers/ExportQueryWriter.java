package com.squid.kraken.v4.writers;

import java.io.OutputStream;

import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheReference;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;
import com.squid.kraken.v4.export.ExportSourceWriter;

public class ExportQueryWriter extends QueryWriter {

	OutputStream out; 
	ExportSourceWriter writer ;
		
	
	public ExportQueryWriter(ExportSourceWriter w, OutputStream out){
		this.out = out;
		this.writer = w ;
		
	}

	
	public void  write(){
			
		if (val instanceof RawMatrix ){
			writer.write((RawMatrix) val, out) ;
		}
 
		if (val instanceof RedisCacheValuesList){
			RedisCacheValuesList list = (RedisCacheValuesList) val;
			writer.write( new ChunkedRawMatrixWrapper(this.key, list), out );			
		}	
		
		
		if (val instanceof RedisCacheReference){
			
			
		}

		
	}
	
}
