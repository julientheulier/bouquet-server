package com.squid.kraken.v4.export;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.export.IRawExportSource;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;

public class ChunkedRawMatrixExportSource extends ChunkedRawMatrixBaseSource implements IRawExportSource{

	private Schema schema;

	int colNumbers;
	String[] columnNames;
	int[] columnTypes;
	

	static final Logger logger = LoggerFactory.getLogger(ChunkedRawMatrixExportSource.class);
	
	public ChunkedRawMatrixExportSource(RedisCacheValuesList refList ) throws InterruptedException, ExecutionException{
		super(refList);
				
		// build metadata in parallel
		this.colNumbers = currentChunk.getColNames().size();
		this.columnNames=  new String[this.colNumbers];
		for(int i = 0 ; i < this.colNumbers ; i++){
			this.columnNames[i] = currentChunk.getColNames().get(i);
		}
		
		this.columnTypes = new int[currentChunk.getColTypes().size()] ;
		for (int i = 0; i < currentChunk.getColTypes().size(); i++){
			columnTypes[i] = currentChunk.getColTypes().get(i).intValue() ;
		}
		
		//construct schema
//		this.schema = SchemaAvro.constructAvroSchema(key, columnTypes, columnNames);
	
	}
	
	@Override
	public int getNumberOfColumns() {
		return colNumbers;
		
	}

	@Override
	public String getColumnName(int pos) {
		return this.columnNames[pos];
	}

	@Override
	public int getColumnType(int pos) {
		return  this.columnTypes[pos];
	}

	@Override
	public int[] getColumnTypes() {
		return this.columnTypes;
	}

	@Override
	public String[] getColumnNames() {
		return this.columnNames;
	}

	@Override
	public Iterator<Object[]> iterator() {
		return new RowInterator();
	}

	@Override
	public Schema getSchema() {
		return this.schema;
	}
	

	
	
	public class RowInterator implements Iterator<Object[]>{
		
		int cursor ;
		
		boolean done =false;
		
		@Override
		public boolean hasNext() {
			return !done;
		}

		@Override
		public Object[] next() {			
			Object[] res;
			res = currentChunk.getRows().get(cursor).data;
			cursor++;
			
			if (cursor>= currentChunk.getRows().size()){
				// get next Chunk
				RawMatrix next;
				try {
					next = processingQuery.get();
				} catch (InterruptedException | ExecutionException e) {
					logger.info("Error retrieving new chunk " + e.toString());
					next = null;
				}
				if (next == null){
					done = true;
				}else{
					currentChunk = next;
					cursor =0 ;
					// launch chunk retrieval in parallel ;
					GetChunk getNextChunk = new GetChunk();
					processingQuery = (Future<RawMatrix>) executor.submit(getNextChunk);
				}				 
			}
			return res;
		}

		@Override
		public void remove() {
            throw new UnsupportedOperationException();			
		}
		
		
	}
}

