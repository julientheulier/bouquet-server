package com.squid.kraken.v4.export;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.avro.Schema;

import com.squid.core.export.IRawExportSource;
import com.squid.kraken.v4.caching.redis.RedisCacheException;
import com.squid.kraken.v4.caching.redis.RedisCacheProxy;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValue;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;

public class ChunkedRawMatrixExportSource implements IRawExportSource{

	private Schema schema;

	
	RawMatrix currentChunk;
	
	int nbChunksRead = 0;
	
	int colNumbers;
	String[] columnNames;
	int[] columnTypes;
	
	String key;
	RedisCacheValuesList refList ;
	Future<RawMatrix>  processingQuery ;

	private ExecutorService executor;
	

	
	public ChunkedRawMatrixExportSource(RedisCacheValuesList refList ) throws InterruptedException, ExecutionException{
		this.key = refList.getRedisKey();
		this.refList = refList;
		this.executor= Executors.newFixedThreadPool(1);
		
		// get first chunk					
		processingQuery = (Future<RawMatrix>) executor.submit(new GetChunk());
		this.currentChunk = processingQuery.get();
		
		// launch second chunk  
		processingQuery = (Future<RawMatrix>) executor.submit(new GetChunk());

		// build metadata in parallel
		this.colNumbers = currentChunk.getColNames().size();
		this.columnNames= (String[]) currentChunk.getColNames().toArray();
		this.columnTypes = new int[currentChunk.getColTypes().size()] ;
		for (int i = 0; i < currentChunk.getColTypes().size(); i++){
			columnTypes[i] = currentChunk.getColTypes().get(i).intValue() ;
		}
		
		//construct schema
		this.schema = SchemaAvro.constructAvroSchema(key, columnTypes, columnNames);
	
	}
	
	private String getNextChunkKey() throws ClassNotFoundException, IOException, InterruptedException{
		if (refList.getReferenceKeys().size() > nbChunksRead){
			String res =  refList.getReferenceKeys().get(nbChunksRead).referencedKey;
			return res ;
		}else{
			if ( ! refList.isDone()){
				boolean ok = false ;
				int waitingCount = 1;
				while (!ok){
					RedisCacheValue  val= RedisCacheValue.deserialize(RedisCacheProxy.getInstance().get(key));
					if (val instanceof RedisCacheValuesList ){					
						this.refList =(RedisCacheValuesList) val;		
						
						if (this.refList.getReferenceKeys().size() > nbChunksRead ){
							ok= true; 
						}else{
							Thread.sleep(waitingCount*10*1000);	
							waitingCount+=1;
						}					
					}else{
						throw new RedisCacheException("could not retrieve chunk list");
					}
				}
				String res =  refList.getReferenceKeys().get(nbChunksRead).referencedKey;
				nbChunksRead ++ ;
				return res ;				
			}else{
				return null;
			}
		}
		
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
	
	
	public class GetChunk implements Callable<RawMatrix> {

		public GetChunk(){
		}

		public RawMatrix call(){
			
			String chunkKey;
			try {
				chunkKey = getNextChunkKey();
				if (chunkKey == null){
					return  null;
				}else{
					RawMatrix res= RedisCacheProxy.getInstance().getRawMatrix(chunkKey);
					nbChunksRead+=1;
					return res;
				}
			} catch (ClassNotFoundException | IOException |InterruptedException e) {
				return null;
			}
		}			
	}
	
	
	public class RowInterator implements Iterator<Object[]>{

		
		int cursor ;
		
		boolean done =false;
		
		@Override
		public boolean hasNext() {
			return done;
		}

		@Override
		public Object[] next() {			
			
			Object[] res = currentChunk.getRows().get(cursor).getData();
			cursor++;
			if (cursor>currentChunk.getRows().size()){
				// get next Chunk
				RawMatrix next;
				try {
					next = processingQuery.get();
				} catch (InterruptedException | ExecutionException e) {
					return null;
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

