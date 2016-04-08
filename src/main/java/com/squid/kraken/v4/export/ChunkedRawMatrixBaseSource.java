package com.squid.kraken.v4.export;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.export.ICol;
import com.squid.kraken.v4.caching.redis.RedisCacheException;
import com.squid.kraken.v4.caching.redis.RedisCacheProxy;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValue;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;

public class ChunkedRawMatrixBaseSource {

	protected RawMatrix currentChunk;

	protected int nbChunksRead = 0;

	protected String key;
	protected RedisCacheValuesList refList;
	protected Future<RawMatrix> processingQuery;

	protected ExecutorService executor;

	private Schema schema;


	static final Logger logger = LoggerFactory.getLogger(ChunkedRawMatrixBaseSource.class);

	public ChunkedRawMatrixBaseSource(RedisCacheValuesList rf) throws InterruptedException, ExecutionException {
		this.refList = rf;
		this.key = this.refList.getRedisKey();
		this.executor= Executors.newFixedThreadPool(1);

		// get first chunk
		processingQuery = (Future<RawMatrix>) executor.submit(new GetChunk());
		this.currentChunk = processingQuery.get();

		// launch second chunk
		processingQuery = (Future<RawMatrix>) executor.submit(new GetChunk());
		
	}

	private String getNextChunkKey() throws ClassNotFoundException, IOException, InterruptedException {
		if (refList.getReferenceKeys().size() > nbChunksRead) {
			String res = refList.getReferenceKeys().get(nbChunksRead).referencedKey;
			return res;
		} else {
			if (!refList.isDone()) {
				boolean ok = false;
				int waitingCount = 1;
				while (!ok) {
					RedisCacheValue val = RedisCacheValue.deserialize(RedisCacheProxy.getInstance().get(key));
					if (val instanceof RedisCacheValuesList) {
						this.refList = (RedisCacheValuesList) val;

						if (this.refList.getReferenceKeys().size() > nbChunksRead) {
							ok = true;
						} else {
							Thread.sleep(waitingCount * 10 * 1000);
							waitingCount += 1;
						}
					} else {
						throw new RedisCacheException("could not retrieve chunk list");
					}
				}
				String res = refList.getReferenceKeys().get(nbChunksRead).referencedKey;
				nbChunksRead++;
				return res;
			} else {
				return null;
			}
		}

	}

	public class GetChunk implements Callable<RawMatrix> {

		public GetChunk() {
		}

		public RawMatrix call() {
			String chunkKey;
			try {
				chunkKey = getNextChunkKey();
				if (chunkKey == null) {
					logger.info("Full matrix retrieve from cache, " + nbChunksRead + "chunks ");
					return null;
				} else {
					RawMatrix res = RedisCacheProxy.getInstance().getRawMatrix(chunkKey);
					nbChunksRead += 1;
					return res;
				}
			} catch (ClassNotFoundException | IOException | InterruptedException e) {
				return null;
			}
		}
	}
	
	public class Col implements ICol{

		private boolean isData;
		private String name;
		private Object pk;
		
		public Col(String name, boolean isData, Object pk){
			this.name = name ;
			this.isData= isData;
			this.pk = pk;
		}
		public String toString(){
			return this.name+" " +this.getRole() ;
			
		}
		
		@Override
		public String getRole() {
			if(isData){
				return "DATA";
			}
			else{
				return "DOMAIN";
			}
		}

		@Override
		public String getName() {
			return this.name;
		}

		@Override
		public Object getPk() {
			return this.pk;
		}
		

	}

}
