package com.squid.kraken.v4.export;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.avro.Schema;

import com.squid.core.export.ICol;
import com.squid.core.export.IRow;
import com.squid.core.export.IStructExportSource;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.QueryMapper;

public class ChunkedRawMatrixStructExportSource implements IStructExportSource {

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
	
	private QueryMapper mapper;


	
	public ChunkedRawMatrixStructExportSource(RedisCacheValuesList reflist, QueryMapper mapper){
		
		this.refList = refList;
		this.key = refList.getRedisKey() ;
		this.mapper = mapper;
		
	}


	
	
	@Override
	public Iterable<IRow> getRows() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ICol> getCols() {
		// TODO Auto-generated method stub
		return null;
	}

}
