package com.squid.kraken.v4.writers;

import com.squid.core.database.model.Database;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValue;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.QueryMapper;

public class QueryWriter {

	protected RedisCacheValue val;
	protected QueryMapper mapper;
	protected Database db;
	protected String SQL;
	
	public void write() throws ScopeException, ComputingException{
		
	}; 
	
	public void setSource(RedisCacheValue val){
		this.val = val;
	};	

	
	public void setMapper(QueryMapper mapper){
		this.mapper = mapper;
	}
	public void setDatabase(Database db){
		this.db = db;
	}
	
	public void setSQL(String sql){
		this.SQL= sql;
	}
	
}
