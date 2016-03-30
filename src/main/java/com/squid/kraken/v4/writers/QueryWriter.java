package com.squid.kraken.v4.writers;

import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValue;

public class QueryWriter {

	public  ISourceWriter writer ;	
	public RedisCacheValue val;
	public String key ;
	
	
	
	
	public void write(){
		
	}; 
	
	public void setSource(RedisCacheValue val){
		this.val = val;
	};	

	public void setKey(String key){
		this.key = key;
	}
}
