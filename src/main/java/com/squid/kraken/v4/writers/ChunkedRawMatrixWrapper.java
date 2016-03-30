package com.squid.kraken.v4.writers;

import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;

public class ChunkedRawMatrixWrapper {

	  public String key;
	  public RedisCacheValuesList refList;
	
	  
	  public ChunkedRawMatrixWrapper(String key, RedisCacheValuesList refList){
		this.key = key;
		this.refList = refList;
		  
	  };
	  
	  
}
