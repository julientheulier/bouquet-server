package com.squid.kraken.v4.caching.redis.datastruct;

public class ChunkRef {

	public String referencedKey ; 
	
	public long upperBound;
	
	public long lowerBound;
	
	public ChunkRef(String ref, long lower, long upper){
		this.referencedKey = ref;
		this.upperBound = upper;
		this.lowerBound = lower;
	}
	
}
