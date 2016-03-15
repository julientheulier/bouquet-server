package com.squid.kraken.v4.caching.redis.datastruct;

public class ChunkRef {
	
	
	public String referencedKey;
	public long lowerBound;
	public long upperBound;

	public ChunkRef(String ref, long lower, long upper){
		this.referencedKey = ref;
		this.lowerBound=lower;
		this.upperBound=upper;
	}
}
