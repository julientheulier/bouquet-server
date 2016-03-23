/*******************************************************************************
 * Copyright © Squid Solutions, 2016
 *
 * This file is part of Open Bouquet software.
 *  
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation (version 3 of the License).
 *
 * There is a special FOSS exception to the terms and conditions of the 
 * licenses as they are applied to this program. See LICENSE.txt in
 * the directory of this program distribution.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * Squid Solutions also offers commercial licenses with additional warranties,
 * professional functionalities or services. If you purchase a commercial
 * license, then it supersedes and replaces any other agreement between
 * you and Squid Solutions (above licenses and LICENSE.txt included).
 * See http://www.squidsolutions.com/EnterpriseBouquet/
 *******************************************************************************/
package com.squid.kraken.v4.caching.redis;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheReference;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValue;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;
import com.squid.kraken.v4.caching.redis.generationalkeysserver.RedisKey;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;

public class RedisCacheProxyMock implements IRedisCacheProxy{

	Map<byte[], byte[]> cache = new HashMap<byte[], byte[]>();

	static final Logger logger = LoggerFactory
			.getLogger(RedisCacheProxyMock.class);

	private static RedisCacheProxyMock INSTANCE;

	public static IRedisCacheProxy getInstance(ServerID redisID) {
		if (INSTANCE == null) {
			INSTANCE = new RedisCacheProxyMock(redisID);
			RedisCacheProxy.initInstance(INSTANCE);
		}
		return INSTANCE;

	}

	public static IRedisCacheProxy getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new RedisCacheProxyMock();
			RedisCacheProxy.initInstance(INSTANCE);
		}
		return INSTANCE;
	}

	public RedisCacheProxyMock() {
	}

	public RedisCacheProxyMock(ServerID redisID) {
	}

	public boolean put(String k, String v) {
		return this.put(k.getBytes(), v.getBytes());
	}

	public boolean put(String k, byte[] v) {
		return this.put(k.getBytes(), v);
	}

	public boolean put(String k, RawMatrix v) {
		try {
			return this.put(k.getBytes(), v.serialize());
		} catch (IOException e) {
			return false;
		}
	}

	public boolean put(byte[] k, byte[] v) {
		cache.put(k, v);
		return true;
	}

	public RawMatrix getRawMatrixOld(String key) {
		byte[] serialized = cache.get(key.getBytes());
		try {
			RawMatrix res = RawMatrix.deserialize(serialized);
			res.setRedisKey(key);
			return res;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public RawMatrix getRawMatrix(String key){
		try {	

			HashSet<String> pastKeys=  new HashSet<String>(); // do not get trapped in circular references
			String currKey = key;
			while(true){

				byte[] serialized = cache.get(currKey.getBytes());

				RedisCacheValue  val = RedisCacheValue.deserialize(serialized);
				if (val instanceof RawMatrix){
					RawMatrix res= (RawMatrix) val;
					res.setRedisKey(currKey); 
					return res;
				}else{
					if(val instanceof  RedisCacheReference){	
						RedisCacheReference ref = (RedisCacheReference )val ;
						logger.info("Cache Reference : "+ currKey + "   " +  ref.getReferenceKey());
						pastKeys.add(currKey);
						currKey = ref.getReferenceKey();
						if (pastKeys.contains(currKey)){
							throw new RuntimeException();
						}

					}else{
						if(val instanceof RedisCacheValuesList){
							RedisCacheValuesList  refList = (RedisCacheValuesList) val;
							return  this.getChunkedRawMatrix(key, refList);
						}else{
							throw new ClassNotFoundException();
						}
					}
				}
			}

		} catch (RuntimeException | ClassNotFoundException | IOException | ComputingException e) {
			logger.error("failed to getRawMatrix() on key="+key);
			throw new RuntimeException("Jedis: getRawMatrix() failed on key="+key, e);
		} 
	}



	private RawMatrix getChunkedRawMatrix (String key, RedisCacheValuesList refList ) throws ComputingException, ClassNotFoundException, IOException{
		logger.info("Rebuilding chunked matrix from cache");
		RedisCacheValuesList currRef = refList;
		int nbChunks = 0;
		RawMatrix res = null;
		boolean done = false;
		while (!done) {
			int nbChunksDone = nbChunks;
			for (int i = nbChunksDone; i < currRef.getReferenceKeys().size(); i++) {
				String chunkKey = currRef.getReferenceKeys().get(nbChunks).referencedKey;
				logger.info("chunk key " + chunkKey);
				RawMatrix chunk = this.getRawMatrix(chunkKey);
				res = RawMatrix.mergeMatrices(res, chunk);
				nbChunks++;
			}
			if (currRef.isDone()) {
				done = true;
			} else {
				byte[] serialized = cache.get(key.getBytes());
				RedisCacheValue val = RedisCacheValue.deserialize(serialized);
				currRef = (RedisCacheValuesList) val;
			}
		}
		res.setRedisKey(key);
		return res; 
	}

	public byte[] get(String key) {
		byte[] res = cache.get(key.getBytes());
		return res;
	}

	public boolean inCache(RedisKey k) {
		return this.inCache(k.toString());
	}

	public boolean inCache(String key) {
		boolean res = cache.containsKey(key.getBytes());
		return res;
	}

	public String clear() {
		cache.clear();
		return null;
	}
	
	public void quit(){
	} 

	public void setTTL(String key, int ttl) {
		// jedis.expire(key.getBytes(), ttl);
	}

}
