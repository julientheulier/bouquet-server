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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.generationalkeysserver.RedisKey;

public class RedisCacheProxyMock implements IRedisCacheProxy{

	Map<byte[], byte[]> cache = new HashMap<byte[], byte[]>();

	static final Logger logger = LoggerFactory
			.getLogger(RedisCacheProxyMock.class);

	private static RedisCacheProxyMock INSTANCE;

	public static IRedisCacheProxy getInstance(ServerID redisID) {
		if (INSTANCE == null) {
			INSTANCE = new RedisCacheProxyMock(redisID);
		}
		return INSTANCE;

	}

	public static IRedisCacheProxy getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new RedisCacheProxyMock();
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

	public RawMatrix getRawMatrix(String key) {
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
