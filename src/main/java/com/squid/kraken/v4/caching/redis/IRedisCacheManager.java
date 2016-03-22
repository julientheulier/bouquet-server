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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.generationalkeysserver.RedisKey;

public interface IRedisCacheManager {

	public void setConfig(AWSRedisCacheConfig confCache);

	public void startCacheManager();

	public RawMatrix getData(String SQLQuery, List<String> dependencies,
			String RSjdbcURL, String username, String pwd, int TTLinSec, long limit)
			throws InterruptedException ;
	
	public RawMatrix getDataLazy(String SQLQuery, List<String> dependencies,
			String RSjdbcURL, String username, String pwd, int TTLinSec)
			throws InterruptedException;

	public void clear();

	public void refresh(String... dependencies);

	public void refresh(List<String> dependencies);

	public void refresh(String key);

	public RedisKey getKey(String key);

	public RedisKey getKey(String key, Collection<String> dependencies);

	public RedisKey getKey(String key, String... dependencies);
	
	public RedisKey getKey(RedisKey key);
	
	public boolean isValid(RedisKey key);

	public boolean inCache(RedisKey key);

	public boolean inCache(String key);

	public RawMatrix getRawMatrix(String k);

	
	public boolean addCacheReference(String sqlNoLimit, List<String> dependencies, String referencedKey );


}