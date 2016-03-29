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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheReference;
import com.squid.kraken.v4.caching.redis.generationalkeysserver.GenerationalKeysServerFactory;
import com.squid.kraken.v4.caching.redis.generationalkeysserver.IGenerationalKeysServer;
import com.squid.kraken.v4.caching.redis.generationalkeysserver.RedisKey;
import com.squid.kraken.v4.caching.redis.queriesserver.IQueriesServer;
import com.squid.kraken.v4.caching.redis.queriesserver.QueriesServerFactory;

public class RedisCacheManager implements IRedisCacheManager  {

    static final Logger logger = LoggerFactory.getLogger(RedisCacheManager.class);

    private static IRedisCacheManager INSTANCE;
    
    private static boolean isMock = false;

    private IRedisCacheProxy redis;
	private AWSRedisCacheConfig conf;
	private IQueriesServer queriesServ;
	private IGenerationalKeysServer genkeysServ ;
	
	//constructors
	
	public RedisCacheManager(){	
	}
	
	public static void setMock(){
		isMock = true;
	}

	public static IRedisCacheManager getInstance(){
		if (INSTANCE == null) {
			if (isMock) {
				INSTANCE = new RedisCacheManagerMock();
			} else {
				INSTANCE = new RedisCacheManager();
			}
		}
		return INSTANCE;
	}
	
	public void setConfig(AWSRedisCacheConfig confCache){
		this.conf=confCache;
	}
		
	public void startCacheManager(){
		logger.info("starting cache manager");
		
		this.genkeysServ= GenerationalKeysServerFactory.INSTANCE.getNewGenerationalKeysServer(conf, false); 
		this.queriesServ = QueriesServerFactory.INSTANCE.getNewQueriesServer(conf,false);
		
		this.genkeysServ.start();
		this.queriesServ.start();
		
		this.redis =  RedisCacheProxy.getInstance(conf.getRedisID());
		
	}
	
	
	public RawMatrix getData(String SQLQuery, List<String> dependencies, String RSjdbcURL,
		String username, String pwd, int TTLinSec, long limit) throws InterruptedException{
		//
		// generate the key by adding projectID and SQL
		String k = buildCacheKey(SQLQuery, dependencies);
		boolean inCache = this.inCache(k);
		logger.debug("cache hit = " + inCache + " for key = " + k);
		if (!inCache){
			boolean fetchOK = this.fetch(k, SQLQuery, RSjdbcURL, username, pwd,TTLinSec, limit );
			if (!fetchOK){
				logger.info("failed to fetch query:\n" +SQLQuery  +"\nfetch failed") ;
				return null;
			}
		}
		RawMatrix res = getRawMatrix(k);
		res.setFromCache(inCache);
		return res;
	}
	
	
	public RawMatrix  getDataLazy(String SQLQuery, List<String> dependencies, String RSjdbcURL,
			String username, String pwd, int TTLinSec){
		String k = buildCacheKey(SQLQuery, dependencies);
		boolean inCache = this.inCache(k);
		logger.debug("cache hit = " + inCache + " for key = " + k);
		if (!inCache){
			return null;
		}else{
			RawMatrix res = getRawMatrix(k);
			res.setFromCache(inCache);
			return res;
		}
	}
	
	public boolean addCacheReference(String sqlNoLimit, List<String> dependencies, String referencedKey ){
		try{
			String k = buildCacheKey(sqlNoLimit, dependencies);
			logger.info("Add reference key : " +  k + "    " + referencedKey);
			RedisCacheReference ref = new RedisCacheReference(referencedKey);		
			return this.redis.put(k, ref.serialize());
		}catch(IOException e){
			return false;
		}
	}
	
	
	private String buildCacheKey(String SQLQuery, List<String> dependencies){
		String key = "";
		if (dependencies.size()>0) {
			key += dependencies.get(0);
		}
		key += "-" + DigestUtils.sha256Hex(SQLQuery);
		//
		RedisKey rk = getKey(key, dependencies);
		return rk.getStringKey();
		
	}
	
	public void clear(){
		logger.info("Clearing SQL cache");
		this.redis.clear();
	}
    
    public void refresh(String... dependencies) {
        this.genkeysServ.refresh(Arrays.asList(dependencies));     
    }
	
	public void refresh(List<String> dependencies){
		this.genkeysServ.refresh(dependencies);		
	}
    
    public void refresh(String key){
        this.genkeysServ.refresh(Collections.singletonList(key));
    }
	
	public RedisKey getKey(String key) {
		return  this.genkeysServ.getKey(key, null);
	}
	
	public RedisKey getKey(String key, Collection<String> dependencies) {
		return  this.genkeysServ.getKey(key, dependencies);
	}
	
	public RedisKey getKey(String key, String... dependencies) {
		return  this.genkeysServ.getKey(key, Arrays.asList(dependencies));
	}
	
	/**
	 * get a possibly update RedisKey for this key
	 * @param key
	 * @return
	 */
	public RedisKey getKey(RedisKey key) {
		return  this.genkeysServ.getKey(key.getName(), key.getDepGen().keySet());
	}
	
	/**
	 * check if the key is still valid
	 * @param key
	 * @return
	 */
	public boolean isValid(RedisKey key) {
		RedisKey check = getKey(key);
		return check.getVersion()==key.getVersion() && check.getUniqueID()==key.getUniqueID();
	}

	public boolean inCache(RedisKey key){
		return  this.redis.inCache(key) ;
	}


	public boolean inCache(String key){
		return  this.redis.inCache(key) ;
	}

	private boolean fetch(String k, String SQLQuery, String RSjdbcURL, String username, String pwd, int ttl, long limit) throws InterruptedException{
		return this.queriesServ.fetch(k, SQLQuery, RSjdbcURL,username, pwd, ttl, limit);
	}
	
	public RawMatrix getRawMatrix(String k){
		RawMatrix r =this.redis.getRawMatrix(k);
		return r;
	}
	
}
