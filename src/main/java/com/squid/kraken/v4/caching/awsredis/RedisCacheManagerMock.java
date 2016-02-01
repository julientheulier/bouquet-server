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
package com.squid.kraken.v4.caching.awsredis;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.caching.awsredis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.awsredis.datastruct.TripletMapping;
import com.squid.kraken.v4.caching.awsredis.generationalkeysserver.GenerationalKeysServerMock;
import com.squid.kraken.v4.caching.awsredis.generationalkeysserver.IGenerationalKeysServer;
import com.squid.kraken.v4.caching.awsredis.generationalkeysserver.RedisKey;
import com.squid.kraken.v4.caching.awsredis.queriesserver.IQueriesServer;

public class RedisCacheManagerMock implements IRedisCacheManager  {

    static final Logger logger = LoggerFactory.getLogger(RedisCacheManagerMock.class);

    private static IRedisCacheManager INSTANCE;

    private IRedisCacheProxy redis;
	private AWSRedisCacheConfig conf;
	private IQueriesServer queriesServ;
	private IGenerationalKeysServer genkeysServ ;
	//private ArrayList<ServerID> queriesWorkers;
	
	private HashSet<TripletMapping> mappings ;
	
	//constructors
	
	public RedisCacheManagerMock(){	
	}

	public static IRedisCacheManager getInstance(){
		if (INSTANCE == null) {
			INSTANCE = new RedisCacheManagerMock();
		}
		return INSTANCE;
	}
	
	public void setConfig(AWSRedisCacheConfig confCache){
		this.conf=confCache;
	}
		
	public void startCacheManager(){
		if (this.genkeysServ == null) {
			logger.info("starting cache manager");
	
			this.genkeysServ= new GenerationalKeysServerMock();
			//this.queriesServ = QueriesServerFactory.INSTANCE.getNewQueriesServer(conf,false);
			this.mappings = new HashSet<TripletMapping>();
			
			this.genkeysServ.start();
			//this.queriesServ.start();
			this.redis =  RedisCacheProxyMock.getInstance();
		}
	}
	
	
	public RawMatrix getData(String SQLQuery, List<String> dependencies, String RSjdbcURL,
		String username, String pwd, int TTLinSec, long limit) throws InterruptedException{
		//
		// generate the key by adding projectID and SQL
		String key = "";
		if (dependencies.size()>0) {
			key += dependencies.get(0);
		}
		key += "-" + DigestUtils.sha256Hex(SQLQuery);
		//
		RedisKey rk = getKey(key, dependencies);
		String k = rk.getStringKey();
		boolean inCache = this.inCache(k);
		logger.info("cache hit = " + inCache + " for key = " + k);
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
	
	@Override
	public RedisKey getKey(RedisKey key) {
		return  this.genkeysServ.getKey(key.getName(), key.getDepGen().keySet());
	}
	
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
	
	
	public boolean addTripletMapping(TripletMapping t){
		return this.mappings.add(t);
	}
	
	public HashSet<TripletMapping> getMappings(){
		return this.mappings;
	}

	@Override
	public RawMatrix getDataLazy(String SQLQuery, List<String> dependencies,
			String RSjdbcURL, String username, String pwd, int TTLinSec)
			throws InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean addCacheReference(String sqlNoLimit,
			List<String> dependencies, String referencedKey) {
		// TODO Auto-generated method stub
		return false;
	}
	
}
