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
package com.squid.kraken.v4.caching.awsredis.queryworkerserver;


import java.io.IOException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.core.database.impl.DatabaseServiceImpl;
import com.squid.core.jdbc.engine.IExecutionItem;
import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.caching.awsredis.AWSRedisCacheConfig;
import com.squid.kraken.v4.caching.awsredis.IRedisCacheProxy;
import com.squid.kraken.v4.caching.awsredis.RedisCacheException;
import com.squid.kraken.v4.caching.awsredis.RedisCacheProxy;
import com.squid.kraken.v4.caching.awsredis.ServerID;
import com.squid.kraken.v4.caching.awsredis.SimpleDatabaseManager;
import com.squid.kraken.v4.caching.awsredis.datastruct.RawMatrix;

public class QueryWorkerServer implements IQueryWorkerServer {

    static final Logger logger = LoggerFactory.getLogger(QueryWorkerServer.class);
    
	//private static final boolean SPARK_FLAG = new Boolean(KrakenConfig.getProperty("feature.spark", "false"));

    
    private HashMap<String, SimpleDatabaseManager> managers ;
	
    private String host;
	private int port;
	

//	private int maxRecords=1024;

	private int maxRecords=-1;

	private int defaultTTLinSec = 3600;
	
	//redis
	private String REDIS_SERVER_HOST;
	private  int REDIS_SERVER_PORT;
	private IRedisCacheProxy redis ;
		

	public QueryWorkerServer(AWSRedisCacheConfig conf) {
		//redis
		this.REDIS_SERVER_HOST = conf.getRedisID().host;
		this.REDIS_SERVER_PORT = conf.getRedisID().port;
		this.managers = new HashMap<String, SimpleDatabaseManager>();
		this.maxRecords = conf.getMaxRecord();
		this.defaultTTLinSec = conf.getTtlInSecond();
		logger.info("New Query Worker "+ this.host + " "+ this.port) ;
	}

	public void start(){
		logger.info("Starting query worker "+ this.host + " "+ this.port + " max record " + this.maxRecords);
		// load the drivers
		DatabaseServiceImpl.initDriver();
		Enumeration<Driver> availableDriver = DriverManager.getDrivers();

		redis =  RedisCacheProxy.getInstance(new ServerID(this.REDIS_SERVER_HOST, this.REDIS_SERVER_PORT));
		//todo jdbc connect to RedShift
	}

	public IRedisCacheProxy getCache(){
		return this.redis;
	}
	
	@Override
	public String hello(){
		return "Hello Query Worker server";
	}
	
	@Override
	public boolean fetch(String k, String SQLQuery, String RSjdbcURL, String username, String pwd, int ttl, long limit){
	    IExecutionItem res;
		byte[] serialized  = new byte[1] ;
		try {
			String key = username +"\\" + RSjdbcURL + "\\" +pwd;
			SimpleDatabaseManager db;
			synchronized(managers){
				db = managers.get(key);
				if (db == null ){
					db = new SimpleDatabaseManager(RSjdbcURL, username, pwd);
					managers.put(key, db);
				}
			}
			res = db.executeQuery(SQLQuery);
			serialized = RawMatrix.streamExecutionItemToByteArray(res, maxRecords, limit);

		} catch (ExecutionException e){
			throw new RedisCacheException("Database Service exception for " + RSjdbcURL + " while executing the Query: "+e.getLocalizedMessage(), e) ;
		} catch( SQLException | IOException e) {
			throw new RedisCacheException("Database Service exception for " + RSjdbcURL + " while reading the Query results: "+e.getLocalizedMessage(), e) ;
		}
		boolean ok = redis.put(k, serialized) ;

		/* ttl = -1=> no ttl
		 * ttl = -2 => default ttl
		 * else set ttl
		 */
		if (ttl == -2)
			redis.setTTL(k, this.defaultTTLinSec);
		else
			if (ttl  != -1)
				redis.setTTL(k, ttl);			
		
		return ok ;
	}
}
