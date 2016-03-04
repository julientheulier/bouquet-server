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
package com.squid.kraken.v4.caching.redis.queryworkerserver;


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
import com.squid.kraken.v4.caching.redis.RedisCacheManager;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrixStreamExecRes;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;
import com.squid.kraken.v4.caching.redis.AWSRedisCacheConfig;
import com.squid.kraken.v4.caching.redis.IRedisCacheProxy;
import com.squid.kraken.v4.caching.redis.RedisCacheException;
import com.squid.kraken.v4.caching.redis.RedisCacheProxy;
import com.squid.kraken.v4.caching.redis.ServerID;
import com.squid.kraken.v4.caching.redis.SimpleDatabaseManager;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;

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
		try
		{
			IExecutionItem item = null;
			boolean ok;
			boolean isDone =false;
			long linesProcessed = 0;
			String key = username +"\\" + RSjdbcURL + "\\" +pwd;
		
			try	
			{
				SimpleDatabaseManager db;
				synchronized(managers){
					db = managers.get(key);
					if (db == null ){
						db = new SimpleDatabaseManager(RSjdbcURL, username, pwd);
						managers.put(key, db);
					}
				}

				boolean firstCall = true;
				item = db.executeQuery(SQLQuery);
				isDone = false;

				RedisCacheValuesList valuesList  = new RedisCacheValuesList();
				int batchLowerBound = 0 ;
				int batchUpperBound = 0 ;
				int nbBatches = 0;
				long nbLinesLeftToRead = limit;
				while (!isDone){

					RawMatrixStreamExecRes serializedRes = RawMatrix.streamExecutionItemToByteArray(item, maxRecords, nbLinesLeftToRead);
					linesProcessed += serializedRes.getNbLines();
					nbLinesLeftToRead -= linesProcessed;
					
					logger.info("limit " + limit + ", linesProcessed " + linesProcessed + " hasMore:"+serializedRes.hasMore() );
//					if ( ((limit>-1) && (linesProcessed >= limit)) || ((limit ==-1) && (!serializedRes.hasMore())) ){
					if ((!serializedRes.hasMore())){
						//(limit with enough lines) or (export and end of dataset reached) 
						isDone =true;
					}else{
						isDone = false ;
					}

					if (isDone){
							logger.info("The whole result set fits in one Redis chunk") ;
							ok = redis.put(k, serializedRes.getStreamedMatrix()) ;
							if (ttl == -2){
								redis.setTTL(k, this.defaultTTLinSec);
							}else{
								if (ttl  != -1){
									redis.setTTL(k, ttl);		
								}
							}
							if (!ok) {
								throw new RedisCacheException("We did not manage to store the result for query " + SQLQuery + "in redis");
							}
					}else{
						logger.info("The whole result set won't fit in one Redis chunk");	
						this.fetchChunkedMatrix(k, item, serializedRes, ttl, SQLQuery, limit);
					}
				/*	batchLowerBound = batchUpperBound;
					batchUpperBound = batchLowerBound+serializedRes.getNbLines();

					String batchKey = k+"_"+batchLowerBound + "-" + batchUpperBound;
					ok  = redis.put(batchKey, serializedRes.getStreamedMatrix());
					if (ttl == -2){
						redis.setTTL(batchKey, this.defaultTTLinSec);
					}else{
						if (ttl  != -1){
							redis.setTTL(batchKey, ttl);		
						}
					}
					if (!ok) {
						throw new RedisCacheException("We did not manage to store the result for query " + SQLQuery + "in redis");
					}
					valuesList.addReferenceKey(batchKey);
					valuesList.setDone(isDone);
					
					this.redis.put(k, valuesList.serialize());	
					nbBatches+=1;
					if (isDone){
						logger.info("The  result set was split into " + nbBatches + " Redis chunks") ;
						logger.info("Chunks : " +  valuesList.toString() ) ;

					} */
				}

				return true ;
			} catch (ExecutionException e){
				throw new RedisCacheException("Database Service exception for " + RSjdbcURL + " while executing the Query: "+e.getLocalizedMessage(), e) ;
			} catch( SQLException | IOException e) {
				throw new RedisCacheException("Database Service exception for " + RSjdbcURL + " while reading the Query results: "+e.getLocalizedMessage(), e) ;
			}
			finally{
				if (item!=null)
					item.close();
			}
		}catch(SQLException e){
			throw new RedisCacheException("Database Service exception for " + RSjdbcURL + " while reading the Query results: "+e.getLocalizedMessage(), e) ;
		}
	}
	
	
	private void fetchChunkedMatrix( String key, IExecutionItem item,  RawMatrixStreamExecRes serializedFirstChunk,  int ttl, String SQLQuery, long limit){
		int nbBatches = 1;		
		long batchLowerBound = 0;
		long batchUpperBound =0 ;
		boolean done = false; 
		boolean ok;
		RawMatrixStreamExecRes  nextRes = serializedFirstChunk;
		RedisCacheValuesList valuesList  = new RedisCacheValuesList();
		long nbLinesLeftToRead  = limit  ;
		boolean error= false;
		do{
			nbLinesLeftToRead -= nextRes.getNbLines();
			batchLowerBound  = batchUpperBound;
			batchUpperBound= batchLowerBound+ nextRes.getNbLines();
			String batchKey = key+"_"+batchLowerBound + "-" + batchUpperBound;
			ok  = redis.put(batchKey, nextRes.getStreamedMatrix());
			if (ttl == -2){
				redis.setTTL(batchKey, this.defaultTTLinSec);
			}else{
				if (ttl  != -1){
					redis.setTTL(batchKey, ttl);		
				}
			}
			if (!ok) {
				throw new RedisCacheException("We did not manage to store the result for query " + SQLQuery + "in redis");
			}
			valuesList.addReferenceKey(batchKey);
			if (!nextRes.hasMore()){
				valuesList.setDone();
				ok = true;
			}else{
				try {
					nextRes = RawMatrix.streamExecutionItemToByteArray(item, maxRecords, nbLinesLeftToRead) ;
				} catch (IOException |SQLException  e) {
					error = true;
				}
			}
			if (error){
				valuesList.setError();
			}
			
			this.redis.put(key, valuesList.serialize());
			nbBatches+=1;
		
		}while(!done && !error);
		
		if (ttl == -2){
			redis.setTTL(key, this.defaultTTLinSec);
		}else{
			if (ttl  != -1){
				redis.setTTL(key, ttl);		
			}
		}
		
	}

	@Override
	public int getLoad() {
		// TODO Auto-generated method stub
		return 0;
	}
	
}