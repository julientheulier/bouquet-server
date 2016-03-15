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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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
import com.squid.kraken.v4.caching.redis.datastruct.ChunkRef;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;

public class QueryWorkerServer implements IQueryWorkerServer {

	static final Logger logger = LoggerFactory.getLogger(QueryWorkerServer.class);

	//private static final boolean SPARK_FLAG = new Boolean(KrakenConfig.getProperty("feature.spark", "false"));


	private HashMap<String, SimpleDatabaseManager> managers ;

	private String host;
	private int port;

	private int threadPoolSize = 5;
	private AtomicInteger load;
	private ExecutorService executor;

	//	private int maxRecords=1024;

	private int maxRecords=-1;

	private int defaultTTLinSec = 3600;

	//redis
	private String REDIS_SERVER_HOST;
	private  int REDIS_SERVER_PORT;
	private IRedisCacheProxy redis ;




	public QueryWorkerServer(AWSRedisCacheConfig conf) {
		this.load = new AtomicInteger(0);
		this.executor = Executors.newFixedThreadPool(threadPoolSize);

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
		IExecutionItem item = null;
		boolean ok;
		String dbKey = username +"\\" + RSjdbcURL + "\\" +pwd;
		try	
		{
			SimpleDatabaseManager db;
			synchronized(managers){
				db = managers.get(dbKey);
				if (db == null ){
					db = new SimpleDatabaseManager(RSjdbcURL, username, pwd);
					managers.put(dbKey, db);
				}
			}

			item = db.executeQuery(SQLQuery);

			RawMatrixStreamExecRes serializedRes = RawMatrix.streamExecutionItemToByteArray(item, maxRecords, limit);				

			logger.info("limit " + limit + ", linesProcessed " + serializedRes.getNbLines() + " hasMore:"+serializedRes.hasMore() );
			if (!serializedRes.hasMore()){		
				try{
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
				}finally{
					if (item!=null)
						item.close();
				}

			}else{
				logger.info("The whole result set won't fit in one Redis chunk");	

				// store first batch 
				String batchKey = k+"_"+0 + "-" + (serializedRes.getNbLines()-1);
				ok  = redis.put(batchKey, serializedRes.getStreamedMatrix());
				if (ttl == -2){
					redis.setTTL(batchKey, defaultTTLinSec);
				}else{
					if (ttl  != -1){
						redis.setTTL(batchKey, ttl);		
					}
				}
				if (!ok) {
					throw new RedisCacheException("We did not manage to store the result for query " + SQLQuery + "in redis");
				}
				// save the batch list under the main key
				RedisCacheValuesList valuesList  = new RedisCacheValuesList();
				valuesList.addReferenceKey( new ChunkRef(batchKey, 0,serializedRes.getNbLines()-1 ));
				ok  = redis.put(k, valuesList.serialize());

				if (ttl == -2){
					redis.setTTL(k, defaultTTLinSec);
				}else{
					if (ttl  != -1){
						redis.setTTL(k, ttl);		
					}
				}

				// process the remaining row in a separate thread
				CallableChunkedMatrixFetch chunkedMatrixFetch =  new CallableChunkedMatrixFetch(k, valuesList, item, ttl, SQLQuery, serializedRes.getNbLines(), limit);
				this.executor.submit(chunkedMatrixFetch);
			}
			return true ;
		} catch (ExecutionException e){
			throw new RedisCacheException("Database Service exception for " + RSjdbcURL + " while executing the Query: "+e.getLocalizedMessage(), e) ;
		} catch( SQLException | IOException e) {
			throw new RedisCacheException("Database Service exception for " + RSjdbcURL + " while reading the Query results: "+e.getLocalizedMessage(), e) ;
		}
	}


	@Override
	public int getLoad() {
		return this.load.get();
	}

	public class CallableChunkedMatrixFetch implements Callable<Boolean> {

		private String key ;
		private IExecutionItem item;
		private int ttl;
		private String SQLQuery;
		private long nbLinesLeftToRead;
		private int nbBatches ;		
		private long batchLowerBound;
		private	long batchUpperBound;
		private RedisCacheValuesList  valuesList;


		public CallableChunkedMatrixFetch(String key, RedisCacheValuesList valuesList, IExecutionItem item, int ttl, String SQLQuery,long nbLinesRead, long limit){
			this.key = key;
			this.item= item;
			this.ttl =  ttl;
			this.SQLQuery = SQLQuery;
			this.nbLinesLeftToRead = limit - nbLinesRead	;
			this.batchLowerBound=0 ;
			this.batchUpperBound = nbLinesRead;
			this.valuesList  = valuesList;
			this.nbBatches=1;
		}

		@Override
		public Boolean call( ) throws SQLException{
			boolean done = false; 
			boolean ok;
			RawMatrixStreamExecRes  nextBatch = null;
			boolean error= false;
			try{
				load.incrementAndGet();

				do{					
					try {
						nextBatch = RawMatrix.streamExecutionItemToByteArray(item, maxRecords, nbLinesLeftToRead) ;
					} catch (IOException |SQLException  e) {
						error = true;
					}
					if (error){
						valuesList.setError();
					}else{					
						nbLinesLeftToRead -= nextBatch.getNbLines();
						batchLowerBound  = batchUpperBound;
						batchUpperBound= batchLowerBound+ nextBatch.getNbLines();
						String batchKey = key+"_"+batchLowerBound + "-" + (batchUpperBound -1);
						ok  = redis.put(batchKey, nextBatch.getStreamedMatrix());
						if (ttl == -2){
							redis.setTTL(batchKey, defaultTTLinSec);
						}else{
							if (ttl  != -1){
								redis.setTTL(batchKey, ttl);		
							}
						}
						valuesList.addReferenceKey(new ChunkRef(batchKey, batchLowerBound, batchUpperBound));
						if (!ok) {
							valuesList.setError();
							error = true;
						}
					}

					if (!nextBatch.hasMore()){
						valuesList.setDone();
						done = true;
					}

					redis.put(key, valuesList.serialize());
					this.nbBatches+=1;

				}while(!done && !error);

				if (error){
					throw new RedisCacheException("We did not manage to store the result for query " + SQLQuery + "in redis");
				}else{
					logger.debug("Result for  " + SQLQuery + "was split into "+ nbBatches +" batches");
				}
				return true;
			}finally{
				load.decrementAndGet();
				if (item!=null)
					item.close();

			}		
		}

	}

}