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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.jdbc.engine.IExecutionItem;
import com.squid.kraken.v4.caching.redis.IRedisCacheProxy;
import com.squid.kraken.v4.caching.redis.RedisCacheConfig;
import com.squid.kraken.v4.caching.redis.RedisCacheException;
import com.squid.kraken.v4.caching.redis.RedisCacheProxy;
import com.squid.kraken.v4.caching.redis.ServerID;
import com.squid.kraken.v4.caching.redis.datastruct.ChunkRef;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrixStreamExecRes;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;
import com.squid.kraken.v4.core.database.impl.ExecuteQueryTask;
import com.squid.kraken.v4.core.database.impl.SimpleDatabaseManager;

public class QueryWorkerServer implements IQueryWorkerServer {

	static final Logger logger = LoggerFactory.getLogger(QueryWorkerServer.class);

	// private static final boolean SPARK_FLAG = new
	// Boolean(KrakenConfig.getProperty("feature.spark", "false"));

	private HashMap<String, SimpleDatabaseManager> managers;

	private String host;
	private int port;

	private int threadPoolSize = 5;
	private AtomicInteger load;
	private ExecutorService executor;

	// private int maxRecords=1024;

	private int maxRecords = -1;

	private int defaultTTLinSec = 3600;

	// redis
	private String REDIS_SERVER_HOST;
	private int REDIS_SERVER_PORT;
	private IRedisCacheProxy redis;

	private ConcurrentHashMap<String, CallableChunkedMatrixFetch> ongoingLongQueries;

	public QueryWorkerServer(RedisCacheConfig conf) {
		this.load = new AtomicInteger(0);
		this.executor = Executors.newFixedThreadPool(threadPoolSize);

		// redis

		this.REDIS_SERVER_HOST = conf.getRedisID().host;
		this.REDIS_SERVER_PORT = conf.getRedisID().port;
		this.managers = new HashMap<String, SimpleDatabaseManager>();
		this.maxRecords = conf.getMaxRecord();
		this.defaultTTLinSec = conf.getTtlInSecond();

		RawMatrix.setMaxChunkSizeInMB(conf.getMaxChunkSizeInMByte());

		this.ongoingLongQueries = new ConcurrentHashMap<String, CallableChunkedMatrixFetch>();

		logger.info("New Query Worker " + this.host + " " + this.port);
	}

	public int getMaxRecords() {
		return maxRecords;
	}

	public void start() {
		logger.info("Starting query worker " + this.host + " " + this.port + " max record " + this.maxRecords);
		redis = RedisCacheProxy.getInstance(new ServerID(this.REDIS_SERVER_HOST, this.REDIS_SERVER_PORT));
	}

	public IRedisCacheProxy getCache() {
		return this.redis;
	}

	@Override
	public String hello() {
		return "Hello Query Worker server";
	}

	@Override
	public int fetch(String k, String SQLQuery, String jobId, String RSjdbcURL, String username, String pwd, int ttl,
			long limit) {
		IExecutionItem item = null;
		String dbKey = username + "\\" + RSjdbcURL + "\\" + pwd;
		try {
			SimpleDatabaseManager db;
			synchronized (managers) {
				db = managers.get(dbKey);
				if (db == null) {
					db = new SimpleDatabaseManager(RSjdbcURL, username, pwd);
					managers.put(dbKey, db);
				}
			}

			ExecuteQueryTask exec = db.createExecuteQueryTask(SQLQuery);
			exec.setWorkerId(this.host + ":" + this.port);
			exec.setJobId(jobId);
			item = exec.call();

			RawMatrixStreamExecRes serializedRes = RawMatrix.streamExecutionItemToByteArray(item, maxRecords, limit);
			logger.info("limit " + limit + ", linesProcessed " + serializedRes.getNbLines() + " hasMore:"
					+ serializedRes.hasMore());
			if (!serializedRes.hasMore()) {
				try {
					logger.info("SQLQuery #" + item.getID() + " jobId " + jobId + " fits in one chunk; queryid="
							+ item.getID());
					if (!put(k, serializedRes.getStreamedMatrix(), ttl)) {
						throw new RedisCacheException("We did not manage to store the result for queryid=#"
								+ item.getID() + "jobId " + jobId + "in redis");
					}
				} finally {
					if (item != null)
						item.close();
				}

			} else {
				logger.info("SQLQuery #" + item.getID() + " jobId " + jobId + " does not fit in one chunk; queryid="
						+ item.getID());
				// store first batch
				String batchKey = k + "_" + 0 + "-" + (serializedRes.getNbLines() - 1);
				if (!put(batchKey, serializedRes.getStreamedMatrix(), ttl)) {
					throw new RedisCacheException("We did not manage to store the result for queryid=" + item.getID()
							+ " jobId " + jobId + " in redis");
				}
				// save the batch list under the main key
				RedisCacheValuesList valuesList = new RedisCacheValuesList();
				valuesList.addReferenceKey(new ChunkRef(batchKey, 0, serializedRes.getNbLines() - 1));
				put(k, valuesList);
				// process the remaining row in a separate thread
				CallableChunkedMatrixFetch chunkedMatrixFetch = new CallableChunkedMatrixFetch(this, k, jobId,
						valuesList, item, ttl, serializedRes.getNbLines(), limit);
				this.executor.submit(chunkedMatrixFetch);
				this.ongoingLongQueries.put(k, chunkedMatrixFetch);
			}
			return item.getID();
		} catch (ExecutionException e) {
			throw new RedisCacheException(e);
		} catch (SQLException | IOException e) {
			throw new RedisCacheException(e);
		}
	}

	@Override
	public int getLoad() {
		return this.load.get();
	}

	protected boolean put(String batchKey, byte[] data, int ttl) {
		boolean ok = redis.put(batchKey, data);
		if (ttl == -2) {
			redis.setTTL(batchKey, defaultTTLinSec);
		} else {
			if (ttl != -1) {
				redis.setTTL(batchKey, ttl);
			}
		}
		return ok;
	}

	protected boolean put(String key, RedisCacheValuesList valuesList) {
		return redis.put(key, valuesList.serialize());
	}

	protected void incrementLoad() {
		load.incrementAndGet();
	}

	public void decrementLoad() {
		load.decrementAndGet();
	}

	public void removeOngoingQuery(String k) {
		this.ongoingLongQueries.remove(k);
	}

	public String getWorkerId() {
		return this.host + ":" + this.port;
	}

	@Override
	public boolean isQueryOngoing(String k) {
		return (this.ongoingLongQueries.containsKey(k));

	}

}