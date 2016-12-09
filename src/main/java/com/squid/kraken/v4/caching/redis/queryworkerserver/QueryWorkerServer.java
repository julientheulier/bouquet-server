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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.squid.kraken.v4.model.ProjectPK;

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

	private int defaultTTLinSec = 3600;

	// redis
	private String REDIS_SERVER_HOST;
	private int REDIS_SERVER_PORT;
	private IRedisCacheProxy redis;

	private Map<String, QueryWorkerJob> executingQueries;
	private Map<String, CallableChunkedMatrixFetch> longRunningQueries;

	public QueryWorkerServer(RedisCacheConfig conf) {
		this.load = new AtomicInteger(0);
		this.executor = Executors.newFixedThreadPool(threadPoolSize);

		// redis

		this.REDIS_SERVER_HOST = conf.getRedisID().host;
		this.REDIS_SERVER_PORT = conf.getRedisID().port;
		this.managers = new HashMap<String, SimpleDatabaseManager>();
		this.defaultTTLinSec = conf.getTtlInSecond();

		RawMatrix.setMaxChunkSizeInMB(conf.getMaxChunkSizeInMByte());

		this.executingQueries = new ConcurrentHashMap<>();
		this.longRunningQueries = new ConcurrentHashMap<>();

		logger.info("New Query Worker " + this.host + " " + this.port);
	}

	public void start() {
		logger.info("Starting query worker " + this.host + " " + this.port);
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
	public int fetch(QueryWorkerJobRequest request) {
		IExecutionItem item = null;
		String dbKey = request.getUsername() + "\\" + request.getJdbcURL() + "\\" + request.getPwd();
		try {
			SimpleDatabaseManager db;
			synchronized (managers) {
				db = managers.get(dbKey);
				if (db == null) {
					db = new SimpleDatabaseManager(request.getJdbcURL(), request.getUsername(), request.getPwd());
					managers.put(dbKey, db);
				}
			}

			ExecuteQueryTask exec = db.createExecuteQueryTask(request.getSQLQuery());
			exec.setWorkerId(this.host + ":" + this.port);
			exec.setJobId(request.getJobId());
			exec.setUserId(request.getUserID());
			executingQueries.put(request.getKey(), new QueryWorkerJob(request, exec));
			item = exec.call();

			long start = System.currentTimeMillis();
			
			RawMatrixStreamExecRes serializedRes = RawMatrix.streamExecutionItemToByteArray(item, request.getLimit());

			long end = System.currentTimeMillis();
			
			logger.debug("limit " + request.getLimit() + ", linesProcessed " + serializedRes.getNbLines() + " hasMore:"
					+ serializedRes.hasMore());
			if (!serializedRes.hasMore()) {
				try {
					logger.info("SQLQuery #" + item.getID() + " jobId " + request.getJobId() + " fits in one chunk; duration="+ (end-start) +"lines=" + (serializedRes.getNbLines() - 1) + "; queryid="
							+ item.getID());
					if (!put(request.getKey(), serializedRes.getStreamedMatrix(), request.getTTL())) {
						throw new RedisCacheException("We did not manage to store the result for queryid=#"
								+ item.getID() + "jobId " + request.getJobId() + "in redis");
					}
				} finally {
					this.executingQueries.remove(request.getKey());
					// in this case the reading is complete, we must close the item
					if (item != null) {
						item.close();
					}
				}

			} else {
				logger.info("SQLQuery #" + item.getID() + " jobId " + request.getJobId() + " does not fit in one chunk; queryid="
						+ item.getID());
				// store first batch
				String batchKey = request.getKey() + "_" + 0 + "-" + (serializedRes.getNbLines() - 1);
				if (!put(batchKey, serializedRes.getStreamedMatrix(), request.getTTL())) {
					throw new RedisCacheException("We did not manage to store the result for queryid=" + item.getID()
							+ " jobId " + request.getJobId() + " in redis");
				}
				// save the batch list under the main key
				RedisCacheValuesList valuesList = new RedisCacheValuesList();
				valuesList.addReferenceKey(new ChunkRef(batchKey, 0, serializedRes.getNbLines() - 1));
				put(request.getKey(), valuesList);
				// process the remaining row in a separate thread
				CallableChunkedMatrixFetch chunkedMatrixFetch = new CallableChunkedMatrixFetch(this, request, valuesList, item, serializedRes.getNbLines(), start);
				this.executor.submit(chunkedMatrixFetch);
				this.executingQueries.remove(request.getKey());
				this.longRunningQueries.put(request.getKey(), chunkedMatrixFetch);
			}
			return item.getID();
		} catch (ExecutionException | SQLException | IOException e) {
			if (item != null) {
				item.close();
			}
			this.executingQueries.remove(request.getKey());
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
		this.longRunningQueries.remove(k);
	}

	public String getWorkerId() {
		return this.host + ":" + this.port;
	}

	@Override
	public boolean isQueryOngoing(String k) {
		return (this.longRunningQueries.containsKey(k));
	}
	
	@Override
	public List<QueryWorkerJobStatus> getOngoingQueries(String customerId) {
		ArrayList<QueryWorkerJobStatus> queries = new ArrayList<>();
		for (QueryWorkerJob job : executingQueries.values()) {
			QueryWorkerJobStatus status = job.getStatus();
			if (status.getProjectPK()!=null && status.getProjectPK().getCustomerId().equals(customerId)) {
				queries.add(status);
			}
		}
		for (CallableChunkedMatrixFetch fetch : longRunningQueries.values()) {
			QueryWorkerJobStatus status = fetch.getStatus();
			if (status.getProjectPK()!=null && status.getProjectPK().getCustomerId().equals(customerId)) {
				queries.add(status);
			}
		}
		return queries;
	}
	
	@Override
	public boolean cancelOngoingQuery(String customerId, String key) {
		// executing ?
		QueryWorkerJob job = executingQueries.get(key);
		if (job!=null) {
			ProjectPK projectPK = job.getStatus().getProjectPK();
			if (projectPK!=null && projectPK.getCustomerId().equals(customerId)) {
				job.cancel();
				return true;
			}
		}
		// reading ?
		CallableChunkedMatrixFetch query = longRunningQueries.get(key);
		if (query!=null) {
			ProjectPK projectPK = query.getStatus().getProjectPK();
			if (projectPK!=null && projectPK.getCustomerId().equals(customerId)) {
				return query.cancel();
			}
		}
		// else
		return false;
	}

}