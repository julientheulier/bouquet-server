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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.caching.redis.IRedisCacheProxy;
import com.squid.kraken.v4.caching.redis.RedisCacheProxy;
import com.squid.kraken.v4.caching.redis.ServerID;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RawRow;

/*
 * This mock query worker does contact not redshift
 * but rather returns a String generated using its ID + a counter 
 */

public class MockQueryWorker implements IQueryWorkerServer {

	static final Logger logger = LoggerFactory.getLogger(MockQueryWorker.class);

	// redis
	public String REDIS_SERVER_HOST;
	public int REDIS_SERVER_PORT;

	// self
	public String host = "localhost";
	public int port = -1;

	private IRedisCacheProxy redis;
	private int repCounter;

	public MockQueryWorker(ServerID self, ServerID redisID) {
		this.host = self.host;
		this.port = self.port;
		this.repCounter = 0;
		this.REDIS_SERVER_HOST = redisID.host;
		this.REDIS_SERVER_PORT = redisID.port;
		logger.info(" new Mock Worker " + this.host + " " + this.port);
	}

	public void start() {
		logger.info("starting Mock Worker " + this.host + " " + this.port);
		redis = RedisCacheProxy.getInstance(new ServerID(this.REDIS_SERVER_HOST, this.REDIS_SERVER_PORT));

	}

	@Override
	public int fetch(QueryWorkerJobRequest request) {
		RawMatrix res = new RawMatrix();
		int id = -1;
		synchronized (this) {
			id = this.repCounter;
			this.repCounter += 1;
			Object[] r = new String[1];
			r[0] = this.host + ":" + this.port + "/mockData-" + id;
			res.addRow(new RawRow(r));
			ArrayList<Integer> types = new ArrayList<Integer>();
			types.add(1);
			res.setColTypes(types);
			ArrayList<String> names = new ArrayList<String>();
			names.add("bla");
			res.setColNames(names);
			res.setMoreData(false);
			res.setExecutionDate(new Date());
		}
		try {
			java.lang.Thread.sleep(10 * 1000);
		} catch (InterruptedException e) {
			return -1;
		}
		boolean ok = redis.put(request.getKey(), res);
		if (ok && request.getTTL() > 0)
			redis.setTTL(request.getKey(), request.getTTL());
		return id;
	}

	@Override
	public String hello() {
		return "Hello Mock Query Worker server";
	}

	@Override
	public int getLoad() {
		return 0;
	}

	@Override
	public boolean isQueryOngoing(String k) {
		// TODO Auto-generated method stub
		return false;
	}
	
	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.caching.redis.queryworkerserver.IQueryWorkerServer#getOngoingQueries()
	 */
	@Override
	public List<QueryWorkerJobStatus> getOngoingQueries(String customerId) {
		return Collections.emptyList();
	}
	
	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.caching.redis.queryworkerserver.IQueryWorkerServer#cancelOngoingQuery(java.lang.String, java.lang.String)
	 */
	@Override
	public boolean cancelOngoingQuery(String customerId, String key) {
		// TODO Auto-generated method stub
		return false;
	}

}
