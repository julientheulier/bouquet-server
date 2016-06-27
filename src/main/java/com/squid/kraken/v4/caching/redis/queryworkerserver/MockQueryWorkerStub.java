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

import java.util.List;

import javax.ws.rs.core.GenericType;

import org.apache.cxf.jaxrs.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.caching.redis.ServerID;

public class MockQueryWorkerStub implements IQueryWorkerServer {

	static final Logger logger = LoggerFactory.getLogger(MockQueryWorkerStub.class);

	private String host;
	private int port;
	private String appName;

	private String baseURL;

	public MockQueryWorkerStub(ServerID self, String appName) {
		this.port = self.port;
		this.host = self.host;
		this.appName = appName;
		logger.info(" new Mock Query Worker Stub " + this.host + " " + this.port);
	}

	@Override
	public int fetch(QueryWorkerJobRequest request) {
		if (logger.isDebugEnabled()) {
			logger.debug((request.getKey()));
		}
		if (logger.isDebugEnabled()) {
			logger.debug((request.getSQLQuery()));
		}
		WebClient client = WebClient.create(baseURL);
		client.path("fetch");
		client.query("request", request);
		int res = client.get(Integer.class);
		return res;
	}


	@Override
	public void start() {
		logger.info(" starting Mock Query Worker Stub" + this.host + " " + this.port);
		this.baseURL = "http://" + this.host + ":" + this.port + "/" + appName + "/cache/queryworker";
		logger.info("base URL " + baseURL);
	}

	@Override
	public String hello() {
		return "Hello Mock Query Worker Stub server";
	}

	@Override
	public int getLoad() {
		return 0;
	}

	@Override
	public boolean isQueryOngoing(String k) {
		if (logger.isDebugEnabled()) {
			logger.debug((k));
		}

		WebClient client = WebClient.create(baseURL);
		client.path("ongoing");
		client.query("key", k);
		boolean res = client.get(Boolean.class);
		return res;
	}
	
	@Override
	public List<QueryWorkerJobStatus> getOngoingQueries(String customerId) {
		WebClient client = WebClient.create(baseURL);
		client.path("queries");
		client.query("customerId", customerId);
		GenericType<List<QueryWorkerJobStatus>> type = new GenericType<List<QueryWorkerJobStatus>>() {};
		List<QueryWorkerJobStatus> res = client.get(type);
		return res;
	}
	
	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.caching.redis.queryworkerserver.IQueryWorkerServer#cancelOngoingQuery(java.lang.String, java.lang.String)
	 */
	@Override
	public boolean cancelOngoingQuery(String customerId, String key) {
		WebClient client = WebClient.create(baseURL);
		client.path("cancel");
		client.query("customerId",  customerId);
		client.query("key",  key);
		boolean res = client.get(Boolean.class);
		return res;
	}

}
