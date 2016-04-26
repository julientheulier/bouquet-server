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

import org.apache.cxf.jaxrs.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.caching.redis.ServerID;

public class QueryWorkerServerStub implements IQueryWorkerServer {


    static final Logger logger = LoggerFactory.getLogger(QueryWorkerServerStub.class);

	private String host;
	private int port;
	
	private String appName;
	private String baseURL;
	
	
	public QueryWorkerServerStub(ServerID self, String appName){
		this.port = self.port;
		this.host= self.host;		
		this.appName=appName;
		logger.info("new Query worker stub "+ this.host +" "+ this.port);
	}
	
	@Override
	public boolean fetch(String k, String SQLQuery, String RSjdbcURL, String username, String pwd, int ttl, long limit) {

		WebClient client = WebClient.create(baseURL);
		client.path("fetch");
		client.query("sqlquery", SQLQuery);
		client.query("key", k);
		client.query("jdbc", RSjdbcURL);
		client.query("pwd", pwd);
		client.query("user",username);
		client.query("ttl", ttl);
		client.query("limit", limit);
		
		boolean res  = client.get(Boolean.class);
		return res;
	}
	
	@Override
	public void start(){
		logger.info("starting Query Worker stub " + this.host +" "+ this.port);
		 this.baseURL ="http://"+this.host+":"+this.port+"/"+appName+ "/cache/queryworker";
		logger.info("base URL " + baseURL);
	}
	@Override
	public String hello(){
		return "Hello Query Worker Stub server";
	}

	@Override
	public int getLoad() {
		WebClient client = WebClient.create(baseURL);
		client.path("load");
		
		int res  = client.get(Integer.class);
		return res;
	}

	@Override
	public boolean isQueryOngoing(String k, String SQLQuery) {
		WebClient client = WebClient.create(baseURL);
		client.path("ongoing");
		client.query("sqlquery", SQLQuery);
		client.query("key", k);
		boolean res  = client.get(Boolean.class);
		return res;
	}
	
}
