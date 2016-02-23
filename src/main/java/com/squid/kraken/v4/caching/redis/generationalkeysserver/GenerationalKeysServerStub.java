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
 package com.squid.kraken.v4.caching.redis.generationalkeysserver;

import java.util.Collection;

import org.apache.cxf.jaxrs.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.caching.redis.ServerID;

public class GenerationalKeysServerStub implements IGenerationalKeysServer {

    static final Logger logger = LoggerFactory.getLogger(GenerationalKeysServerStub.class);

	private String host;
	private int port;
	private String appName;
	private String baseURL;
		
	public GenerationalKeysServerStub(ServerID id, String appName) {
		this.host=id.host;
		this.port = id.port;
		this.appName=appName;
		logger.info("new Generational Keys Server stub "+ this.host + " " + this.port); 
		
	}
	
	
	@Override
	public RedisKey getKey(String SQLQuery, Collection<String> dependencies) {
		if(logger.isDebugEnabled()){logger.debug(("GenerationalKeysServerStub in getKey"));}
		WebClient client = WebClient.create(this.baseURL);
		client.path("getKey");
		client.query("sqlquery", SQLQuery);
		if (dependencies!=null) {
			for (String s : dependencies) {
				client.query("deps",s);
			}
		}
		String jsonKey = client.get(String.class);
		return RedisKey.fromJson(jsonKey);
	}

	@Override
	public boolean refresh(Collection<String> dependencies) {
		if(logger.isDebugEnabled()){logger.debug(("refres deps "+ dependencies.size()));}
		WebClient client = WebClient.create(this.baseURL);
		client.path("refresh");
		for (String s : dependencies)
			 client.query("deps",s);
	
		boolean res = client.get(Boolean.class);
		return res;	
	}
	

	@Override
	public void start() {
		logger.info("starting Generational Keys Server stub"); 
		this.baseURL = "http://"+this.host+":"+this.port+"/"+ appName+"/cache/generationalkeys";
		logger.info("base URL " + baseURL); 
			
	}

}
