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
package com.squid.kraken.v4.caching.awsredis.generationalkeysserver;

import java.util.ArrayList;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

public class GenerationalKeysRestService {

	GenerationalKeysServer serv;
	
	public GenerationalKeysRestService (GenerationalKeysServer s){
		this.serv =s;
	}
	static final Logger logger = LoggerFactory.getLogger(GenerationalKeysRestService.class);
	
	
	
	@GET
	@Path("/getKey")
	public synchronized String getKey( @QueryParam("sqlquery") String SQLQuery, @QueryParam("deps") ArrayList<String> dependencies) throws JsonProcessingException{	
		if(logger.isDebugEnabled()){logger.debug((SQLQuery));}
		RedisKey  k= serv.getKey(SQLQuery, dependencies);
		return k.toJson();
	}
	
	@GET
	@Path("/refresh")
	public synchronized boolean refresh(@QueryParam("deps") ArrayList<String> dependencies) {
		return serv.refresh(dependencies);
	}
	
	@GET
	@Path("/hello")
	public synchronized String hello() {
		return serv.hello();
	}	
}	
