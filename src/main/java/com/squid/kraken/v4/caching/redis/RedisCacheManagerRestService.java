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
package com.squid.kraken.v4.caching.redis;

import java.util.ArrayList;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import com.squid.kraken.v4.model.ProjectPK;

public class RedisCacheManagerRestService {

	public RedisCacheManagerRestService(){	
	}
	
	@GET
	@Path("/clear")
	public void clear(){
		RedisCacheManager.getInstance().clear();
	}
	
	@GET
	@Path("/refresh")
	public void refresh(@QueryParam("deps") ArrayList<String> dependencies){
		RedisCacheManager.getInstance().refresh(dependencies);	
	}
	
	@GET
	@Path("/getData")
	public Object getData(
			@QueryParam("user") String userID,
			@QueryParam("login") String login,
			@QueryParam("project") ProjectPK projectPK,
			@QueryParam("sqlquery") String SQLQuery, 
			@QueryParam("deps") ArrayList<String> dependencies, 
			@QueryParam("jobid") String jobId, 
			@QueryParam("jdbc") String RSjdbcURL, 
			@QueryParam("user") String username, 
			@QueryParam("pwd") String pwd,
			@QueryParam("ttl") int TTLinSec,
			@QueryParam("limit") long limit) throws InterruptedException{
		return RedisCacheManager.getInstance().getData(userID, login, projectPK, SQLQuery, dependencies, jobId, RSjdbcURL, username, pwd,
				TTLinSec, limit);
	}
}
