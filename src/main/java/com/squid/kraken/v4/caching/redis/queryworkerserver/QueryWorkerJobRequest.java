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

import com.squid.kraken.v4.model.ProjectPK;

/**
 * A POJO that defines a query worker job
 * @author sergefantino
 *
 */
public class QueryWorkerJobRequest {
	
	// provide a simple way to associate the job to user, required for job management
	private String userID;
	private String login;//for logging
	private ProjectPK projectPK;
	
	private String key;
	private String SQLQuery;
	
	private String jobId;
	private String jdbcURL;
	private String username;
	private String pwd;

	private int ttl;
	private long limit;
	
	public QueryWorkerJobRequest(String userID, String login, ProjectPK projectPK, String key, String sQLQuery, String jobId,
			String jdbcURL, String username, String pwd, int ttl, long limit) {
		super();
		this.userID = userID;
		this.login = login;
		this.projectPK = projectPK;
		this.key = key;
		SQLQuery = sQLQuery;
		this.jobId = jobId;
		this.jdbcURL = jdbcURL;
		this.username = username;
		this.pwd = pwd;
		this.ttl = ttl;
		this.limit = limit;
	}

	public String getUserID() {
		return userID;
	}
	
	public String getLogin() {
		return login;
	}
	
	/**
	 * return the userID and the login - use this for logging
	 * @return
	 */
	public String getUserIdandLogin() {
		return userID+" ("+login+")";
	}

	public ProjectPK getProjectPK() {
		return projectPK;
	}

	public String getKey() {
		return key;
	}

	public String getSQLQuery() {
		return SQLQuery;
	}

	public String getJobId() {
		return jobId;
	}

	public String getJdbcURL() {
		return jdbcURL;
	}

	public String getUsername() {
		return username;
	}

	public String getPwd() {
		return pwd;
	}

	public int getTTL() {
		return ttl;
	}

	public long getLimit() {
		return limit;
	}

}
