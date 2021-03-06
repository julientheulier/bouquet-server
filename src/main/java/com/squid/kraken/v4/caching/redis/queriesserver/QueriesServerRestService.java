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
package com.squid.kraken.v4.caching.redis.queriesserver;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import com.squid.kraken.v4.caching.redis.queryworkerserver.QueryWorkerJobRequest;
import com.squid.kraken.v4.caching.redis.queryworkerserver.QueryWorkerJobStatus;

public class QueriesServerRestService {

	private QueriesServer delegate;

	public QueriesServerRestService(QueriesServer s) {

		this.delegate = s;
	}

	@GET
	@Path("/fetch")
	public int fetch(@QueryParam("request") QueryWorkerJobRequest request) throws InterruptedException {
		return this.delegate.fetch(request);
	}

	@GET
	@Path("/ongoing")
	public boolean ongoing(@QueryParam("key") String key) {
		return this.delegate.isQueryOngoing(key);
	}
	
	@GET
	@Path("/queries")
	public List<QueryWorkerJobStatus> queries(@QueryParam("customerId") String customerId) {
		return this.delegate.getOngoingQueries(customerId);
	}

	@GET
	@Path("/hello")
	public String hello() {
		return delegate.hello();
	}
}
