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

import com.squid.kraken.v4.core.database.impl.ExecuteQueryTask;

/**
 * A simple wrapper to keep track of the job
 * @author sergefantino
 *
 */
public class QueryWorkerJob {
	
	private QueryWorkerJobRequest request;
	private ExecuteQueryTask job;
	
	private long start;
	
	public QueryWorkerJob(QueryWorkerJobRequest request, ExecuteQueryTask job) {
		super();
		this.request = request;
		this.job = job;
		this.start = System.currentTimeMillis();
	}
	
	public QueryWorkerJobStatus getStatus() {
		long elapse = System.currentTimeMillis() - start;
		return new QueryWorkerJobStatus(
				request.getUserID(), 
				request.getLogin(),
				request.getProjectPK(),
				request.getJobId(),
				request.getKey(), 
				job.getID(), 
				request.getSQLQuery(),
				start,
				elapse);
	}

	/**
	 * @return
	 */
	public void cancel() {
		job.cancel();
	}

}
