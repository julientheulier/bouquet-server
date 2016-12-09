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

package com.squid.kraken.v4.core.analysis.engine.query;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.RenderingException;
import com.squid.kraken.v4.caching.NotInCacheException;
import com.squid.kraken.v4.caching.redis.RedisCacheManager;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValue;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.processor.DataMatrixTransformOrderBy;
import com.squid.kraken.v4.core.analysis.engine.processor.DataMatrixTransformTruncate;
import com.squid.kraken.v4.core.sql.script.SQLScript;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.writers.QueryWriter;

public class QueryRunner {

	static final Logger logger = LoggerFactory.getLogger(QueryRunner.class);

	private AppContext ctx;
	private BaseQuery query;
	private QueryWriter writer;
	private String jobId;
	private boolean lazy;

	public QueryRunner(AppContext ctx, BaseQuery query, boolean lazy, QueryWriter writer, String jobId) {
		this.ctx = ctx;
		this.query = query;
		this.lazy = lazy;
		this.writer = writer;
		this.jobId = jobId;
	}

	public void run() throws ComputingException, NotInCacheException {
		try {

			Project project = query.getUniverse().asRootUserContext().getProject();
			//
			List<String> deps = query.computeDependencies();// to override
			//
			String url = project.getDbUrl();
			String user = project.getDbUser();
			String pwd = project.getDbPassword();
			//
			SQLScript script = query.generateScript();
			String sql = script.render();
			String sqlNoLimitNoOrder = null;// Optionally the "full" version
			boolean noRollups = true;
			if (query instanceof SimpleQuery) {
				SimpleQuery sq = (SimpleQuery) query;
				noRollups = !sq.hasRollups();
			}

			boolean checkFullVersion = (query.getSelect().getStatement().hasLimitValue()
					|| query.getSelect().getStatement().hasOffsetValue()
					|| query.getSelect().getStatement().hasOrderByPieces()) && noRollups;
			RedisCacheValue result;
			// first check if the original query is in cache (lazy)
			result = RedisCacheManager.getInstance().getRedisCacheValueLazy(sql, deps, url, user, pwd, -2);
			// check full version if different
			if (result == null && checkFullVersion) {
				// else try to use the query with no orderBy/limit
				sqlNoLimitNoOrder = query.renderNoLimitNoOrderBy();
				result = RedisCacheManager.getInstance().getRedisCacheValueLazy(sqlNoLimitNoOrder, deps, url, user, pwd,
						-2);
				if (result instanceof RedisCacheValuesList) {
					// we'd rather recompute the data than rebuild a big result
					// set in memory
					result = null;
				}
				if (result != null) {
					if (!query.getOrderBy().isEmpty()) {
						query.addPostProcessing(new DataMatrixTransformOrderBy(query.getOrderBy()));
					}
					if (query.getSelect().getStatement().hasLimitValue()
							|| query.getSelect().getStatement().hasOffsetValue()) {
						query.addPostProcessing(
								new DataMatrixTransformTruncate(query.getSelect().getStatement().getLimitValue(),
										query.getSelect().getStatement().getOffsetValue()));
					}
				}
			}
			if (result != null) {
				// all right
			} else if (lazy) {
				// do not compute
				// logger.info("Lazy query, analysis not in cache");
				throw new NotInCacheException("Lazy query, analysis " + jobId + " not in cache");
			} else {
				// compute
				ProjectPK projectPK = project.getId();
				result = RedisCacheManager.getInstance().getRedisCacheValue(ctx.getUser().getOid(),
						ctx.getUser().getLogin(), // T2324
						projectPK, sql,
						deps, jobId, url, user, pwd, -2, query.getSelect().getStatement().getLimitValue());
				if (result == null) {
					throw new ComputingException("Failed to compute or retrieve the matrix for job " + jobId);
				} else {
					if (checkFullVersion && result instanceof RawMatrix) {
						// create a new reference
						RawMatrix rm = (RawMatrix) result;
						if (!rm.isMoreData()) {// only if the result is complete
												// == full version
							String refKey = RedisCacheManager.getInstance().addCacheReference(sqlNoLimitNoOrder, deps,
									rm.getRedisKey());
							if (refKey != null) {
								logger.info("Analysis " + jobId
										+ " full result set: Creating new Cache Reference to NO-LIMIT version: "
										+ refKey + " to " + rm.getRedisKey());
							}
						}
					}
				}
			}
			writer.setSource(result);
			writer.setMapper(script.getMapper());
			writer.setDatabase(query.getDatasource().getDBManager().getDatabase());
			writer.write();

		} catch (InterruptedException | RenderingException | ScopeException | SQLScopeException e) {
			throw new ComputingException(
					"Failed to compute or retrieve the matrix for job " + jobId + ": " + e.getMessage(), e);
		}

	}

}
