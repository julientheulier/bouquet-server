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
package com.squid.kraken.v4.api.core.customer;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.BaseServiceRest;
import com.squid.kraken.v4.api.core.InvalidCredentialsAPIException;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.caching.redis.RedisCacheManager;
import com.squid.kraken.v4.caching.redis.queryworkerserver.QueryWorkerJobStatus;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchyManager;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.Authorization;
import com.wordnik.swagger.annotations.AuthorizationScope;

/**
 * @author sergefantino
 *
 */

@Api(hidden = true, value = "queries", authorizations = { @Authorization(value = "kraken_auth", type = "oauth2", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
@Produces({ MediaType.APPLICATION_JSON })
public class QueriesServiceRest extends BaseServiceRest {

	private final static String PARAM_NAME = "queryId";

	/**
	 * @param userContext
	 */
	public QueriesServiceRest(AppContext userContext) {
		super(userContext);
	}

	@GET
	@Path("")
	@ApiOperation(value = "Gets all ongoing queries")
	public List<QueryWorkerJobStatus> getQueries() {
		// Retrieves all ongoing queries for the current customer
		String customerId = userContext.getCustomerId();
		List<QueryWorkerJobStatus> queries = RedisCacheManager.getInstance().getQueryServer().getOngoingQueries(customerId);
		queries.addAll(DomainHierarchyManager.INSTANCE.getOngoingQueries(customerId)) ;
		List<QueryWorkerJobStatus> visible = new ArrayList<>();
		for (QueryWorkerJobStatus query : queries) {
			ProjectPK projectPK = query.getProjectPK();
			try {
				Project project = ProjectManager.INSTANCE.getProject(userContext, projectPK);
				// restrict to privileged user
				if (checkACL(customerId, project, query)) {
					visible.add(query);
				}
			} catch (ScopeException e) {
				// ignore
			}
		}
		return visible;
	}

	@GET
    @Path("{"+PARAM_NAME+"}")
	@ApiOperation(value = "get ongoing query")
	public QueryWorkerJobStatus getQuery(@PathParam(PARAM_NAME) String key) {
		// first check if the query is available
		String customerId = userContext.getCustomerId();
		List<QueryWorkerJobStatus> queries = RedisCacheManager.getInstance().getQueryServer().getOngoingQueries(customerId);
		queries.addAll(DomainHierarchyManager.INSTANCE.getOngoingQueries(customerId)) ;
		for (QueryWorkerJobStatus query : queries) {
			if (query.getKey().equals(key)) {
				ProjectPK projectPK = query.getProjectPK();
				try {
					Project project = ProjectManager.INSTANCE.getProject(userContext, projectPK);
					// restrict to privileged user
					if (checkACL(customerId, project, query)) {
						return query;
					} else {
						throw new InvalidCredentialsAPIException("user not allowed to cancel query ID="+key, true);
					}
				} catch (ScopeException e) {
					// ignore
				}
			}
		}
		//
		throw new ObjectNotFoundAPIException("no query with ID="+key+" found", true);
	}

	@GET
    @Path("{"+PARAM_NAME+"}/cancel")
	@ApiOperation(value = "Cancel an ongoing query")
	public boolean cancelQuery(@PathParam(PARAM_NAME) String key) {
		return doCancelQuery(key);
	}

	@DELETE
    @Path("{"+PARAM_NAME+"}")
	@ApiOperation(value = "Delete an ongoing query (same as cancel)")
	public boolean deleteQuery(@PathParam(PARAM_NAME) String key) {
		return doCancelQuery(key);
	}
	
	protected boolean doCancelQuery(String key) {
		// first check if the query is available
		String customerId = userContext.getCustomerId();
		
		// analysis
		List<QueryWorkerJobStatus> queries = RedisCacheManager.getInstance().getQueryServer().getOngoingQueries(customerId);
		for (QueryWorkerJobStatus query : queries) {
			if (query.getKey().equals(key)) {
				ProjectPK projectPK = query.getProjectPK();
				try {
					Project project = ProjectManager.INSTANCE.getProject(userContext, projectPK);
					// restrict to privileged user
					if (checkACL(customerId, project, query)) {
						// ok, cancel
						return RedisCacheManager.getInstance().getQueryServer().cancelOngoingQuery(customerId, key);
					} else {
						throw new InvalidCredentialsAPIException("user not allowed to cancel query ID="+key, true);
					}
				} catch (ScopeException e) {
					// ignore
				}
			}
		}
		//hierarchies
		queries = DomainHierarchyManager.INSTANCE.getOngoingQueries(customerId);
		for (QueryWorkerJobStatus query : queries) {
			if (query.getKey().equals(key)) {
				ProjectPK projectPK = query.getProjectPK();
				try {
					Project project = ProjectManager.INSTANCE.getProject(userContext, projectPK);
					// restrict to privileged user
					if (checkACL(customerId, project, query)) {
						// ok, cancel
						return DomainHierarchyManager.INSTANCE.cancelOngoingQuery(customerId, key);
					} else {
						throw new InvalidCredentialsAPIException("user not allowed to cancel query ID="+key, true);
					}
				} catch (ScopeException e) {
					// ignore
				}
			}
		}
		
		
		//
		throw new ObjectNotFoundAPIException("no query with ID="+key+" found", true);
	}
	
	private boolean checkACL(String customerId, Project project, QueryWorkerJobStatus query) {
		if (AccessRightsUtils.getInstance().hasRole(userContext, project, Role.WRITE)) {
			return true;
		}  else if (AccessRightsUtils.getInstance().hasRole(userContext, project, Role.READ)) {
			// or to the query owner
			if (query.getUserID().equals(userContext.getUser().getOid())) {
				return true;
			}
		}
		// else
		return false;
	}
	

	
}
