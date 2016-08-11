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
package com.squid.kraken.v4.api.core.domain;

import java.util.List;
import java.util.Set;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.squid.kraken.v4.api.core.BaseServiceRest;
import com.squid.kraken.v4.api.core.dimension.DimensionServiceRest;
import com.squid.kraken.v4.api.core.metric.MetricServiceRest;
import com.squid.kraken.v4.api.core.relation.DomainRelationServiceRest;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.ExpressionSuggestion;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.ValueType;
import com.squid.kraken.v4.persistence.AppContext;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.Authorization;
import com.wordnik.swagger.annotations.AuthorizationScope;

@Api(value = "domains", hidden = true, authorizations = { @Authorization(value = "kraken_auth", type = "oauth2", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
@Produces({ MediaType.APPLICATION_JSON })
public class DomainServiceRest extends BaseServiceRest {

	private final static String PARAM_NAME = "domainId";

	private DomainServiceBaseImpl delegate = DomainServiceBaseImpl
			.getInstance();

	public DomainServiceRest(AppContext userContext) {
		super(userContext);
	}

	@GET
	@Path("")
	@ApiOperation(value = "Gets all domains")
	public List<Domain> readDomains(@PathParam("projectId") String projectId) {
		return delegate.readAll(userContext, projectId);
	}

	@DELETE
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Deletes a domain")
	public boolean delete(@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String domainId) {
		return delegate.delete(userContext,
				new DomainPK(userContext.getCustomerId(), projectId, domainId));
	}

	@GET
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Gets a domain")
	public Domain read(@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String domainId, @QueryParam("deepread") Boolean deepread) {
		return delegate.read(userContext,
				new DomainPK(userContext.getCustomerId(), projectId, domainId));
	}

	@POST
	@Path("")
	@ApiOperation(value = "Creates a domain")
	public Domain store(@PathParam("projectId") String projectId,
			@ApiParam(required = true) Domain domain) {
		return delegate.store(userContext, domain);
	}

	@POST
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Creates a domain")
	public Domain store(@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String domainId,
			@ApiParam(required = true) Domain domain) {
		domain.setId(new DomainPK(userContext.getCustomerId(), projectId, domainId));
		return delegate.store(userContext, domain);
	}

	@PUT
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Updates a domain")
	public Domain update(@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String domainId,
			@ApiParam(required = true) Domain domain) {
		domain.setId(new DomainPK(userContext.getCustomerId(), projectId, domainId));
		return delegate.store(userContext, domain);
	}

	@Path("{" + PARAM_NAME + "}" + "/access")
	@GET
	@ApiOperation(value = "Gets a domain's access rights")
	public Set<AccessRight> readAccessRights(
			@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String domainId) {
		return delegate.readAccessRights(userContext,
				new DomainPK(userContext.getCustomerId(), projectId, domainId));
	}

	@Path("{" + PARAM_NAME + "}" + "/access")
	@POST
	@ApiOperation(value = "Sets a domain's access rights")
	public Set<AccessRight> storeAccessRights(
			@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String domainId,
			@ApiParam(required = true) Set<AccessRight> accessRights) {
		return delegate.storeAccessRights(userContext,
				new DomainPK(userContext.getCustomerId(), projectId, domainId),
				accessRights);
	}

	@Path("{" + PARAM_NAME + "}" + "/dimensions-suggestion")
	@GET
	@ApiOperation(value = "Gets suggestions for a dimension definition")
	public ExpressionSuggestion getDimensionSuggestion(
			@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@QueryParam("dimensionId") String dimensionId,
			@QueryParam("expression") String expression,
			@QueryParam("offset") Integer offset,
            @QueryParam("filterType") ValueType filterType){
		return delegate.getDimensionSuggestion(userContext, projectId, domainId, dimensionId, expression, offset, filterType);
	}

	@Path("{" + PARAM_NAME + "}" + "/dimensions")
	@ApiOperation(value = "Gets Dimensions")
	public DimensionServiceRest getDimensionService() {
		return new DimensionServiceRest(userContext);
	}

	@Path("{" + PARAM_NAME + "}" + "/relations")
	@ApiOperation(value = "Gets Relations for the given domain")
	public DomainRelationServiceRest getRelationService() {
		return new DomainRelationServiceRest(userContext);
	}

	@Path("{" + PARAM_NAME + "}" + "/metrics-suggestion")
	@GET
	@ApiOperation(value = "Gets suggestions for a metric definition")
	public ExpressionSuggestion getMetricSuggestion(
			@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@QueryParam("metricId") String metricId,
			@QueryParam("expression") String expression,
			@QueryParam("offset") Integer offset,
            @QueryParam("filterType") ValueType filterType) {
		return delegate.getMetricSuggestion(userContext, projectId, domainId, metricId,
				expression, offset, filterType);
	}

	@Path("{" + PARAM_NAME + "}" + "/metrics")
	@ApiOperation(value = "Gets Metrics")
	public MetricServiceRest getMetricService() {
		return new MetricServiceRest(userContext);
	}

	@Path("{" + PARAM_NAME + "}" + "/refreshData")
	@GET
	public boolean refreshDomain(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId) {
		return delegate.refreshDomainData(userContext, projectId, domainId);
	}

	@Path("{" + PARAM_NAME + "}" + "/cache")
	@GET
	@ApiOperation(value = "Gets cache status for this domain")
	public Object readCacheInfo(@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String domainId) {
		return delegate.readCacheInfo(userContext,
				new DomainPK(userContext.getCustomerId(), projectId, domainId));
	}

	@Path("{" + PARAM_NAME + "}" + "/cache/refresh")
	@GET
	@ApiOperation(value = "Force a cache refresh for this domain")
	public Object refreshCache(@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String domainId) {
		return delegate.refreshCache(userContext, new ProjectPK(userContext.getCustomerId(), projectId),
				new DomainPK(userContext.getCustomerId(), projectId, domainId));
	}

	@Path("{" + PARAM_NAME + "}" + "/segment-suggestion")
	@GET
	@ApiOperation(value = "Gets suggestions for a segment definition")
	public ExpressionSuggestion getSegmentSuggestion(
			@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@QueryParam("expression") String expression,
			@QueryParam("offset") Integer offset,
            @QueryParam("filterType") ValueType filterType) {
		return delegate.getSegmentSuggestion(userContext, projectId, domainId,
				expression, offset, filterType);
	}

}
