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
package com.squid.kraken.v4.api.core.metric;

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
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.MetricExt;
import com.squid.kraken.v4.model.MetricPK;
import com.squid.kraken.v4.persistence.AppContext;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;

@Produces({ MediaType.APPLICATION_JSON })
@Api(hidden=false, value = "metrics", authorizations = { @Authorization(value = "kraken_auth", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
public class MetricServiceRest extends BaseServiceRest {

	private final static String PARAM_NAME = "metricId";

	private MetricServiceBaseImpl delegate = MetricServiceBaseImpl
			.getInstance();

	public MetricServiceRest(AppContext userContext) {
		super(userContext);
	}

	@Path("")
	@GET
	@ApiOperation(value = "Gets All Metrics")
	public List<MetricExt> readMetrics(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId) throws ComputingException, InterruptedException {
		return delegate.readAll(userContext,
				new DomainPK(userContext.getCustomerId(), projectId, domainId));
	}

	@DELETE
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Deletes a Metric")
	public boolean deleteMetric(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam(PARAM_NAME) String metricId) {
		return delegate.delete(userContext,
				new MetricPK(userContext.getCustomerId(), projectId, domainId,
						metricId));
	}

	@GET
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Gets a Metric")
	public Metric readMetric(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam(PARAM_NAME) String metricId, @QueryParam("deepread") Boolean deepread) throws ComputingException, InterruptedException {
		return delegate.read(userContext,
				new MetricPK(userContext.getCustomerId(), projectId, domainId,
						metricId));
	}

	@POST
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Creates a Metric")
	public Metric storeMetric(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam(PARAM_NAME) String metricId,
			@ApiParam(required = true) Metric metric) {
		return delegate.store(userContext, metric);
	}

	@POST
	@Path("")
	@ApiOperation(value = "Creates a Metric")
	public Metric storeMetric2(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@ApiParam(required = true) Metric metric) {
		return delegate.store(userContext, metric);
	}

	@PUT
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Updates a Metric")
	public Metric updateMetric(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam(PARAM_NAME) String metricId,
			@ApiParam(required = true) Metric metric) {
		return delegate.store(userContext, metric);
	}

	@Path("{" + PARAM_NAME + "}"+"/access")
	@GET
	@ApiOperation(value = "Gets a Metric's access rights")
	public Set<AccessRight> readAccessRightsMetric(
			@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam(PARAM_NAME) String metricId) {
		return delegate.readAccessRights(userContext,
				new MetricPK(userContext.getCustomerId(), projectId, domainId,
						metricId));
	}

	@Path("{" + PARAM_NAME + "}"+"/access")
	@POST
	@ApiOperation(value = "Sets a Metric's access rights")
	public Set<AccessRight> storeAccessRightsMetric(
			@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam(PARAM_NAME) String metricId,
			@ApiParam(required = true) Set<AccessRight> accessRights) {
		return delegate.storeAccessRights(userContext,
				new MetricPK(userContext.getCustomerId(), projectId, domainId,
						metricId), accessRights);
	}

}
