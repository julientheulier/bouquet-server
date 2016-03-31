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
package com.squid.kraken.v4.api.core.attribute;

import java.util.List;
import java.util.Set;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.squid.kraken.v4.api.core.BaseServiceRest;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.Attribute;
import com.squid.kraken.v4.model.AttributePK;
import com.squid.kraken.v4.model.DimensionPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.Authorization;
import com.wordnik.swagger.annotations.AuthorizationScope;

@Produces({ MediaType.APPLICATION_JSON })
@Api(value = "attributes", hidden = true, authorizations = { @Authorization(value = "kraken_auth", type = "oauth2", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
public class AttributeServiceRest extends BaseServiceRest {

	private final static String PARAM_NAME = "attributeId";

	private AttributeServiceBaseImpl delegate = AttributeServiceBaseImpl
			.getInstance();

	public AttributeServiceRest(AppContext userContext) {
		super(userContext);
	}
	
	@Path("")
	@GET
	@ApiOperation(value = "Gets all Attribute")
	public List<Attribute> readAttributes(
			@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam("dimensionId") String dimensionId) {
		return delegate.readAll(userContext,
				new DimensionPK(userContext.getCustomerId(), projectId,
						domainId, dimensionId));
	}

	@DELETE
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Deletes an Attribute")
	public boolean delete(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam("dimensionId") String dimensionId,
			@PathParam(PARAM_NAME) String attributeId) {
		return delegate.delete(userContext,
				new AttributePK(userContext.getCustomerId(), projectId,
						domainId, dimensionId, attributeId));
	}

	@GET
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Gets an Attribute")
	public Attribute read(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam("dimensionId") String dimensionId,
			@PathParam(PARAM_NAME) String attributeId) {
		return delegate.read(userContext,
				new AttributePK(userContext.getCustomerId(), projectId,
						domainId, dimensionId, attributeId));
	}

	@POST
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Creates an Attribute")
	public Attribute store(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam("dimensionId") String dimensionId,
			@PathParam(PARAM_NAME) String attributeId,
			@ApiParam(required = true) Attribute attribute) {
		return delegate.store(userContext, attribute);
	}

	@PUT
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Updates an Attribute")
	public Attribute update(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam("dimensionId") String dimensionId,
			@PathParam(PARAM_NAME) String attributeId,
			@ApiParam(required = true) Attribute attribute) {
		return delegate.store(userContext, attribute);
	}

	@POST
	@Path("")
	@ApiOperation(value = "Creates an Attribute")
	public Attribute store(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam("dimensionId") String dimensionId,
			@ApiParam(required = true) Attribute attribute) {
		return delegate.store(userContext, attribute);
	}

	@Path("{" + PARAM_NAME + "}"+"/access")
	@GET
	public Set<AccessRight> readAccessRights(
			@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam("dimensionId") String dimensionId,
			@PathParam(PARAM_NAME) String attributeId) {
		return delegate.readAccessRights(userContext, new AttributePK(
				userContext.getCustomerId(), projectId, domainId, dimensionId,
				attributeId));
	}

	@Path("{" + PARAM_NAME + "}"+"/access")
	@POST
	public Set<AccessRight> storeAccessRights(
			@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam("dimensionId") String dimensionId,
			@PathParam(PARAM_NAME) String attributeId,
			Set<AccessRight> accessRights) {
		return delegate.storeAccessRights(userContext, new AttributePK(
				userContext.getCustomerId(), projectId, domainId, dimensionId,
				attributeId), accessRights);
	}
}
