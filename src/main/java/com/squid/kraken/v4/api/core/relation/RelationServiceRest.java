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
package com.squid.kraken.v4.api.core.relation;

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
import com.squid.kraken.v4.model.Relation;
import com.squid.kraken.v4.model.RelationPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.Authorization;
import com.wordnik.swagger.annotations.AuthorizationScope;

@Produces({ MediaType.APPLICATION_JSON })
@Api(value = "relations", hidden = true, authorizations = { @Authorization(value = "kraken_auth", type = "oauth2", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
public class RelationServiceRest extends BaseServiceRest {

	private final static String PARAM_NAME = "relationId";

	private RelationServiceBaseImpl delegate = RelationServiceBaseImpl
			.getInstance();

	public RelationServiceRest(AppContext userContext) {
		super(userContext);
	}

	@GET
	@Path("")
	@ApiOperation(value = "Gets All Relations")
	public List<Relation> readAll(@PathParam("projectId") String projectId) {
		return delegate.readAll(userContext, projectId);
	}

	@DELETE
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Deletes a Relation")
	public boolean delete(@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String relationId) {
		return delegate.delete(userContext,
				new RelationPK(userContext.getCustomerId(), projectId,
						relationId));
	}

	@GET
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Gets a Relation")
	public Relation read(@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String relationId) {
		return delegate.read(userContext,
				new RelationPK(userContext.getCustomerId(), projectId,
						relationId));
	}

	@POST
	@Path("")
	@ApiOperation(value = "Creates a Relation")
	public Relation store(@PathParam("projectId") String projectId,
			@ApiParam(required = true) Relation relation) {
		return delegate.store(userContext, relation);
	}

	@POST
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Creates a Relation")
	public Relation store(@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String relationId,
			@ApiParam(required = true) Relation relation) {
		return delegate.store(userContext, relation);
	}

	@PUT
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Updates a Relation")
	public Relation update(@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String relationId,
			@ApiParam(required = true) Relation relation) {
		return delegate.store(userContext, relation);
	}

	@Path("{" + PARAM_NAME + "}" + "/access")
	@GET
	public Set<AccessRight> readAccessRights(
			@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String relationId) {
		return delegate.readAccessRights(userContext, new RelationPK(
				userContext.getCustomerId(), projectId, relationId));
	}

	@Path("{" + PARAM_NAME + "}" + "/access")
	@POST
	public Set<AccessRight> storeAccessRights(
			@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String relationId,
			Set<AccessRight> accessRights) {
		return delegate.storeAccessRights(userContext, new RelationPK(
				userContext.getCustomerId(), projectId, relationId),
				accessRights);
	}

}
