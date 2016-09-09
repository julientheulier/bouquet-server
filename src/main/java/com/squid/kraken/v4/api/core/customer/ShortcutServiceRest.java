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
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.Shortcut;
import com.squid.kraken.v4.model.ShortcutPK;
import com.squid.kraken.v4.persistence.AppContext;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;

@Api(hidden = true, value = "shortcuts", authorizations = { @Authorization(value = "kraken_auth", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
@Produces({ MediaType.APPLICATION_JSON })
public class ShortcutServiceRest extends BaseServiceRest {

	private final static String PARAM_NAME = "shortcutId";

	private ShortcutServiceBaseImpl delegate = ShortcutServiceBaseImpl
			.getInstance();

	public ShortcutServiceRest(AppContext userContext) {
		super(userContext);
	}

	@DELETE
	@Path("{"+PARAM_NAME+"}")
	@ApiOperation(value = "Deletes a shortcut")
	public boolean delete(@PathParam(PARAM_NAME) String shortcutId) {
		return delegate.delete(userContext,
				new ShortcutPK(userContext.getCustomerId(), shortcutId));
	}

	@GET
	@Path("{"+PARAM_NAME+"}")
	@ApiOperation(value = "Gets a shortcut")
	public Shortcut read(@PathParam(PARAM_NAME) String shortcutId, @QueryParam("deepread") Boolean deepread) {
		return delegate.read(userContext,
				new ShortcutPK(userContext.getCustomerId(), shortcutId));
	}

	@GET
	@Path("")
	@ApiOperation(value = "Gets all shortcuts owned by the caller")
	public List<Shortcut> find(@QueryParam("role") String role) {
		if (AccessRight.Role.OWNER.equals(role)) {
			return delegate.findByOwner(userContext);
		} else {
			return delegate.readShortcuts(userContext);
		}
	}

	@PUT
	@Path("{"+PARAM_NAME+"}")
	@ApiOperation(value = "Updates a shortcut")
	public Shortcut store(@PathParam(PARAM_NAME) String shortcutId,
			@ApiParam(required = true) Shortcut shortcut) {
		return delegate.store(userContext, shortcut);
	}

	@POST
	@Path("{"+PARAM_NAME+"}")
	@ApiOperation(value = "Creates a shortcut")
	public Shortcut store2(@PathParam(PARAM_NAME) String shortcutId,
			@ApiParam(required = true) Shortcut shortcut) {
		return delegate.store(userContext, shortcut);
	}

	@POST
	@Path("")
	@ApiOperation(value = "Creates a shortcut")
	public Shortcut store(@ApiParam(required = true) Shortcut shortcut) {
		return delegate.store(userContext, shortcut);
	}

	@Path("{"+PARAM_NAME+"}"+"/access")
	@GET
	@ApiOperation(value = "Gets the ACLs")
	public Set<AccessRight> readAccessRights(
			@PathParam(PARAM_NAME) String shortcutId) {
		return delegate.readAccessRights(userContext, new ShortcutPK(
				userContext.getCustomerId(), shortcutId));
	}

	@Path("{"+PARAM_NAME+"}"+"/access")
	@POST
	@ApiOperation(value = "Sets the ACLs")
	public Set<AccessRight> storeAccessRights(
			@PathParam(PARAM_NAME) String shortcutId,
			@ApiParam(required = true) Set<AccessRight> accessRights) {
		return delegate.storeAccessRights(userContext, new ShortcutPK(
				userContext.getCustomerId(), shortcutId), accessRights);
	}

}
