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

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.squid.kraken.v4.api.core.BaseServiceRest;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.State;
import com.squid.kraken.v4.model.StatePK;
import com.squid.kraken.v4.persistence.AppContext;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;

@Api(hidden = true, value = "states", authorizations = { @Authorization(value = "kraken_auth", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
@Produces({ MediaType.APPLICATION_JSON })
public class StateServiceRest extends BaseServiceRest {

	private final static String PARAM_NAME = "stateId";

	private StateServiceBaseImpl delegate = StateServiceBaseImpl
			.getInstance();

	public StateServiceRest(AppContext userContext) {
		super(userContext);
	}

	@GET
	@Path("{"+PARAM_NAME+"}")
	@ApiOperation(value = "Gets a state")
	public State read(@PathParam(PARAM_NAME) String stateId) {
		return delegate.read(userContext,
				new StatePK(userContext.getCustomerId(),
						stateId));
	}
	
	@GET
	@Path("")
	@ApiOperation(value = "Gets all non-temporary states")
	public List<State> read() {
		return delegate.readAll(userContext);
	}

	@POST
	@Path("")
	@ApiOperation(value = "Creates a state")
	public State addState(@ApiParam(required = true) State state) {
		return delegate.store(userContext, state);
	}

	@Path("{"+PARAM_NAME+"}"+"/access")
	@GET
	@ApiOperation(value = "Gets the ACLs")
	public Set<AccessRight> readAccessRights(
			@PathParam(PARAM_NAME) String objectId) {
		return delegate.readAccessRights(userContext, new StatePK(
				userContext.getCustomerId(), objectId));
	}

}
