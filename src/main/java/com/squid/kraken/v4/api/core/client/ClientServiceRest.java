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
package com.squid.kraken.v4.api.core.client;

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
import com.squid.kraken.v4.model.Client;
import com.squid.kraken.v4.model.ClientPK;
import com.squid.kraken.v4.persistence.AppContext;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;

/**
 * {@link Client} management service.
 */
@Produces({ MediaType.APPLICATION_JSON })
@Api(value = "clients", authorizations = { @Authorization(value = "kraken_auth", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
public class ClientServiceRest extends BaseServiceRest {

	private final static String PARAM_NAME = "clientId";

	private ClientServiceBaseImpl delegate = ClientServiceBaseImpl
			.getInstance();

	public ClientServiceRest(AppContext userContext) {
		super(userContext);
	}

	/**
	 * Get the list of {@link Client}.<br>
	 * Requires READ Role on the Customer.
	 */
	@Path("")
	@GET
	@ApiOperation(value = "Gets All Clients")
	public List<Client> readAll() {
		return delegate.readAll(userContext);
	}

	/**
	 * Delete an existing {@link Client}.
	 */
	@DELETE
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Deletes a Client")
	public boolean delete(@PathParam(PARAM_NAME) String clientId) {
		return delegate.delete(userContext,
				new ClientPK(userContext.getCustomerId(), clientId));
	}

	/**
	 * Read an existing {@link Client}.
	 */
	@Path("{" + PARAM_NAME + "}")
	@GET
	@ApiOperation(value = "Gets a Client")
	public Client read(@PathParam(PARAM_NAME) String clientId) {
		return delegate.read(userContext,
				new ClientPK(userContext.getCustomerId(), clientId));
	}

	/**
	 * Update an existing {@link Client}.
	 */
	@POST
	@Path("")
	@ApiOperation(value = "Creates a Client")
	public Client store(@ApiParam(required = true) Client client) {
		return delegate.store(userContext, client);
	}

	/**
	 * Update an existing {@link Client}.
	 */
	@POST
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Creates a Client")
	public Client store(@PathParam(PARAM_NAME) String clientId,
			@ApiParam(required = true) Client client) {
		return delegate.store(userContext, client);
	}

	/**
	 * Update an existing {@link Client}.
	 */
	@PUT
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Updates a Client")
	public Client update(@PathParam(PARAM_NAME) String clientId,
			@ApiParam(required = true) Client client) {
		return store(clientId, client);
	}

	@Path("{" + PARAM_NAME + "}"+"/access")
	@GET
	@ApiOperation(value = "Gets a Client's access rights")
	public Set<AccessRight> readAccessRights(
			@PathParam(PARAM_NAME) String objectId) {
		return delegate.readAccessRights(userContext,
				new ClientPK(userContext.getCustomerId(), objectId));
	}

}
