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
package com.squid.kraken.v4.api.core.nlu;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.customer.CoreAuthenticatedServiceRest;
import com.squid.kraken.v4.persistence.AppContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;

/**
 * @author sergefantino
 *
 */
@Path("")
@Api(
		value = "nlu", 
		hidden = false, 
		description = "natural langage understanding support",
		authorizations = { @Authorization(value = "kraken_auth", scopes = { @AuthorizationScope(scope = "access", description = "Access") }) })
@Produces({ MediaType.APPLICATION_JSON })
public class NluServiceRest extends CoreAuthenticatedServiceRest {
	
	private static final String BBID_PARAM_NAME = "REFERENCE";
	private static final String MESSAGE_PARAM_NAME = "msg";
	
	@Context
	UriInfo uriInfo;

	@GET
	@Path("/nlu/{" + BBID_PARAM_NAME + "}/train")
	@ApiOperation(value = "generate a learning dataset for this bookmark")
	public Object generateTrainingSet(
			@Context HttpServletRequest request, 
			@PathParam(BBID_PARAM_NAME) String BBID) throws ScopeException {
		AppContext userContext = getUserContext(request);
		return delegate(userContext).generateTrainingSet(BBID);
	}

	@GET
	@Path("/nlu/{" + BBID_PARAM_NAME + "}/query")
	@ApiOperation(value = "proceed a query")
	public Object query(
			@Context HttpServletRequest request, 
			@PathParam(BBID_PARAM_NAME) String BBID,
			@QueryParam(MESSAGE_PARAM_NAME) String message) throws ScopeException {
		AppContext userContext = getUserContext(request);
		return delegate(userContext).query(BBID, message);
	}

	/**
	 * @param userContext
	 * @return
	 */
	private NluServiceBaseImpl delegate(AppContext userContext) {
		return new NluServiceBaseImpl(uriInfo, userContext);
	}

}
