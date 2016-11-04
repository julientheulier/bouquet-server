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
package com.squid.enterprise.api;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.enterprise.model.ShareQuery;
import com.squid.enterprise.model.ShareReply;
import com.squid.enterprise.model.Status;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.api.core.customer.CoreAuthenticatedServiceRest;
import com.squid.kraken.v4.persistence.AppContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;

/**
 * This is the API for OB enterprise features
 * @author serge.fantino
 *
 */
@Path("/rs/enterprise")
@Api(hidden = true, value = "enterprise", authorizations = { @Authorization(value = "kraken_auth", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
@Produces({ MediaType.APPLICATION_JSON })
public class EnterpriseServiceRest extends CoreAuthenticatedServiceRest {

	static final Logger logger = LoggerFactory
			.getLogger(EnterpriseServiceRest.class);

	public final static String BBID_PARAM_NAME = "REFERENCE";

	private EnterpriseServiceBaseImpl delegate = EnterpriseServiceBaseImpl
			.getInstance();
	
	@GET
	@Path("/status")
	@ApiOperation(
			value = "Open Bouquet Enterprise Service status",
			notes = "")
	public Status getStatus(
			@Context HttpServletRequest request) 
	{
		return new Status();
	}
	
	@GET
	@Path("/share/{" + BBID_PARAM_NAME + "}")
	@ApiOperation(
			value = "Get information regarding who is sharing this resource",
			notes = "")
	public ShareQuery getShare(
			@Context HttpServletRequest request,
			@PathParam(BBID_PARAM_NAME) String reference) 
	{
		AppContext ctx = getUserContext(request);
		// escalate to root when needing to deal with Customer or updating User's rights
		AppContext root = ServiceUtils.getInstance().getRootUserContext(ctx);
		return delegate.getShare(ctx, root, reference);
	}
	
	/**
	 * POST method used by the client app to share some contents with a set of users.
	 * As of 4.2.28, content can be Project or Bookmark. User can get access as Editor (Write Role) or Viewer (Read Role only).
	 * Note that an User may not be already a registered user n the system. If the user is not yet part of the team, it will provision a "dummy" user locally (identified by its email).
	 * Once the new user will join the team, he will find his resources with access control already set.
	 * 
	 * This method performs two tasks:
	 * - granting local access to the resources to the users
	 * - forwarding an invitation for each team member (or future one) to OB.io
	 * 
	 * @param request
	 * @param query
	 * @return
	 */
	@POST
	@Path("/share")
	@ApiOperation(
			value = "Share some resources with some users",
			notes = "")
	public ShareReply doShare(
			@Context HttpServletRequest request,
			ShareQuery query) 
	{
		AppContext ctx = getUserContext(request);
		return delegate.doShare(ctx, query);
	}

}
