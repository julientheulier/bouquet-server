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
package com.squid.kraken.v4.api.core.bookmark;

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
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.BookmarkPK;
import com.squid.kraken.v4.persistence.AppContext;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;

@Api(value = "bookmarks", hidden = true, authorizations = { @Authorization(value = "kraken_auth", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
@Produces({ MediaType.APPLICATION_JSON })
public class BookmarkServiceRest extends BaseServiceRest {

	private final static String PARAM_NAME = "bookmarkId";

	private BookmarkServiceBaseImpl delegate = BookmarkServiceBaseImpl
			.getInstance();

	public BookmarkServiceRest(AppContext userContext) {
		super(userContext);
	}

	@GET
	@Path("")
	@ApiOperation(value = "Gets all bookmarks")
	public List<Bookmark> readAll(@PathParam("projectId") String projectId, @QueryParam("path") String path ) {
		if (path == null) {
			return delegate.readAll(userContext, projectId);
		} else {
			return delegate.readAll(userContext, projectId, path);
		}
	}

	@DELETE
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Deletes a bookmark")
	public boolean delete(@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String id) {
		return delegate.delete(userContext,
				new BookmarkPK(userContext.getCustomerId(), projectId, id));
	}

	@GET
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Gets a bookmark")
	public Bookmark read(@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String id) {
		return delegate.read(userContext,
				new BookmarkPK(userContext.getCustomerId(), projectId, id));
	}

	@POST
	@Path("")
	@ApiOperation(value = "Creates a bookmark")
	public Bookmark store(@PathParam("projectId") String projectId,
			@ApiParam(required = true) Bookmark bookmark) {
		return delegate.store(userContext, bookmark);
	}

	@POST
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Creates a bookmark")
	public Bookmark store(@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String bookmarkId,
			@ApiParam(required = true) Bookmark bookmark) {
		bookmark.setId(new BookmarkPK(userContext.getCustomerId(), projectId, bookmarkId));
		return delegate.store(userContext, bookmark);
	}

	@PUT
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Updates a bookmark")
	public Bookmark update(@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String bookmarkId,
			@ApiParam(required = true) Bookmark bookmark) {
		bookmark.setId(new BookmarkPK(userContext.getCustomerId(), projectId, bookmarkId));
		return delegate.store(userContext, bookmark);
	}

	@Path("{" + PARAM_NAME + "}" + "/access")
	@GET
	@ApiOperation(value = "Gets a bookmark's access rights")
	public Set<AccessRight> readAccessRights(
			@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String domainId) {
		return delegate.readAccessRights(userContext,
				new BookmarkPK(userContext.getCustomerId(), projectId, domainId));
	}

	@Path("{" + PARAM_NAME + "}" + "/access")
	@POST
	@ApiOperation(value = "Sets a bookmark's access rights")
	public Set<AccessRight> storeAccessRights(
			@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String domainId,
			@ApiParam(required = true) Set<AccessRight> accessRights) {
		return delegate.storeAccessRights(userContext,
				new BookmarkPK(userContext.getCustomerId(), projectId, domainId),
				accessRights);
	}

}
