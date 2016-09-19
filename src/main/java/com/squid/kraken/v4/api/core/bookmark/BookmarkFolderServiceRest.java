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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.api.core.BaseServiceRest;
import com.squid.kraken.v4.model.BookmarkFolder;
import com.squid.kraken.v4.persistence.AppContext;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;

@Api(hidden=true, value = "bookmark-folders", authorizations = { @Authorization(value = "kraken_auth", scopes = { @AuthorizationScope(scope = "access", description = "Access") }) })
@Produces({ MediaType.APPLICATION_JSON })
public class BookmarkFolderServiceRest extends BaseServiceRest {

	static final Logger logger = LoggerFactory
			.getLogger(BookmarkFolderServiceRest.class);

	private BookmarkFolderServiceBaseImpl delegate = BookmarkFolderServiceBaseImpl
			.getInstance();

	public BookmarkFolderServiceRest(AppContext userContext) {
		super(userContext);
	}

	@GET
	@Path("")
	@ApiOperation(value = "Get My Bookmarks")
	public BookmarkFolder read() {
		return delegate.read(userContext, null);
	}

	@GET
	@Path("{path}")
	@ApiOperation(value = "Get a Bookmark folder given a full path")
	public BookmarkFolder read(@PathParam("path") String path) {
		return delegate.read(userContext, path);
	}

	@GET
	@Path("folders")
	@ApiOperation(value = "Get all Bookmark folders under My Bookmarks")
	public List<BookmarkFolder> readFolders() {
		return delegate.readFolders(userContext, null);
	}

	@GET
	@Path("{path}/folders")
	@ApiOperation(value = "Get all Bookmark folders under the specified full path")
	public List<BookmarkFolder> readFolders(@PathParam("path") String path) {
		return delegate.readFolders(userContext, path);
	}

}
