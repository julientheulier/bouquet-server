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

import com.squid.kraken.v4.api.core.GenericServiceImpl;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.BookmarkPK;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.BookmarkDAO;

public class BookmarkServiceBaseImpl extends
		GenericServiceImpl<Bookmark, BookmarkPK> {

	private static BookmarkServiceBaseImpl instance;

	public static BookmarkServiceBaseImpl getInstance() {
		if (instance == null) {
			instance = new BookmarkServiceBaseImpl();
		}
		return instance;
	}

	private BookmarkServiceBaseImpl() {
		// made private for singleton access
		super(Bookmark.class);
	}

	public List<Bookmark> readAll(AppContext ctx, String projectId) {
		return ((BookmarkDAO) DAOFactory.getDAOFactory().getDAO(Bookmark.class))
				.findByParent(ctx, new ProjectPK(ctx.getCustomerId(), projectId));
	}
	
	public List<Bookmark> readAll(AppContext ctx, String projectId, String path) {
		return ((BookmarkDAO) DAOFactory.getDAOFactory().getDAO(Bookmark.class))
				.findByPath(ctx, new ProjectPK(ctx.getCustomerId(), projectId), path);
	}
}
