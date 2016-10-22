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
package com.squid.kraken.v4.core.analysis.engine.bookmark;

import java.util.List;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.BookmarkConfig;
import com.squid.kraken.v4.model.BookmarkPK;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.BookmarkDAO;

/**
 * @author sergefantino
 *
 */
public class BookmarkManager {
	
	public static final BookmarkManager INSTANCE = new BookmarkManager();
	
	private BookmarkDAO delegate = (BookmarkDAO)DAOFactory.getDAOFactory().getDAO(Bookmark.class);
	
	private BookmarkManager() {
		//
	}
	
	public BookmarkDAO getDAO() { return delegate; }
	
	public BookmarkConfig readConfig(Bookmark bookmark) throws ScopeException {
		if (bookmark==null) return null;
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		try {
			BookmarkConfig config = mapper.readValue(bookmark.getConfig(), BookmarkConfig.class);
			return config;
		} catch (Exception e) {
			throw new ScopeException("unable to read the bookmark '"+bookmark.getBBID()+"' definition"+e.getMessage(), e);
		}
	}
	
	public Optional<Bookmark> readBookmark(AppContext userContext, BookmarkPK bookmarkPk) {
		return delegate.read(userContext, bookmarkPk);
	}
	
	public List<Bookmark> findBookmarksByParent(AppContext userContext, String parentPath) {
		return delegate.findByPath(userContext, parentPath);
	}

	public String getMyBookmarkPath(AppContext ctx) {
		return Bookmark.SEPARATOR + Bookmark.Folder.USER
					+ Bookmark.SEPARATOR + ctx.getUser().getOid();
	}
	
	public Space getBookmarkSpace(Universe universe, Bookmark bookmark) throws ScopeException {
		BookmarkConfig config = BookmarkManager.INSTANCE.readConfig(bookmark);
		return getBookmarkSpace(universe, bookmark, config);
	}
	
	public Space getBookmarkSpace(Universe universe, Bookmark bookmark, BookmarkConfig config) throws ScopeException {
		String domainId = config.getDomain();
		DomainPK domainPk = new DomainPK(bookmark.getId().getParent(), domainId);
		Domain domain = universe.getDomain(domainPk);
		return new Space(universe, domain, bookmark);
	}

}
