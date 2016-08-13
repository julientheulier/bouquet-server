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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.BookmarkConfig;
import com.squid.kraken.v4.model.BookmarkPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.GenericDAO;

/**
 * @author sergefantino
 *
 */
public class BookmarkManager {
	
	public static final BookmarkManager INSTANCE = new BookmarkManager();
	
	private GenericDAO<Bookmark, BookmarkPK> delegate = DAOFactory.getDAOFactory().getDAO(Bookmark.class);
	
	private BookmarkManager() {
		//
	}
	
	public BookmarkConfig readConfig(Bookmark bookmark) {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		try {
			BookmarkConfig config = mapper.readValue(bookmark.getConfig(), BookmarkConfig.class);
			return config;
		} catch (Exception e) {
			throw new APIException(e);
		}
	}
	
	public Optional<Bookmark> readBookmark(AppContext userContext, BookmarkPK bookmarkPk) {
		return delegate.read(userContext, bookmarkPk);
	}

}