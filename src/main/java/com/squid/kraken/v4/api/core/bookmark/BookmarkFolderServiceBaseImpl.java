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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.BookmarkFolder;
import com.squid.kraken.v4.model.BookmarkPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.BookmarkDAO;

public class BookmarkFolderServiceBaseImpl {

	protected DAOFactory factory = DAOFactory.getDAOFactory();

	private static BookmarkFolderServiceBaseImpl instance;

	public static BookmarkFolderServiceBaseImpl getInstance() {
		if (instance == null) {
			instance = new BookmarkFolderServiceBaseImpl();
		}
		return instance;
	}

	private BookmarkFolderServiceBaseImpl() {
	}

	public BookmarkFolder read(AppContext ctx, String path) {
		BookmarkFolder bf = new BookmarkFolder();
		String fullPath = "";
		// get my bookmarks
		String myPath = Bookmark.SEPARATOR+Bookmark.Folder.USER+Bookmark.SEPARATOR+ctx.getUser().getOid();
		if (path != null) {
			if (!path.startsWith(Bookmark.SEPARATOR)) {
				myPath += Bookmark.SEPARATOR;
			}
			fullPath = myPath+path;
		}
		bf.setId(path);
		
		List<Bookmark> bookmarks = ((BookmarkDAO) factory.getDAO(Bookmark.class)).findByPath(ctx, fullPath);
		if (bookmarks.size() == 0) {
			throw new ObjectNotFoundAPIException("No folder found for path : "+path, ctx.isNoError());
		} else {
			// compute the folders
			List<BookmarkPK> pkList = new ArrayList<BookmarkPK>();
			Set<String> folders = new HashSet<String>();
	        for (Bookmark o : bookmarks) {
	        	String p = o.getPath();
	        	// ignore leading mypath
	        	p = p.substring(myPath.length());
	        	// process the trailing path
				if (p != null) {
					String[] split = p.split(Bookmark.SEPARATOR);
					if (split.length > 0) {
						if (split[0].equals("")) {
							if (split.length > 1) {
								folders.add(split[1]);
							}
						} else {
							folders.add(split[0]);
						}
					} else {
						folders.add(p);
					}
				}
	            pkList.add(o.getId());
	        }
			bf.setBookmarks(pkList);
			bf.setChildren(new ArrayList<>(folders));
			return bf;
		}
	}

}
