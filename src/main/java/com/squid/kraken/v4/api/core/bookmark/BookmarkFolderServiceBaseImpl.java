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

import org.apache.commons.codec.binary.Base64;

import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.BookmarkFolder;
import com.squid.kraken.v4.model.BookmarkFolder.BookmarkLink;
import com.squid.kraken.v4.model.BookmarkFolderPK;
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

	/**
	 * Build a decoded path.
	 * 
	 * @param ctx
	 * @param pathBase64
	 *            a base64 encoded path or null
	 * @return decoded path or current user path
	 */
	private String buildBookmarksPath(AppContext ctx, String pathBase64) {
		String fullPath;
		if (pathBase64 == null) {
			// get my bookmarks
			String myPath = Bookmark.SEPARATOR + Bookmark.Folder.USER
					+ Bookmark.SEPARATOR + ctx.getUser().getOid();
			fullPath = myPath;
		} else {
			fullPath = new String(Base64.decodeBase64(pathBase64));
		}
		return fullPath;
	}

	private List<Bookmark> getBookmarks(AppContext ctx, String path, boolean isRoot) {
		List<Bookmark> bookmarks = ((BookmarkDAO) factory
				.getDAO(Bookmark.class)).findByPath(ctx, path);
		if ((bookmarks.size() == 0) && (!isRoot)) {
			throw new ObjectNotFoundAPIException("No folder found for path : "
					+ path, ctx.isNoError());
		} else {
			return bookmarks;
		}
	}

	public BookmarkFolder read(AppContext ctx, String pathBase64) {
		BookmarkFolder bf = new BookmarkFolder();
		String fullPath = buildBookmarksPath(ctx, pathBase64);
		String bookmarkFolderOid = Base64.encodeBase64URLSafeString(fullPath
				.getBytes());
		bf.setId(new BookmarkFolderPK(ctx.getCustomerId(), bookmarkFolderOid));
		if (pathBase64 != null) {
			bf.setName(buildFolderName(ctx, fullPath));
		}
		List<Bookmark> bookmarks = getBookmarks(ctx, fullPath, pathBase64 == null);
		// build the folder content
		List<BookmarkLink> bmList = new ArrayList<BookmarkLink>();
		for (Bookmark o : bookmarks) {
			String p = o.getPath();
			// only handle the exact path
			if (p.equals(fullPath)) {
				BookmarkLink bm = new BookmarkLink(o.getId());
				bm.setName(o.getName());
				bm.setDescription(o.getDescription());
				bmList.add(bm);
			}
		}
		bf.setBookmarks(bmList);
		return bf;
	}

	public List<BookmarkFolder> readFolders(AppContext ctx, String pathBase64) {
		List<BookmarkFolder> bfList = new ArrayList<BookmarkFolder>();
		String fullPath = buildBookmarksPath(ctx, pathBase64);
		List<Bookmark> bookmarks = getBookmarks(ctx, fullPath, pathBase64 == null);

		// compute the folders
		List<BookmarkPK> pkList = new ArrayList<BookmarkPK>();
		Set<String> folders = new HashSet<String>();
		for (Bookmark o : bookmarks) {
			String p = o.getPath();
			// ignore leading mypath
			p = p.substring(fullPath.length());
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
		// build the BookmarkFolder list
		for (String s : folders) {
			String folderPath = fullPath + Bookmark.SEPARATOR + s;
			String bookmarkFolderOid = Base64
					.encodeBase64URLSafeString(folderPath.getBytes());
			BookmarkFolder bf = new BookmarkFolder(new BookmarkFolderPK(
					ctx.getCustomerId(), bookmarkFolderOid));
			bf.setName(buildFolderName(ctx, folderPath));
			bfList.add(bf);
		}
		return bfList;
	}

	private String buildFolderName(AppContext ctx, String fullPath) {
		// set the name
		String userPath = buildBookmarksPath(ctx, null);
		int idx = fullPath.indexOf(userPath);
		if (idx == 0) {
			return fullPath.substring(userPath.length());
		} else {
			return fullPath.substring(fullPath.lastIndexOf(Bookmark.SEPARATOR));
		}
	}

}
