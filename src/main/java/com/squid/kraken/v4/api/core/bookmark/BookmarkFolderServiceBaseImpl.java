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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;

import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.model.AccessRight.Role;
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
	private String buildBookmarksPath(AppContext ctx, String path) {
		String fullPath;
		if (path == null || path.equals("")) {
			// get my bookmarks
			String myPath = Bookmark.SEPARATOR + Bookmark.Folder.USER
					+ Bookmark.SEPARATOR + ctx.getUser().getOid();
			fullPath = myPath;
		} else {
			fullPath = path;
		}
		return fullPath;
	}

	private List<Bookmark> getBookmarks(AppContext ctx, String path, boolean isRoot) {
		List<Bookmark> bookmarks = ((BookmarkDAO) factory
				.getDAO(Bookmark.class)).findByPath(ctx, path);
		if ((bookmarks.size() == 0) && (!isRoot)) {
			// T1705: return an empty list if no bookmarks instad of an error
			return Collections.emptyList();
		} else {
			return bookmarks;
		}
	}
	
	private BookmarkFolder getMyBookmarksFolder(AppContext ctx, boolean folders, boolean bookmarks) {
		// this is actually the user/OID folder
		BookmarkFolder bf = new BookmarkFolder();
		String fullPath = BookmarkFolder.MYBOOKMARKS;
		String bookmarkFolderOid = genOID(fullPath);
		bf.setId(new BookmarkFolderPK(ctx.getCustomerId(), bookmarkFolderOid));
		bf.setName("/My Bookmarks");// we don't really care
		if (folders) {
			bf.setFolders(readFolders(ctx, BookmarkFolder.MYBOOKMARKS, folders, bookmarks));
		}
		if (bookmarks) {
			String internalPath = convertMyBookmarksToInternalPath(ctx, fullPath);
			bf.setBookmarks(readBookmarks(ctx, fullPath, internalPath));
		}
		return bf;
	}

	private BookmarkFolder getSharedWithMeFolder(AppContext ctx, boolean folders, boolean bookmarks) {
		// this is actually the user/OID folder
		BookmarkFolder bf = new BookmarkFolder();
		String fullPath = BookmarkFolder.SHAREDWITHME;
		String bookmarkFolderOid = genOID(fullPath);
		bf.setId(new BookmarkFolderPK(ctx.getCustomerId(), bookmarkFolderOid));
		bf.setName("/Shared with me");// we don't really care
		if (folders) {
			bf.setFolders(readFolders(ctx, BookmarkFolder.MYBOOKMARKS, folders, bookmarks));
		}
		return bf;
	}

	private BookmarkFolder getSharedFolder(AppContext ctx, boolean folders, boolean bookmarks) {
		// this is actually the user/OID folder
		BookmarkFolder bf = new BookmarkFolder();
		String fullPath = BookmarkFolder.SHARED;
		String bookmarkFolderOid = genOID(fullPath);
		bf.setId(new BookmarkFolderPK(ctx.getCustomerId(), bookmarkFolderOid));
		bf.setName("/Public");// we don't really care
		if (folders) {
			bf.setFolders(readFolders(ctx, BookmarkFolder.MYBOOKMARKS, folders, bookmarks));
		}
		return bf;
	}

	public BookmarkFolder read(AppContext ctx, String path) {
		if (path==null || path.equals("") || path.equals(BookmarkFolder.ROOT)) {
			// create the ROOT folder
			BookmarkFolder bf = new BookmarkFolder();
			String fullPath = BookmarkFolder.ROOT;
			String bookmarkFolderOid = genOID(fullPath);
			bf.setId(new BookmarkFolderPK(ctx.getCustomerId(), bookmarkFolderOid));
			bf.setName("/");// we don't really care
			return bf;
		} else if (path.equals(BookmarkFolder.MYBOOKMARKS)) {
			return getMyBookmarksFolder(ctx, false, true);
		} else if (path.equals(BookmarkFolder.SHAREDWITHME)) {
			return getSharedWithMeFolder(ctx,false, true);
		} else if (path.equals(BookmarkFolder.SHARED)) {
			return getSharedFolder(ctx, false, true);
		} else if (path.startsWith(BookmarkFolder.MYBOOKMARKS)) {
			String internalPath = convertMyBookmarksToInternalPath(ctx, path);
			return readInternal(ctx, path, internalPath);
		} else if (path.startsWith(BookmarkFolder.SHAREDWITHME)) {
			return readSharedWithMeInternal(ctx, path);
		} else if (path.startsWith(BookmarkFolder.SHARED)) {
			String internalPath = convertSharedToInternalPath(ctx, path);
			return readInternal(ctx, path, internalPath);
		} else if (path.startsWith(Bookmark.SEPARATOR)) {
			// try the regular path?
			// in this case internal path is the actual path
			return readInternal(ctx, path, path);
		} else {
			throw new ObjectNotFoundAPIException("undefined bookmark path", true);
		}
	}
	
	private BookmarkFolder readInternal(AppContext ctx, String path, String internalPath) {
		BookmarkFolder bf = new BookmarkFolder();
		// for real folder, using the internal path
		String bookmarkFolderOid = genOID(path);
		bf.setId(new BookmarkFolderPK(ctx.getCustomerId(), bookmarkFolderOid));
		if (path != null) {
			bf.setName(buildFolderName(ctx, path));
		}
		bf.setBookmarks(readBookmarks(ctx, path, internalPath));
		return bf;
	}
	
	private List<BookmarkLink> readBookmarks(AppContext ctx, String path, String internalPath) {
		List<Bookmark> bookmarks = getBookmarks(ctx, internalPath, path == null);
		// build the folder content
		List<BookmarkLink> links = new ArrayList<BookmarkLink>();
		for (Bookmark o : bookmarks) {
			String p = o.getPath();
			// only handle the exact path
			if (p.equals(internalPath)) {
				BookmarkLink bm = new BookmarkLink(o.getId());
				bm.setName(o.getName());
				bm.setDescription(o.getDescription());
				links.add(bm);
			}
		}
		return links;
	}
	
	private BookmarkFolder readSharedWithMeInternal(AppContext ctx, String path) {
		String internalPath = Bookmark.SEPARATOR + Bookmark.Folder.USER;
		String userPath = "/"+ctx.getUser().getOid();
		String filterPath = path.substring(BookmarkFolder.SHAREDWITHME.length());
		BookmarkFolder bf = new BookmarkFolder();
		String bookmarkFolderOid = genOID(path);
		bf.setId(new BookmarkFolderPK(ctx.getCustomerId(), bookmarkFolderOid));
		if (path != null) {
			bf.setName(buildFolderName(ctx, path));
		}
		List<Bookmark> bookmarks = getBookmarks(ctx, internalPath, path == null);
		// build the folder content
		List<BookmarkLink> bmList = new ArrayList<BookmarkLink>();
		for (Bookmark o : bookmarks) {
			String p = o.getPath();
			// only handle the exact pathp = p.substring(internalPath.length());
			p = p.substring(internalPath.length());
			if (!p.startsWith(userPath)) {// excluding my bookmarks
				String subPath = getSubPath(p);
				if (filterPath.equals("") || subPath.equals(filterPath)) {
					BookmarkLink bm = new BookmarkLink(o.getId());
					bm.setName(o.getName());
					bm.setDescription(o.getDescription());
					bmList.add(bm);
				}
			}
		}
		bf.setBookmarks(bmList);
		return bf;
	}
	
	private String genOID(String path) {
		return Base64.encodeBase64URLSafeString(path.getBytes());
	}
	
	// this is the legacy method
	public List<BookmarkFolder> readFolders(AppContext ctx, String path) {
		return readFolders(ctx, path, false, true);
	}

	public List<BookmarkFolder> readFolders(AppContext ctx, String path, boolean folders, boolean bookmarks) {
		if (path==null || path.equals("") || path.equals(BookmarkFolder.ROOT)) {
			// create fake folders for MyBokmarks and SharedWithMe
			List<BookmarkFolder> bfList = new ArrayList<BookmarkFolder>();
			bfList.add(getMyBookmarksFolder(ctx, folders, bookmarks));
			bfList.add(getSharedWithMeFolder(ctx, folders, bookmarks));
			// disabling Shared folder for now...
			//bfList.add(getSharedFolder(ctx, folders, bookmarks));
			return bfList;
		} else if (path.startsWith(BookmarkFolder.MYBOOKMARKS)) {
			return readMyBookmarkFolders(ctx, path, folders, bookmarks);
		} else if (path.startsWith(BookmarkFolder.SHAREDWITHME)) {
			return readSharedWithMeFolders(ctx, path, folders, bookmarks);
		} else if (path.startsWith(BookmarkFolder.SHARED)) {
			return readSharedFolders(ctx, path, folders, bookmarks);
		} else {
			throw new ObjectNotFoundAPIException("undefined bookmark path", true);
		}
	}
	
	protected String getInternalUserPath(AppContext ctx) {
		return Bookmark.SEPARATOR + Bookmark.Folder.USER
				+ Bookmark.SEPARATOR + ctx.getUser().getOid();
	}
	
	protected String convertMyBookmarksToInternalPath(AppContext ctx, String path) {
		String folderPath = path.substring(BookmarkFolder.MYBOOKMARKS.length());
		return getInternalUserPath(ctx)+folderPath;
	}
	
	protected String convertSharedToInternalPath(AppContext ctx, String path) {
		String folderPath = path.substring(BookmarkFolder.SHARED.length());
		return "/SHARED"+folderPath;
	}
	
	protected List<BookmarkFolder> readMyBookmarkFolders(AppContext ctx, String path, boolean folders, boolean bookmarks) {
		String internalPath = convertMyBookmarksToInternalPath(ctx, path);
		return readInternalFolders(ctx, path, internalPath, folders, bookmarks);
	}
	
	protected List<BookmarkFolder> readSharedFolders(AppContext ctx, String path, boolean folders, boolean bookmarks) {
		String internalPath = convertSharedToInternalPath(ctx, path);
		return readInternalFolders(ctx, path, internalPath, folders, bookmarks);
	}
	
	protected List<BookmarkFolder> readInternalFolders(AppContext ctx, String path, String internalPath, boolean dofolders, boolean dobookmarks) {
		List<BookmarkFolder> bfList = new ArrayList<BookmarkFolder>();
		List<Bookmark> bookmarks = getBookmarks(ctx, internalPath, path == null);
		// compute the folders
		List<BookmarkPK> pkList = new ArrayList<BookmarkPK>();
		Set<String> folders = new HashSet<String>();
		for (Bookmark o : bookmarks) {
			String p = o.getPath();
			// ignore leading mypath
			p = p.substring(internalPath.length());
			// process the trailing path
			if (p != null && !p.equals("")) {
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
			String folderPath = path + Bookmark.SEPARATOR + s;
			String folderInternalPath = internalPath + Bookmark.SEPARATOR + s;
			if (dobookmarks) {
				BookmarkFolder bf = readInternal(ctx, folderPath, folderInternalPath);
				bfList.add(bf);
			} else {
				bfList.add(createEmptyFolder(ctx, folderPath));
			}
		}
		return bfList;
	}
	
	private String getSubPath(String path) {
		// remove the user OID part (first part)
		int pos = path.indexOf("/",1);
		if (pos>=0) {
			return path.substring(pos);
		} else {
			return "";
		}
	}
	
	protected List<BookmarkFolder> readSharedWithMeFolders(AppContext ctx, String path, boolean folders2, boolean bookmarks2) {
		String internalPath = Bookmark.SEPARATOR + Bookmark.Folder.USER;
		String userPath = "/"+ctx.getUser().getOid();
		String filterPath = path.substring(BookmarkFolder.SHAREDWITHME.length());
		List<BookmarkFolder> bfList = new ArrayList<BookmarkFolder>();
		List<Bookmark> bookmarks = getBookmarks(ctx, internalPath, path == null);
		// compute the folders
		Set<String> folders = new HashSet<String>();
		for (Bookmark o : bookmarks) {
			String p = o.getPath();
			// ignore leading mypath
			p = p.substring(internalPath.length());
			if (!p.startsWith(userPath)) {// excluding my bookmarks
				String subPath = getSubPath(p);
				if (filterPath.equals("") || subPath.startsWith(filterPath)) {
					// process the trailing path
					String trailing = !filterPath.equals("")?subPath.substring(filterPath.length()):subPath;
					if (trailing != null) {
						String[] split = trailing.split(Bookmark.SEPARATOR);
						if (split.length > 1) {
							folders.add(split[1]);
						} else {
							//folders.add("");
						}
					}
				}
			}
		}
		// build the BookmarkFolder list
		for (String s : folders) {
			String folderPath = path + Bookmark.SEPARATOR + s;
			bfList.add(createEmptyFolder(ctx, folderPath));
		}
		return bfList;
	}
	
	private BookmarkFolder createEmptyFolder(AppContext ctx, String path) {
		BookmarkFolder bf = new BookmarkFolder();
		String bookmarkFolderOid = genOID(path);
		bf.setId(new BookmarkFolderPK(ctx.getCustomerId(), bookmarkFolderOid));
		if (path != null) {
			bf.setName(buildFolderName(ctx, path));
		}
		return bf;
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
	
	/**
	 * this method list the bookmarks shared with the current user
	 * @param ctx
	 * @return
	 */
	public List<Bookmark> listSharedBookmarks(AppContext ctx, String path) {
		List<Bookmark> result = new ArrayList<>();
		// this is someone else bookmark, so we can only filter by /USER/
		String usersPath = Bookmark.SEPARATOR + Bookmark.Folder.USER
			+ Bookmark.SEPARATOR;
		List<Bookmark> bookmarks = ((BookmarkDAO) factory
				.getDAO(Bookmark.class)).findByPath(ctx, usersPath);
		// this is my bookmarks - but we don't want them
		String myPath = usersPath + ctx.getUser().getOid();
		for (Bookmark bookmark : bookmarks) {
			if (!bookmark.getPath().startsWith(myPath)) {// not mine
				// do user have access to it?
				if (AccessRightsUtils.getInstance().hasRole(ctx, bookmark, Role.READ)) {
					result.add(bookmark);
				}
			}
		}
		return result;
	}

}
