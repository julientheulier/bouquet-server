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
package com.squid.kraken.v4.persistence.dao;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.google.common.base.Optional;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.InvalidCredentialsAPIException;
import com.squid.kraken.v4.api.core.AccessRightsUtils.Inheritance;
import com.squid.kraken.v4.caching.Cache;
import com.squid.kraken.v4.caching.CacheFactoryEHCache;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.BookmarkPK;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserPK;
import com.squid.kraken.v4.persistence.AccessRightsPersistentDAO;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.DataStore;
import com.squid.kraken.v4.persistence.DataStoreEvent;
import com.squid.kraken.v4.persistence.DataStoreEventObserver;
import com.squid.kraken.v4.persistence.DataStoreFilterOperator;
import com.squid.kraken.v4.persistence.DataStoreQueryField;

public class BookmarkDAO extends
		AccessRightsPersistentDAO<Bookmark, BookmarkPK> implements
		DataStoreEventObserver {

	private final Cache<ProjectPK, List<BookmarkPK>> findByProjectCache;

	public BookmarkDAO(DataStore ds) {
		super(Bookmark.class, ds);
		findByProjectCache = CacheFactoryEHCache.getCacheFactory()
				.getCollectionsCache(BookmarkPK.class, "findByProject");
	}

	public List<Bookmark> findByParent(AppContext app, ProjectPK projectId) {
		List<DataStoreQueryField> queryFields = new ArrayList<DataStoreQueryField>(
				1);
		queryFields.add(new DataStoreQueryField("id.projectId", projectId
				.getProjectId()));
		return super.find(app, projectId, queryFields, findByProjectCache);
	}

	public List<Bookmark> findByPath(AppContext app, ProjectPK projectId,
			String path) {
		List<DataStoreQueryField> queryFields = new ArrayList<DataStoreQueryField>(
				1);
		List<DataStoreFilterOperator> filterOperators = new ArrayList<DataStoreFilterOperator>();
		if (projectId != null) {
			queryFields.add(new DataStoreQueryField("id.projectId", projectId
					.getProjectId()));
			filterOperators.add(DataStoreFilterOperator.EQUAL);
		}
		queryFields.add(new DataStoreQueryField("path", path));
		filterOperators.add(DataStoreFilterOperator.STARTS_WITH);
		return super.find(app, projectId, queryFields, filterOperators, null);
	}
	
	public List<Bookmark> findByPath(AppContext app,
			String path) {
		return findByPath(app, null, path);
	}
	
	public List<Bookmark> findByOwner(AppContext app) {
		List<DataStoreQueryField> queryFields = new LinkedList<DataStoreQueryField>();
        List<DataStoreFilterOperator> operators = new ArrayList<DataStoreFilterOperator>();
        List<AccessRight> rights = new ArrayList<AccessRight>();
        rights.add(new AccessRight(Role.OWNER, app.getUser().getOid(), null));
        queryFields.add(new DataStoreQueryField("accessRights", rights));
        operators.add(DataStoreFilterOperator.IN);
        return super.find(app, null, queryFields, operators, null);
	}

	@Override
	public Bookmark create(AppContext ctx, Bookmark bookmark) {
		Persistent<? extends GenericPK> parent = bookmark.getParentObject(ctx);
		AccessRightsUtils.getInstance().checkRole(ctx, parent, Role.READ);
		applyPathRules(ctx, parent.getAccessRights(), null, bookmark);
		AccessRightsUtils.getInstance().setAccessRights(ctx, bookmark, parent);
		return ds.create(ctx, bookmark);
	}

	@Override
	public void update(AppContext ctx, Bookmark bookmark) {
		Bookmark toUpdate = ds.read(ctx, type, bookmark.getId()).get();
		AccessRightsUtils.getInstance().checkRole(ctx, toUpdate, Role.WRITE);
		Set<AccessRight> newAccessRights = AccessRightsUtils.getInstance()
				.applyAccessRights(ctx, toUpdate.getAccessRights(),
						bookmark.getAccessRights());
		applyPathRules(ctx, toUpdate.getAccessRights(), toUpdate, bookmark);
		bookmark.setAccessRights(newAccessRights);
		ds.update(ctx, bookmark);
	}

	private void applyPathRules(AppContext ctx, Set<AccessRight> accessRights, Bookmark toUpdate, Bookmark newBookmark) {
		String path = newBookmark.getPath();
		if (path == null) {
			path = "";
		}
		if (path.equals("")) {
			path = path + Bookmark.SEPARATOR;
		}
		boolean isAdmin = AccessRightsUtils.getInstance().hasRole(ctx.getUser(),
				accessRights, Role.WRITE);
		String[] pathSplit = path.split("\\"+Bookmark.SEPARATOR);
		// T2184
		if (toUpdate!=null) {
			boolean isOwner = AccessRightsUtils.getInstance().hasRole(ctx.getUser(),
					accessRights, Role.OWNER);
			if (!isOwner) {
				// if not an owner, cannot change the root path
				String[] originalPathSplit = toUpdate.getPath().split("\\"+Bookmark.SEPARATOR);
				if (originalPathSplit.length<=1) {
					if (pathSplit.length>1) {
						throw new InvalidCredentialsAPIException(
								"User cannot move the bookmark root folder", ctx.isNoError());
					}
				} else {
					if (originalPathSplit[1].equals(Bookmark.Folder.SHARED.name())) {
						if (!pathSplit[1].equals(Bookmark.Folder.SHARED.name())) {
							throw new InvalidCredentialsAPIException(
									"User cannot move the bookmark root folder", ctx.isNoError());
						}
					}
					if (originalPathSplit[1].equals(Bookmark.Folder.USER.name())) {
						if (!pathSplit[1].equals(Bookmark.Folder.USER.name())) {
							throw new InvalidCredentialsAPIException(
									"User cannot move the bookmark root folder", ctx.isNoError());
						} else {
							// check OID
							String pathUserId = pathSplit.length>2?pathSplit[2]:"";
							String originalPathUserId = originalPathSplit.length>2?originalPathSplit[2]:"";
							if (!originalPathUserId.equals(pathUserId)) {
								throw new InvalidCredentialsAPIException(
										"User cannot move the bookmark root folder", ctx.isNoError());
							}
						}
					}
				}
			}
		}
		if (pathSplit.length>1) {
			// check if path is a USER path
			boolean isUserPath = pathSplit[1].equals(Bookmark.Folder.USER.name());
			if (isUserPath) {
				// get the userID
				String pathUserId = pathSplit.length>2?pathSplit[2]:"";
				if (pathUserId.equals(ctx.getUser().getOid())) {
					// path user id is current user : keep it like it is
				} else {
					// check if this is a valid user id
					DAOFactory
							.getDAOFactory()
							.getDAO(User.class)
							.readNotNull(ctx,
									new UserPK(ctx.getCustomerId(), pathUserId));
					// check if ctx user is admin
					if (!isAdmin) {
						throw new InvalidCredentialsAPIException(
								"User cannot write in /USER path", ctx.isNoError());
					}
				}
			} else {
				// check if path is SHARED path
				if (!pathSplit[1].equals(Bookmark.Folder.SHARED.name())) {
					// not shared
					if (path.charAt(0) != Bookmark.SEPARATOR.charAt(0)) {
						path = Bookmark.SEPARATOR + path;
					}
					// force current user's path
					path = Bookmark.SEPARATOR + Bookmark.Folder.USER
							+ Bookmark.SEPARATOR + ctx.getUser().getOid() + path;
				} else if (!isAdmin) {
					throw new InvalidCredentialsAPIException(
							"User cannot write in " + Bookmark.Folder.SHARED, ctx.isNoError());
				}
			}
		} else {
			// force current user's path
			path = Bookmark.SEPARATOR + Bookmark.Folder.USER
					+ Bookmark.SEPARATOR + ctx.getUser().getOid() + path;
		}
		// remove any leading slash
		if (path.endsWith(Bookmark.SEPARATOR)) {
			path = path.substring(0, path.length()-1);
		}
		newBookmark.setPath(path);
	}

	@Override
	public void notifyEvent(DataStoreEvent event) {
		BookmarkPK id = null;
		if (event.getSource().getClass().equals(BookmarkPK.class)) {
			// deletion
			id = (BookmarkPK) event.getSource();
			instanceCache.remove(id);
		}
		if (event.getSource() instanceof Bookmark) {
			// creation or update
			Bookmark source = (Bookmark) event.getSource();
			id = source.getId();
			instanceCache.put(id, source);
		}
		if (id != null) {
			// finder cache invalidation
			findByProjectCache.remove(new ProjectPK(id.getCustomerId(), id
					.getProjectId()));
		}
	}
	
	/**
	 * override in order to apply special bookmark rules
	 */
	@Override
	public Optional<Bookmark> read(AppContext ctx, BookmarkPK id) {
		Optional<Bookmark> object = ds.read(ctx, type, id);
		if (object.isPresent()) {
			// check is based on the path
			if (!checkBookmarkRole(ctx, object.get())) {
				throw new InvalidCredentialsAPIException(
					"Insufficient privileges : caller hasn't READ role on " + object.get().getId(), ctx.isNoError());
			}
		}
		return super.read(ctx, id);
	}
	
	private boolean checkBookmarkRole(AppContext ctx, Bookmark object) {
		String path = object.getPath();
		if (path==null) {
			// apply default
			return AccessRightsUtils.getInstance().hasRole(ctx, object,
					Role.READ);
		} else if (path.startsWith(Bookmark.SEPARATOR + Bookmark.Folder.SHARED)) {
			// this is a shared bookmark, anyone with at least execute right on the project can access
			return AccessRightsUtils.getInstance().hasRole(ctx, object,
					Role.EXECUTE);
		} else if (path.startsWith(Bookmark.SEPARATOR + Bookmark.Folder.USER)) {
			// need to check the oid
			if (path.startsWith(Bookmark.SEPARATOR + Bookmark.Folder.USER + Bookmark.SEPARATOR + ctx.getUser().getOid() + Bookmark.SEPARATOR)
			 || (path.equals(Bookmark.SEPARATOR + Bookmark.Folder.USER + Bookmark.SEPARATOR + ctx.getUser().getOid())))
			 {
				// just always OK
				return true;
			} else {
				// check if it is explicitly shared with me
				final Role role = Role.EXECUTE;
				return AccessRightsUtils.getInstance().hasRole(ctx.getUser(), object.getAccessRights(), role, 
						Inheritance.NONE);
			}
		} else {
			// apply default
			return AccessRightsUtils.getInstance().hasRole(ctx, object,
					Role.READ);
		}
	}

}
