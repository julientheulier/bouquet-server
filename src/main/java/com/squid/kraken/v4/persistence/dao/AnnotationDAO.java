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
import java.util.List;

import com.google.common.base.Optional;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.caching.Cache;
import com.squid.kraken.v4.caching.CacheFactoryEHCache;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Annotation;
import com.squid.kraken.v4.model.AnnotationPK;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.persistence.AccessRightsPersistentDAO;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DataStore;
import com.squid.kraken.v4.persistence.DataStoreEvent;
import com.squid.kraken.v4.persistence.DataStoreQueryField;

public class AnnotationDAO extends
		AccessRightsPersistentDAO<Annotation, AnnotationPK> {

	private final Cache<ProjectPK, List<AnnotationPK>> findByProjectCache;

	/**
	 * Constructor with data store
	 * 
	 * @param ds
	 *            data store
	 */
	public AnnotationDAO(DataStore ds) {
		super(Annotation.class, ds);
		findByProjectCache = CacheFactoryEHCache.getCacheFactory()
				.getCollectionsCache(AnnotationPK.class, "findByProject");
	}

	@Override
	public Annotation create(AppContext ctx, Annotation newInstance) {
		// in a project, everyone whose access rights >= READ can create an
		// annotation
		Persistent<? extends GenericPK> parent = newInstance
				.getParentObject(ctx);
		AccessRightsUtils.getInstance().checkRole(ctx, parent, Role.READ);
		AccessRightsUtils.getInstance().setAccessRights(ctx, newInstance, parent);
		return ds.create(ctx, newInstance);
	}

	@Override
	public Annotation readNotNull(AppContext ctx, AnnotationPK id) {
		// in a project, everyone whose access righs defined in the project can
		// read an annotation
		Optional<Annotation> optionalObject = ds.read(ctx, type, id);
		if (optionalObject.isPresent()) {
			Annotation object = optionalObject.get();
			if (object != null) {
				Persistent<? extends GenericPK> parent = object
						.getParentObject(ctx);
				AccessRightsUtils.getInstance().checkRole(ctx, parent,
						Role.READ);
				return object;
			}
		}
		throw new ObjectNotFoundAPIException(String.format(
				"Object %s (id: %s) does not exist.", type, id.toString()),
				true);
	}

	@Override
	public void update(AppContext ctx, Annotation newInstance) {
		// in a project, the author of the annotation can update it,
		// other users whose access right >= WRITE can update it
		Persistent<? extends GenericPK> parent = newInstance
				.getParentObject(ctx);

		User author = newInstance.getAuthor();
		if (author == null) {
			throw new IllegalArgumentException(
					"Author of the annotation is NULL. It should not.");
		}
		String authorId = author.getId().getUserId();
		if (!ctx.getUser().getId().getUserId().equals(authorId)) {
			// not author --> WRITE requested
			AccessRightsUtils.getInstance().checkRole(ctx, parent, Role.WRITE);
		} else {
			// author of the annotation --> READ enough
			AccessRightsUtils.getInstance().checkRole(ctx, parent, Role.READ);
		}
		AccessRightsUtils.getInstance().setAccessRights(ctx, newInstance, parent);
		ds.update(ctx, newInstance);
	}

	@Override
	public void delete(AppContext ctx, AnnotationPK id) {
		// in a project, the author of the annotation can delete it,
		// other users whose access right >= WRITE can delete it
		String ctxUserId = ctx.getUser().getId().getUserId();
		Persistent<? extends GenericPK> parent = null;

		// check access right for Annotation
		Annotation annotation = ds.read(ctx, type, id).get();
		User author = annotation.getAuthor();
		if (author == null) {
			throw new IllegalArgumentException(
					"Author of the annotation is NULL. It should not.");
		}
		String authorId = author.getId().getUserId();
		parent = annotation.getParentObject(ctx);
		if (!ctxUserId.equalsIgnoreCase(authorId)) {
			// not author --> WRITE requested
			AccessRightsUtils.getInstance().checkRole(ctx, parent, Role.WRITE);
		} else {
			// author of the annotation --> READ enough
			AccessRightsUtils.getInstance().checkRole(ctx, parent, Role.READ);
		}

		ds.delete(ctx, type, id);
	}

	/**
	 * Find a list of annotations of a given project.
	 * 
	 * @param app
	 *            app context
	 * @param projectId
	 *            project id
	 * @param orderBy
	 *            order the result. Example:
	 *            <ul>
	 *            <li>age</li>
	 *            <li>-age (descending order)</li>
	 *            <li>age,date</li>
	 *            <li>age,-date (age ascending, date descending)</li>
	 *            </ul>
	 * @return list of annotations
	 */
	public List<Annotation> findByProject(AppContext app, ProjectPK projectPk,
			String orderBy) {
		List<DataStoreQueryField> queryFields = new ArrayList<DataStoreQueryField>(1);
		queryFields.add(new DataStoreQueryField("id.projectId", projectPk
				.getProjectId()));
		return super.find(app, projectPk, queryFields, findByProjectCache,
				orderBy);
	}

	@Override
	public void notifyEvent(DataStoreEvent event) {
		AnnotationPK id = null;
		if (event.getSource() instanceof AnnotationPK) {
			// deletion
			id = (AnnotationPK) event.getSource();
			instanceCache.remove(id);
		}
		if (event.getSource() instanceof Annotation) {
			// creation or update
			Annotation source = (Annotation) event.getSource();
			id = source.getId();
			instanceCache.put(id, source);
		}
		if (id != null) {
			// finder cache invalidation
			findByProjectCache.remove(new ProjectPK(id.getCustomerId(), id
					.getProjectId()));
		}
	}

}
