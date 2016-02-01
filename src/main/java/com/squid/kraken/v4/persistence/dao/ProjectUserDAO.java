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

import com.google.common.base.Optional;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.model.ProjectUser;
import com.squid.kraken.v4.model.ProjectUserPK;
import com.squid.kraken.v4.persistence.AccessRightsPersistentDAO;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DataStore;
import com.squid.kraken.v4.persistence.DataStoreEvent;

public class ProjectUserDAO extends
		AccessRightsPersistentDAO<ProjectUser, ProjectUserPK> {

	/**
	 * Constructor with data store
	 * 
	 * @param ds
	 *            data store
	 */
	public ProjectUserDAO(DataStore ds) {
		super(ProjectUser.class, ds);
	}

	@Override
	public ProjectUser create(AppContext ctx, ProjectUser newInstance) {
		// in a project, everyone whose access rights >= READ can create an
		// annotation
		Persistent<? extends GenericPK> parent = newInstance
				.getParentObject(ctx);
		AccessRightsUtils.getInstance().checkRole(ctx, parent, Role.READ);
		AccessRightsUtils.getInstance().setAccessRights(ctx, newInstance, parent);
		return ds.create(ctx, newInstance);
	}

	@Override
	public ProjectUser readNotNull(AppContext ctx, ProjectUserPK id) {
		// in a project, everyone whose access righs defined in the project can
		// read an annotation
		Optional<ProjectUser> optionalObject = ds.read(ctx, type, id);
		if (optionalObject.isPresent()) {
			ProjectUser object = optionalObject.get();
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
	public void delete(AppContext ctx, ProjectUserPK id) {
		// in a project, the author of the annotation can delete it,
		// other users whose access right >= WRITE can delete it
		String ctxUserId = ctx.getUser().getId().getUserId();
		Persistent<? extends GenericPK> parent = null;

		// check access right
		// get the related project to verify access rights
		ProjectUser projectUser = (ProjectUser) ds.read(ctx, type, id).get();
		parent = projectUser.getParentObject(ctx);
		if (!ctxUserId.equalsIgnoreCase(id.getUserId())) { 
			// not author --> WRITE requested
			AccessRightsUtils.getInstance().checkRole(ctx, parent, Role.WRITE);
		} else { 
			// author of the annotation --> READ enough
			AccessRightsUtils.getInstance().checkRole(ctx, parent, Role.READ);
		}

		ds.delete(ctx, type, id);
	}

	@Override
	public void notifyEvent(DataStoreEvent event) {
		ProjectUserPK id = null;
		if (event.getSource() instanceof ProjectUserPK) {
			// deletion
			id = (ProjectUserPK) event.getSource();
			instanceCache.remove(id);
		}
		if (event.getSource() instanceof ProjectUser) {
			// creation or update
			ProjectUser source = (ProjectUser) event.getSource();
			id = source.getId();
			instanceCache.put(id, source);
		}
	}

}
