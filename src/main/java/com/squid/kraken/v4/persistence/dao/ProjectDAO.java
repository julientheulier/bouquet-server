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

import java.util.List;

import com.google.common.base.Optional;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.caching.Cache;
import com.squid.kraken.v4.caching.CacheFactoryEHCache;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.CustomerPK;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AccessRightsPersistentDAO;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DataStore;
import com.squid.kraken.v4.persistence.DataStoreEvent;

public class ProjectDAO extends AccessRightsPersistentDAO<Project, ProjectPK> {

    private Cache<CustomerPK, List<ProjectPK>> findByCustomerCache;

    public ProjectDAO(DataStore ds) {
        super(Project.class, ds);
        this.findByCustomerCache = CacheFactoryEHCache.getCacheFactory().getCollectionsCache(ProjectPK.class,
                "findByCustomer");
    }

    public List<Project> findByCustomer(AppContext app, CustomerPK customerId) {
        return super.find(app, customerId, null, findByCustomerCache);
    }
    
    @Override
    public void notifyEvent(DataStoreEvent event) {
        ProjectPK id = null;
        if (event.getSource().getClass().equals(ProjectPK.class)) {
            // deletion
            id = (ProjectPK) event.getSource();
            instanceCache.remove(id);
        }
        if (event.getSource() instanceof Project) {
            // creation or update
            Project source = (Project) event.getSource();
            id = source.getId();
            instanceCache.put(id, source);
        }
        if (id != null) {
            // finder cache invalidation
            findByCustomerCache.remove(new CustomerPK(id.getCustomerId()));
        }
    }
    
    
	/**
	 * Role {@link Role#READ} is required to perform the operation but
	 * {@link Role#WRITE} is required to view JDBC attributes.<br>
	 */
    @Override
	public Optional<Project> read(AppContext ctx, ProjectPK id) {
		Optional<Project> object = ds.read(ctx, type, id);
		if (object.isPresent()) {
			// check the access rights
			checkReadRights(ctx, (Project) object.get());
		}
		return object;
	}

	/**
	 * Role {@link Role#READ} is required to perform the operation but
	 * {@link Role#WRITE} is required to view JDBC attributes.<br>
	 */
	@Override
	public Project readNotNull(AppContext ctx, ProjectPK id) {
		Project object = ds.readNotNull(ctx, type, id);
		// check the access rights
		checkReadRights(ctx, (Project) object);
		return object;
	}

	@Override
	public void delete(AppContext ctx, ProjectPK id) {
		// TODO Auto-generated method stub
		super.delete(ctx, id);
	}

	@Override
    public void update(AppContext ctx, Project projectData) {
        if (projectData.getDbPassword() == null) {
            // do not update
            Project existingProject = ds.readNotNull(ctx, type, projectData.getId());
            projectData.setDbPassword(existingProject.getDbPassword());
        }
        super.update(ctx, projectData);
    }

	private void checkReadRights(AppContext ctx, Project project) {
		// check the access rights
		AccessRightsUtils.getInstance().checkRole(ctx, project, Role.READ);
		boolean hasWriteRole = AccessRightsUtils.getInstance().hasRole(
				ctx.getUser(), project.getAccessRights(), Role.WRITE);
		if (project.getDbPassword() != null) {
			project.setDbPasswordLength(project.getDbPassword().length());
		}
		if (!hasWriteRole) {
			// prevent from exposing JDBC attributes
			project.setDbPassword(null);
			project.setDbUrl(null);
			project.setDbUser(null);
			project.setDbSchemas(null);
		} else if (!ctx.getUser().isSuperUser()) {
			project.setDbPassword(null);
		}
	}

}
