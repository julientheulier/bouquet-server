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

import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.caching.Cache;
import com.squid.kraken.v4.caching.CacheFactoryEHCache;
import com.squid.kraken.v4.model.FacetSelection;
import com.squid.kraken.v4.model.ProjectFacetJob;
import com.squid.kraken.v4.model.ProjectFacetJobPK;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DataStore;
import com.squid.kraken.v4.persistence.DataStoreEvent;
import com.squid.kraken.v4.persistence.DataStoreQueryField;

public class ProjectFacetJobDAO extends JobDAO<ProjectFacetJob, ProjectFacetJobPK> {

    private final Cache<ProjectPK, List<ProjectFacetJobPK>> findByProjectCache;

    public ProjectFacetJobDAO(DataStore ds) {
        super(ProjectFacetJob.class, ds);
        findByProjectCache = CacheFactoryEHCache.getCacheFactory().getCollectionsCache(ProjectFacetJobPK.class,
        "findByProject");
    }

    public List<ProjectFacetJob> findByProject(AppContext app, ProjectPK projectId) {
        List<DataStoreQueryField> queryFields = new ArrayList<DataStoreQueryField>(1);
        queryFields.add(new DataStoreQueryField("id.projectId", projectId.getProjectId()));
        return super.find(app, projectId, queryFields, findByProjectCache);
    }

    @Override
    public void notifyEvent(DataStoreEvent event) {
        ProjectFacetJobPK id = null;
        if (event.getSource() instanceof ProjectFacetJobPK) {
            // deletion
            id = (ProjectFacetJobPK) event.getSource();
            instanceCache.remove(id);
        }
        if (event.getSource() instanceof ProjectFacetJob) {
            // creation or update
            ProjectFacetJob source = (ProjectFacetJob) event.getSource();
            id = source.getId();
            // prevent from caching the job results
            FacetSelection results = source.getResults();
            source.setResults(null);
            instanceCache.put(id, source);
            source.setResults(results);
        }
        if (id != null) {
            // finder cache invalidation
            findByProjectCache.remove(new ProjectPK(id.getCustomerId(), id.getProjectId()));
        }
    }

    @Override
    public ProjectFacetJob create(AppContext ctx, ProjectFacetJob job) {
        if (job.getDomains() == null || (job.getDomains().isEmpty())) {
            throw new APIException("Domains must be set", ctx.isNoError());
        }
        job.setSelection(getCleanFacetSelection(job.getSelection()));
        return super.create(ctx, job);
    }

    @Override
    public void update(AppContext ctx, ProjectFacetJob job) {
        if (job.getDomains() == null || (job.getDomains().isEmpty())) {
            throw new APIException("Domains must be set", ctx.isNoError());
        }
        job.setSelection(getCleanFacetSelection(job.getSelection()));
        super.update(ctx, job);
    }
    
    

}
