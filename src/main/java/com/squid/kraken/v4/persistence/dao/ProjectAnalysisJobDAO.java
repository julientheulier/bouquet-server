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

import com.squid.kraken.v4.caching.Cache;
import com.squid.kraken.v4.caching.CacheFactoryEHCache;
import com.squid.kraken.v4.model.DataTable;
import com.squid.kraken.v4.model.ProjectAnalysisJob;
import com.squid.kraken.v4.model.ProjectAnalysisJobPK;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DataStore;
import com.squid.kraken.v4.persistence.DataStoreEvent;
import com.squid.kraken.v4.persistence.DataStoreEventObserver;
import com.squid.kraken.v4.persistence.DataStoreQueryField;

public class ProjectAnalysisJobDAO extends JobDAO<ProjectAnalysisJob, ProjectAnalysisJobPK> implements
        DataStoreEventObserver {

    private final Cache<ProjectPK, List<ProjectAnalysisJobPK>> findByProjectCache;

    public ProjectAnalysisJobDAO(DataStore ds) {
        super(ProjectAnalysisJob.class, ds);
        findByProjectCache = CacheFactoryEHCache.getCacheFactory().getCollectionsCache(ProjectAnalysisJobPK.class,
                "findByProject");
    }

    public List<ProjectAnalysisJob> findByProject(AppContext app, ProjectPK projectId) {
        List<DataStoreQueryField> queryFields = new ArrayList<DataStoreQueryField>(1);
        queryFields.add(new DataStoreQueryField("id.projectId", projectId.getProjectId()));
        return super.find(app, projectId, queryFields, findByProjectCache);
    }

    @Override
    public void notifyEvent(DataStoreEvent event) {
        ProjectAnalysisJobPK id = null;
        if (event.getSource() instanceof ProjectAnalysisJobPK) {
            // deletion
            id = (ProjectAnalysisJobPK) event.getSource();
            instanceCache.remove(id);
        }
        if (event.getSource() instanceof ProjectAnalysisJob) {
            // creation or update
            ProjectAnalysisJob source = (ProjectAnalysisJob) event.getSource();
            id = source.getId();
            // prevent from caching the job results
            DataTable results = source.getResults();
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
	public ProjectAnalysisJob create(AppContext ctx,
			ProjectAnalysisJob job) {
		job.setSelection(getCleanFacetSelection(job.getSelection()));
		return super.create(ctx, job);
	}

	@Override
	public void update(AppContext ctx, ProjectAnalysisJob job) {
		job.setSelection(getCleanFacetSelection(job.getSelection()));
		super.update(ctx, job);
	}

}
