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
package com.squid.kraken.v4.api.core.simpleanalysisjob;

import java.util.List;

import org.apache.commons.lang3.StringEscapeUtils;

import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.RenderingException;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.model.DataTable;
import com.squid.kraken.v4.model.ProjectAnalysisJob;
import com.squid.kraken.v4.model.ProjectAnalysisJobPK;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.dao.ProjectAnalysisJobDAO;

public class AnalysisJobServiceBaseImpl extends JobServiceBaseImpl<ProjectAnalysisJob, ProjectAnalysisJobPK, DataTable> {

    private static AnalysisJobServiceBaseImpl instance;

    public static AnalysisJobServiceBaseImpl getInstance() {
        if (instance == null) {
            instance = new AnalysisJobServiceBaseImpl();
        }
        return instance;
    }

    private AnalysisJobServiceBaseImpl() {
        // made private for singleton access
        super(ProjectAnalysisJob.class, new AnalysisJobComputer());
    }
    
    public List<ProjectAnalysisJob> readAll(AppContext app, String projectId) {
        return ((ProjectAnalysisJobDAO) factory.getDAO(ProjectAnalysisJob.class)).findByProject(app, new ProjectPK(app
                .getCustomerId(), projectId));
    }

    @Override
    protected DataTable paginateResults(DataTable jobResults, Integer maxResults, Integer startIndex) {
    	// pagination is now performed by computation task
        return jobResults;
    }

	@Override
	public ProjectAnalysisJob store(AppContext ctx, ProjectAnalysisJob job,
			Integer timeout, Integer maxResults, Integer startIndex, boolean lazy) {
		if (timeout == null) {
			// just here to support old timeout=null behavior for export app
			// should be using job's autoRun property instead
			job = super.store(ctx, job, timeout, false, maxResults, startIndex, lazy);
		} else {
			job = super.store(ctx, job, timeout, maxResults, startIndex, lazy);
		}
		return job;
		
	}

	public String viewSQL(AppContext ctx, ProjectAnalysisJobPK jobId, boolean prettyfier) throws ComputingException, InterruptedException, ScopeException, SQLScopeException, RenderingException {
		final ProjectAnalysisJob job = read(ctx, jobId);
		AnalysisJobComputer phonyjob = new AnalysisJobComputer();// need to create a new one
		String sql = phonyjob.viewSQL(ctx, job);
		if (prettyfier) {
			StringBuilder html = new StringBuilder("<head>");
			html.append("<script src='https://cdn.rawgit.com/google/code-prettify/master/loader/run_prettify.js?lang=sql'></script>");
			html.append("</head><body>");
			html.append("<pre class='prettyprint lang-sql' style='white-space: pre-wrap;white-space: -moz-pre-wrap;white-space: -pre-wrap;white-space: -o-pre-wrap;word-wrap: break-word;padding:0px;margin:0px'>");
			html.append(StringEscapeUtils.escapeHtml4(sql));
			html.append("</pre>");
			html.append("</body>");
			return html.toString();
		} else {
			return sql;
		}
	}

}
