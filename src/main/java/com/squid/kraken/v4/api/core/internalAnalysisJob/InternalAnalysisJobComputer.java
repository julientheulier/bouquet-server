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
package com.squid.kraken.v4.api.core.internalAnalysisJob;

import java.io.OutputStream;
import java.util.List;

import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.RenderingException;
import com.squid.kraken.v4.api.core.JobComputer;
import com.squid.kraken.v4.api.core.projectanalysisjob.AnalysisJobComputer;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingService;
import com.squid.kraken.v4.core.analysis.engine.query.SimpleQuery;
import com.squid.kraken.v4.core.analysis.model.DashboardAnalysis;
import com.squid.kraken.v4.export.ExportSourceWriter;
import com.squid.kraken.v4.model.DataTable;
import com.squid.kraken.v4.model.ProjectAnalysisJob;
import com.squid.kraken.v4.model.ProjectAnalysisJobPK;
import com.squid.kraken.v4.persistence.AppContext;

/**
 * Created by lrabiet on 18/11/15.
 */
public class InternalAnalysisJobComputer implements
        JobComputer<ProjectAnalysisJob, ProjectAnalysisJobPK, DataTable>
    {

        @Override
        public DataTable compute(AppContext ctx, ProjectAnalysisJob job, Integer maxResults, Integer startIndex, boolean lazy) throws ComputingException, InterruptedException {
            return null;
        }

        @Override
        public DataTable compute(AppContext ctx, ProjectAnalysisJob job, OutputStream outputStream, ExportSourceWriter writer, boolean lazy) throws ComputingException, InterruptedException {
            return null;
        }

        public List<SimpleQuery> reinject(AppContext ctx, ProjectAnalysisJob job) throws ComputingException, InterruptedException, ScopeException, SQLScopeException, RenderingException {
            DashboardAnalysis analysis;
            try {
                analysis = AnalysisJobComputer.INSTANCE.buildDashboardAnalysis(ctx, job);
            } catch (ScopeException e1) {
                throw new ComputingException(e1);
            }
            // run the analysis
            return ComputingService.INSTANCE.reinject(analysis);
        }
    }
