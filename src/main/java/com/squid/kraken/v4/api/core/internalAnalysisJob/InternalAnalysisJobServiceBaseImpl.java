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

import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.RenderingException;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.query.SimpleQuery;
import com.squid.kraken.v4.model.*;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.DomainDAO;
import com.squid.kraken.v4.persistence.dao.ProjectDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lrabiet on 18/11/15.
 */
public class InternalAnalysisJobServiceBaseImpl extends JobServiceBaseImpl<ProjectAnalysisJob, ProjectAnalysisJobPK, DataTable> {
    private static final Logger logger = LoggerFactory
            .getLogger(InternalAnalysisJobServiceBaseImpl.class);

    private static InternalAnalysisJobServiceBaseImpl instance;

    public static InternalAnalysisJobServiceBaseImpl getInstance() {
        if (instance == null) {
            instance = new InternalAnalysisJobServiceBaseImpl();
        }
        return instance;
    }

    private InternalAnalysisJobServiceBaseImpl() {
        // made private for singleton access
        super(ProjectAnalysisJob.class, new InternalAnalysisJobComputer());
    }

    @Override
    protected DataTable paginateResults(DataTable results, Integer maxResults, Integer startIndex) {
        return null;
    }

    public interface Predicate<T> { boolean apply(T type); }

    Predicate<Domain> isInternalDomain = new Predicate<Domain>() {
        public boolean apply(Domain domain) {
            return domain.getOptions().getReinjected() || domain.getOptions().getAlink();
            }
    };

    public static <T> List<T> filter(List<T> col, Predicate<T> predicate) {
        List<T> result = new ArrayList<T>();
        for (T element : col) {
            if (predicate.apply(element)) {
                result.add(element);
            }
        }
        return result;
    }

    public List<Domain> readAll(AppContext ctx) {
        List <Project> projects = ((ProjectDAO) DAOFactory.getDAOFactory().getDAO(Project.class))
                .findByCustomer(ctx, ctx.getCustomerPk());
        List<Domain> result = new ArrayList<Domain>();
        for (Project project: projects) {
            List<Domain> domains = ((DomainDAO) factory.getDAO(Domain.class)).findByProject(ctx, project.getId());
            result.addAll(filter(domains,isInternalDomain));
        }
        return result;
        // domains.stream().filter(p -> p.getOptions().getReinjected());

    }

    public List<SimpleQuery> reinject(AppContext ctx, ProjectAnalysisJobPK jobId) throws ComputingException, InterruptedException, ScopeException, SQLScopeException, RenderingException {
        final ProjectAnalysisJob job = read(ctx, jobId);
        InternalAnalysisJobComputer xxx = new InternalAnalysisJobComputer();// need to create a new one
        return xxx.reinject(ctx, job);
    }



}
