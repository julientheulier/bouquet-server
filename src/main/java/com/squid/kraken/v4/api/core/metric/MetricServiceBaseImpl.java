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
package com.squid.kraken.v4.api.core.metric;

import java.util.Collection;
import java.util.List;

import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.GenericServiceImpl;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchy;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchyManager;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.DynamicObject;
import com.squid.kraken.v4.model.ExpressionObject;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.MetricExt;
import com.squid.kraken.v4.model.MetricPK;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;

public class MetricServiceBaseImpl extends GenericServiceImpl<Metric, MetricPK> {

    private static MetricServiceBaseImpl instance;

    public static MetricServiceBaseImpl getInstance() {
        if (instance == null) {
            instance = new MetricServiceBaseImpl();
        }
        return instance;
    }

    private MetricServiceBaseImpl() {
        // made private for singleton access
        super(Metric.class);
    }

    public List<MetricExt> readAll(AppContext ctx, DomainPK domainPk) {
    	try {
	        ProjectPK projectPk = new ProjectPK(ctx.getCustomerId(), domainPk.getProjectId());
	    	// T70
	        Domain domain = ProjectManager.INSTANCE.getDomain(ctx, domainPk);
	    	DomainHierarchy domainHierarchy = DomainHierarchyManager.INSTANCE.getHierarchy(projectPk, domain, true);
	    	return domainHierarchy.getMetricsExt(ctx);
    	} catch (InterruptedException | ComputingException e) {
    		throw new APIException(e, true);
    	} catch (ScopeException e) {
    		throw new ObjectNotFoundAPIException(e.getMessage(), e, false);
    	}
    }
    
    @Override
    public Metric read(AppContext ctx, MetricPK metricId) {
    	if (!ctx.isDeepRead()) {
	    	try {
				DomainPK domainPk = metricId.getParent();
				ProjectPK projectPk = domainPk.getParent();
				// T70
		        Domain domain = ProjectManager.INSTANCE.getDomain(ctx, domainPk);
		    	DomainHierarchy domainHierarchy = DomainHierarchyManager.INSTANCE.getHierarchy(projectPk, domain, true);
				Metric metric = domainHierarchy.getMetric(ctx, metricId.getMetricId());
				return metric;
	    	} catch (InterruptedException | ComputingException e) {
	    		throw new APIException(e, true);
	    	} catch (ScopeException e) {
	    		throw new ObjectNotFoundAPIException(e.getMessage(), e, false);
	    	}
    	} else {
    		return super.read(ctx, metricId, true);
    	}
    }

	
	@Override
	public Metric store(AppContext ctx, Metric metric) {
		try {
	        if (metric.getExpression()==null) {
	        	throw new APIException("Metric must not have a null Expression", ctx.isNoError());
	        }
			// first check if the Domain is dynamic
			MetricPK metricPk = metric.getId();
	        if (metricPk==null) {
	        	throw new APIException("Metric must not have a null key", ctx.isNoError());
	        }
			metricPk.setCustomerId(ctx.getCustomerId());// force the customerID so we can check if natural
			DomainPK domainPk = metricPk.getParent();
	        Domain domain = ProjectManager.INSTANCE.getDomain(ctx, domainPk);
	        // check name
	        if (metric.getName()==null || metric.getName().length()==0) {
	        	throw new APIException("Metric name must be defined", ctx.isNoError());
	        }
	        DomainHierarchy hierarchy = DomainHierarchyManager.INSTANCE.getHierarchy(domainPk.getParent(), domain, true);
			// check if exists
	        Metric old = null;
	        try {
	        	old = hierarchy.getMetric(ctx, metricPk.getMetricId());
	        	/*
	        	if (!old.isDynamic() && metric.isDynamic()) {
	        		// turn the dimension back to dynamic => delete it
	        		if (DynamicManager.INSTANCE.isNatural(metric)) {
	        			if (delete(ctx, metricPk)) {
	        				return hierarchy.getMetric(ctx, metricPk.getMetricId());
	        			}
	        		}
	        		// else
	        		// let me store it... keep continuing
	        	}
	        	*/
	        } catch (Exception e) {
	        	// ok, ignore
	        }
	        // check if name already defined
	        if (old!=null && old.getName().equals(metric.getName())) {
	        	// no need to check
	        } else {
	        	// check
	        	Object check = hierarchy.findByName(metric.getName());
	        	// ok to borrow a name from a dynamic ?
        		if (check instanceof DynamicObject<?>) {
        			DynamicObject<?> dyn = (DynamicObject<?>)check;
        			if (dyn.isDynamic()) {
        				check=  null; 
        			}
        		}
	        	if (check!=null) {
	        		throw new APIException("An object with that name already exists in this Domain scope", ctx.isNoError());
	        	}
	        }
	        if (domain.isDynamic()) {
	        	// if dynamic, must create first
	        	domain.setDynamic(false);
	        	domain = DAOFactory.getDAOFactory().getDAO(Domain.class).create(ctx, domain);
	        }
	        // validate the expression
			Project project = ProjectManager.INSTANCE.getProject(ctx, metric.getId().getParent().getParent());
	        Universe universe = new Universe(ctx, project);
	        ExpressionAST expr = universe.getParser().parse(domain, metric);
	        Collection<ExpressionObject<?>> references = universe.getParser().analyzeExpression(metric.getId(), metric.getExpression(), expr);
	        universe.getParser().saveReferences(references);
	        // ok
			return super.store(ctx, metric);
		} catch (ScopeException | ComputingException | InterruptedException e) {
			throw new ObjectNotFoundAPIException(e.getMessage(), e, false);
		}
	}
	
}
