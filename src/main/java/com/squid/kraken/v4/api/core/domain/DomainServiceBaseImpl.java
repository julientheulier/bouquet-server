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
package com.squid.kraken.v4.api.core.domain;

import java.util.ArrayList;
import java.util.List;

import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.GenericServiceImpl;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.caching.awsredis.RedisCacheManager;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchyManager;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.scope.DimensionExpressionScope;
import com.squid.kraken.v4.core.expression.scope.ExpressionSuggestionHandler;
import com.squid.kraken.v4.core.expression.scope.MetricExpressionScope;
import com.squid.kraken.v4.core.expression.scope.SegmentExpressionScope;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.DimensionPK;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.ExpressionSuggestion;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.MetricDAO;

public class DomainServiceBaseImpl extends GenericServiceImpl<Domain, DomainPK> {

	private static DomainServiceBaseImpl instance;

	private static final MetricDAO metricDAO = ((MetricDAO) DAOFactory.getDAOFactory().getDAO(Metric.class));

	public static DomainServiceBaseImpl getInstance() {
		if (instance == null) {
			instance = new DomainServiceBaseImpl();
		}
		return instance;
	}

	private DomainServiceBaseImpl() {
		// made private for singleton access
		super(Domain.class);
	}
	
    @Override
    public Domain read(AppContext ctx, DomainPK domainPk) {
    	if (!ctx.isDeepRead()) {
	        // override to support T70 - dynamic Domains
	    	try {
	    		return ProjectManager.INSTANCE.getDomain(ctx, domainPk);
	    	} catch (ScopeException e) {
	    		throw new ObjectNotFoundAPIException(e.getMessage(), e, false);
	    	}
    	} else {
    		// deepRead support
    		return super.read(ctx, domainPk, true);
    	}
    }

	public List<Domain> readAll(AppContext ctx, String projectId) {
		try {
			ProjectPK id = new ProjectPK(ctx.getCustomerId(), projectId);
			List<Domain> domains = ProjectManager.INSTANCE.getDomains(ctx,id);
			ArrayList<Domain> visibles = new ArrayList<Domain>();
			for (Domain domain : domains) {
				if (domain.isDynamic()) {
					// if it is dynamic and already visible, don't change it
					visibles.add(domain);
				} else if (AccessRightsUtils.getInstance().hasRole(ctx, domain, Role.WRITE)) {
					// write access
					visibles.add(domain);
				} else {
					// not dynamic and not write, check if has metrics
					List<Metric> metrics = metricDAO.findByDomain(ctx, domain.getId());
					if (!metrics.isEmpty()) {
						visibles.add(domain);
					}
				}
			}
			return visibles;
		} catch (ScopeException e) {
			throw new ObjectNotFoundAPIException(e.getMessage(), e, false);
		}
	}
	
	@Override
	public Domain store(AppContext ctx, Domain domain) {
        // check ID
		DomainPK domainPk = domain.getId();
        if (domainPk==null) {
        	throw new APIException("Domain must not have a null key", ctx.isNoError());
        }
		// check name
        if (domain.getName()==null || domain.getName().length()==0) {
        	throw new APIException("Domain name must be defined", ctx.isNoError());
        }
        // check project
        Project project = null;
        try {
        	project = ProjectManager.INSTANCE.getProject(ctx, domainPk.getParent());
            Domain check = ProjectManager.INSTANCE.findDomainByName(ctx, domainPk.getParent(), domain.getName());
            if (check!=null) {
            	// check if it is self
            	if (!check.getId().getDomainId().equals(domainPk.getDomainId())) {
            		throw new APIException("A Domain with that name already exists in this project", ctx.isNoError());
            	}
            }
        } catch (ScopeException e) {
        	throw new APIException(e.getLocalizedMessage(), ctx.isNoError());
        }
        // check definition
        // -- we allow subject to be null mainly for unit-testing
        if (domain.getSubject()!=null) {
        	if (domain.getSubject().getValue()==null || domain.getSubject().getValue().equals("")) {
        		throw new APIException("Domain must not have an undefined subject", ctx.isNoError());
        	}
        	Universe universe = new Universe(ctx, project);
        	try {
    	        ExpressionAST expr = universe.getParser().parse(domain);
    	        universe.getParser().analyzeExpression(domain.getId(), domain.getSubject(), expr);
        	} catch (ScopeException e) {
        		throw new APIException("Domain subject is invalid:\n"+e.getLocalizedMessage(), ctx.isNoError());
        	}
        	//
        }
		return super.store(ctx, domain);
	}

	public ExpressionSuggestion getDimensionSuggestion(AppContext ctx,
			String projectId, String domainId, String dimensionId, String expression, int offset) {
		//
		if (expression == null) {
			expression = "";
		}
		try {
			ProjectPK projectPK = new ProjectPK(ctx.getCustomerId(), projectId);
			Project project = ProjectManager.INSTANCE.getProject(ctx, projectPK);
			DomainPK domainPK = new DomainPK(projectPK, domainId);
			Domain domain = ProjectManager.INSTANCE.getDomain(ctx, domainPK);
			Dimension dimension = dimensionId!=null ? DomainHierarchyManager.INSTANCE.findDimension(ctx, projectPK, domain, new DimensionPK(domainPK, dimensionId)) : null;
			Universe universe = new Universe(ctx, project);
			//
			DimensionExpressionScope scope = new DimensionExpressionScope(
					universe, domain, dimension);
			ExpressionSuggestionHandler handler = new ExpressionSuggestionHandler(
					scope);
			if (offset == 0) {
				offset = expression.length();
			}
			return handler.getSuggestion(expression, offset);
		} catch (ScopeException | ComputingException | InterruptedException e) {
			return new ExpressionSuggestion(e);
		}
	}

	public ExpressionSuggestion getMetricSuggestion(AppContext ctx,
			String projectId, String domainId, String metricId, String expression, int offset) {
		//
		try {
			ProjectPK projectPK = new ProjectPK(ctx.getCustomerId(), projectId);
			Project project = ProjectManager.INSTANCE.getProject(ctx, projectPK);
			DomainPK domainPK = new DomainPK(projectPK, domainId);
			//
			Universe universe = new Universe(ctx, project);
			Domain domain = ProjectManager.INSTANCE.getDomain(ctx, domainPK);
			Metric metric = metricId!=null?DomainHierarchyManager.INSTANCE.findMetric(ctx, projectPK, domain, metricId):null;
			MetricExpressionScope scope = new MetricExpressionScope(universe,
					domain, metric);
			ExpressionSuggestionHandler handler = new ExpressionSuggestionHandler(
					scope);
			if (offset == 0) {
				offset = expression.length();
			}
			return handler.getSuggestion(expression, offset);
		} catch (ScopeException | ComputingException | InterruptedException e) {
			return new ExpressionSuggestion(e);
		}
	}

	public ExpressionSuggestion getSegmentSuggestion(AppContext ctx,
			String projectId, String domainId, String expression, int offset) {
		//
		try {
			ProjectPK projectPK = new ProjectPK(ctx.getCustomerId(), projectId);
			Project project = ProjectManager.INSTANCE.getProject(ctx, projectPK);
			DomainPK domainPK = new DomainPK(projectPK, domainId);
			//
			Universe universe = new Universe(ctx, project);
			Domain domain = ProjectManager.INSTANCE.getDomain(ctx, domainPK);
			SegmentExpressionScope scope = new SegmentExpressionScope(universe,
					domain);
			ExpressionSuggestionHandler handler = new ExpressionSuggestionHandler(
					scope);
			if (offset == 0) {
				offset = expression.length();
			}
			return handler.getSuggestion(expression, offset);
		} catch (ScopeException e) {
			return new ExpressionSuggestion(e);
		}
	}

	/**
	 * refresh the data (table) associated with that domain
	 * 
	 * @param userContext
	 * @param domainId
	 * @return
	 */
	public boolean refreshDomainData(AppContext userContext, String projectId,
			String domainId) {
		// compute the table key
		try {
			Space space = getSpace(userContext, projectId, domainId);
			String uuid = space.getTableUUID();
			if (uuid != null) {
				RedisCacheManager.getInstance().refresh(uuid);
				// refresh the domain too
				RedisCacheManager.getInstance().refresh(
						space.getDomain().getId().toUUID());
				DomainHierarchyManager.INSTANCE.invalidate(new DomainPK( projectId, domainId));
				return true;
			}
		} catch (ScopeException e) {
			// ignore
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	protected Space getSpace(AppContext ctx, String projectId, String domainId)
			throws ScopeException {
		ProjectPK projectPK = new ProjectPK(ctx.getCustomerId(), projectId);
		Project project = ProjectManager.INSTANCE.getProject(ctx, projectPK);
		DomainPK domainPK = new DomainPK(projectPK, domainId);
		Domain domain = ProjectManager.INSTANCE.getDomain(ctx, domainPK);
		//
		Universe universe = new Universe(ctx, project);
		return universe.S(domain);
	}
	
	public Object refreshCache(AppContext ctx, ProjectPK projectPk, DomainPK domainPk) {
		//
		try {
			return ProjectManager.INSTANCE.refreshDomainData(ctx, domainPk);
		} catch (ScopeException e) {
			throw new APIException("failed to refresh the domain data caused by:\n"+e.getLocalizedMessage(), e, ctx.isNoError());
		}
	}
	

}
