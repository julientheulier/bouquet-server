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
package com.squid.kraken.v4.api.core.dimension;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.GenericServiceImpl;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndexProxy;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchy;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchyManager;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.scope.AttributeExpressionScope;
import com.squid.kraken.v4.core.expression.scope.DimensionDefaultValueScope;
import com.squid.kraken.v4.core.expression.scope.ExpressionSuggestionHandler;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.DimensionOption;
import com.squid.kraken.v4.model.DimensionOptionPK;
import com.squid.kraken.v4.model.DimensionPK;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.DynamicObject;
import com.squid.kraken.v4.model.ExpressionObject;
import com.squid.kraken.v4.model.ExpressionSuggestion;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.ValueType;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.DimensionDAO;
import com.squid.kraken.v4.persistence.dao.ProjectDAO;

public class DimensionServiceBaseImpl extends
		GenericServiceImpl<Dimension, DimensionPK> {

	static final Logger logger = LoggerFactory
			.getLogger(DimensionServiceBaseImpl.class);

	private static DimensionServiceBaseImpl instance;

	public static DimensionServiceBaseImpl getInstance() {
		if (instance == null) {
			instance = new DimensionServiceBaseImpl();
		}
		return instance;
	}

	private DimensionServiceBaseImpl() {
		super(Dimension.class);
	}
	
    /**
     * Read all Dimensions for the Domain (including child dimensions).
     * @param customerId
     * @param domainId
     * @return
     * @throws InterruptedException 
     * @throws ComputingException 
     */
    public List<Dimension> readAll(AppContext ctx, DomainPK domainPk) {
    	try {
	        ProjectPK projectPk = new ProjectPK(ctx.getCustomerId(), domainPk.getProjectId());
	    	// T70
	        Domain domain = ProjectManager.INSTANCE.getDomain(ctx, domainPk);
	    	DomainHierarchy domainHierarchy = DomainHierarchyManager.INSTANCE.getHierarchy(projectPk, domain, true);
	    	return domainHierarchy.getDimensions(ctx);
    	} catch (InterruptedException | ComputingException | ScopeException e) {
    		throw new APIException(e, true);
    	}
    }
    
    @Override
    public Dimension read(AppContext ctx, DimensionPK dimensionId) {

    	if (!ctx.isDeepRead()) {
	    	try {
	    		DomainPK domainPk = dimensionId.getParent();
	    		ProjectPK projectPk = domainPk.getParent();
		    	// T70
		        Domain domain = ProjectManager.INSTANCE.getDomain(ctx, domainPk);
		    	DomainHierarchy domainHierarchy = DomainHierarchyManager.INSTANCE.getHierarchy(projectPk, domain, true);
		    	for (DimensionIndex index : domainHierarchy.getDimensionIndexes()) {
		    		if (!(index instanceof DimensionIndexProxy)) {
		    			Dimension dimension = index.getDimension();
		    			if (dimension.getId().equals(dimensionId) && AccessRightsUtils.getInstance().hasRole(ctx, dimension, Role.READ)) {
		    				return dimension;
		    			}
		    		}
		    	}
		    	// else
		    	throw new ObjectNotFoundAPIException("Object not found with id : "+dimensionId, false);
	    	} catch (InterruptedException | ComputingException e) {
	    		throw new APIException(e, true);
			} catch (ScopeException e) {
				throw new ObjectNotFoundAPIException(e.getMessage(), e, false);
			}
    	} else {
    		return super.read(ctx, dimensionId,true);
    	}
    }

	public ExpressionSuggestion getAttributeSuggestion(AppContext ctx,
			String projectId, String domainId, String dimensionId,
			String expression, int offset, ValueType filterType) {
		//
		ProjectPK projectPk = new ProjectPK(ctx.getCustomerId(), projectId);
		Project project = ((ProjectDAO) factory.getDAO(Project.class)).read(
				ctx, projectPk).get();
		DomainPK domainPk = new DomainPK(projectPk, domainId);
		try {
	    	// T70
	        Domain domain = ProjectManager.INSTANCE.getDomain(ctx, domainPk);
			Universe universe = new Universe(ctx, project);
			AttributeExpressionScope scope = new AttributeExpressionScope(
					universe, domain);
			ExpressionSuggestionHandler handler = new ExpressionSuggestionHandler(
					scope);
			if (offset == 0) {
				offset = expression.length();
			}
			return handler.getSuggestion(expression, offset, filterType);
		} catch (ScopeException e) {
			ExpressionSuggestion error = new ExpressionSuggestion();
			error.setValidateMessage(e.getLocalizedMessage());
			return error;
		}
	}

	public List<Dimension> readSubDimensions(AppContext ctx,
			DimensionPK dimensionId) {
		Optional<Dimension> parent = ((DimensionDAO) factory.getDAO(Dimension.class))
				.read(ctx, dimensionId);
		if (parent.isPresent()) {
			List<Dimension> dimensions = ((DimensionDAO) factory
					.getDAO(Dimension.class)).findByParent(ctx, parent.get());
			return dimensions;
		} else {
			return Collections.emptyList();
		}
	}
	
	/**
	 * check that the DimensionOption definition is valid
	 * @param ctx
	 * @param dimension
	 * @param option
	 * @throws ScopeException
	 */
	public void checkDimensionOption(AppContext ctx, Dimension dimension, DimensionOption option) throws ScopeException {
		// check default selection definition
		if (option.getDefaultSelection()!=null && option.getDefaultSelection().getValue()!=null) {
			Project project = ProjectManager.INSTANCE.getProject(ctx, dimension.getId().getParent().getParent());
	        Universe universe = new Universe(ctx, project);
			DomainPK domainPk = dimension.getId().getParent();
	        Domain domain = ProjectManager.INSTANCE.getDomain(ctx, domainPk);
	        ExpressionAST expr = universe.getParser().parse(domain, dimension);
			DimensionDefaultValueScope scope = new DimensionDefaultValueScope(ctx, dimension, expr.getImageDomain());
			try {
				scope.parseExpression(option.getDefaultSelection().getValue());
			} catch (ScopeException e) {
				throw new ScopeException("Invalid Dimension Option definition: unable to parse the default selection: "+e.getLocalizedMessage(), e);
			}
		}
	}
	
	public void checkDimensionOption(AppContext ctx, Domain domain, Dimension dimension, ExpressionAST expr, DimensionOption option) throws ScopeException {
		// check default selection definition
		if (option.getDefaultSelection()!=null && option.getDefaultSelection().getValue()!=null) {
			DimensionDefaultValueScope scope = new DimensionDefaultValueScope(ctx, dimension, expr.getImageDomain());
			try {
				scope.parseExpression(option.getDefaultSelection().getValue());
			} catch (ScopeException e) {
				throw new ScopeException("Invalid Dimension Option definition: unable to parse the default selection: "+e.getLocalizedMessage(), e);
			}
		}
	}
	
	@Override
	public Dimension store(AppContext ctx, Dimension dimension) {
		if (dimension.getId()==null) {
            throw new APIException("Dimension should not have a null id", ctx.isNoError());
		}
        if (dimension.getExpression()==null) {
        	throw new APIException("Dimension should not have a null Expression", ctx.isNoError());
        }
		try {
			DimensionPK dimensionPk = dimension.getId();
	        if (dimensionPk==null) {
	        	throw new APIException("Dimension must not have a null key", ctx.isNoError());
	        }
			dimensionPk.setCustomerId(ctx.getCustomerId());// force the customerID so we can check if natural
			DomainPK domainPk = dimensionPk.getParent();
	        Domain domain = ProjectManager.INSTANCE.getDomain(ctx, domainPk);
	        // check name
	        if (dimension.getName()==null || dimension.getName().length()==0) {
	        	throw new APIException("Dimension name must be defined", ctx.isNoError());
	        }
	        DomainHierarchy hierarchy = DomainHierarchyManager.INSTANCE.getHierarchy(domainPk.getParent(), domain, true); 
			// check if exists
	        Dimension old = null;
	        try {
	        	old = hierarchy.findDimension(ctx, dimensionPk);
        		/*
	        	if (old!=null && !old.isDynamic() && dimension.isDynamic()) {
	        		// turn the dimension back to dynamic => delete it
	        		if (DynamicManager.INSTANCE.isNatural(dimension)) {
	        			if (delete(ctx, dimensionPk)) {
	        				return hierarchy.getDimension(ctx, dimensionPk);
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
	        if (old!=null && old.getName().equals(dimension.getName())) {
	        	// no need to check
	        } else {
	        	// check
	        	Object check = hierarchy.findByName(dimension.getName());
	        	if (check!=null) {
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
	        }
			// first check if the Domain is dynamic
	        if (domain.isDynamic()) {
	        	domain.setDynamic(false);// make it concrete
	        	domain = DAOFactory.getDAOFactory().getDAO(Domain.class).create(ctx, domain);
	        }
	        // check if the parentId is valid
	        if (dimension.getParentId()!=null && dimension.getParentId().getDimensionId()==null) {
	        	dimension.setParentId(null);
	        }
	        if (dimension.getParentId()!=null) {
	        	// check if it is valid
	        	DimensionPK parentPk = dimension.getParentId();
	        	parentPk.setCustomerId(ctx.getCustomerId());
	        	if (!parentPk.getDomainId().equals(dimensionPk.getDomainId())
	        	||  !parentPk.getProjectId().equals(dimensionPk.getProjectId())) {
	        		throw new ObjectNotFoundAPIException("Invalid parent ID, the parent Dimension must belong to the same Domain", false);
	        	}
	        	// check if exists
	        	Dimension parent = hierarchy.getDimension(ctx, dimension.getParentId());// check if exists and can access
	        	// parent must be visible
	        	if (parent.isDynamic()) {
	    			throw new ObjectNotFoundAPIException("Invalid parent ID, the parent Dimension cannot be dynamic", false);
	        	}
	        }
	        // validate the expression
			Project project = ProjectManager.INSTANCE.getProject(ctx, dimension.getId().getParent().getParent());
	        Universe universe = new Universe(ctx, project);
	        ExpressionAST expr = universe.getParser().parse(domain, dimension);
	        Collection<ExpressionObject<?>> references = universe.getParser().analyzeExpression(dimension.getId(), dimension.getExpression(), expr);
	        universe.getParser().saveReferences(references);
	        //
	        // check the options
	        HashMap<DimensionOptionPK, DimensionOption> checkOld = new HashMap<>();
	        if (old!=null && old.getOptions()!=null) {
	        	for (DimensionOption option : old.getOptions()) {
	        		checkOld.put(option.getId(), option);
	        	}
	        }
	        if (dimension.getOptions()!=null) {
	        	for (DimensionOption option : dimension.getOptions()) {
	        		// enforce ID
	        		if (option.getId()==null) {
	        			DimensionOptionPK id = new DimensionOptionPK(dimensionPk);
	        			id.setObjectId(ObjectId.get().toString());
	        			option.setId(id);
	        		} else {
	        			DimensionOptionPK id = new DimensionOptionPK(dimensionPk);
	        			if (option.getId().getObjectId()==null || option.getId().getObjectId().equals("")) {
	        				id.setObjectId(ObjectId.get().toString());
	        			} else {
	        				id.setObjectId(option.getId().getObjectId());
	        			}
	        			option.setId(id);
	        		}
	        		// check if new or updated
	        		DimensionOption check = checkOld.get(option.getId());
	        		if (check==null || !check.equals(option)) {
	        			checkDimensionOption(ctx, domain, dimension, expr, option);
	        		}
	        	}
	        }
	        // ok
			return super.store(ctx, dimension);
		} catch (ScopeException | ComputingException | InterruptedException e) {
			throw new ObjectNotFoundAPIException(e.getMessage(), e, false);
		}
	}

	@Override
    public boolean delete(AppContext ctx, DimensionPK objectId) {
		Dimension dim = this.read(ctx, objectId);		
    	if (dim == null){
    		throw new ObjectNotFoundAPIException("Invalid dimension id " + objectId.toString(), false);
    	}else{
    		boolean shouldHide= dim.hide();
    		if (shouldHide){
    			store(ctx, dim);
    			return true;
    		}else{
    			return super.delete(ctx, objectId);
    		}
    	}    	
	}
	
}
