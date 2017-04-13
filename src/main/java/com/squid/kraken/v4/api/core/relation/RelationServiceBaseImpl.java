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
package com.squid.kraken.v4.api.core.relation;

import java.util.List;

import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.GenericServiceImpl;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.Relation;
import com.squid.kraken.v4.model.RelationPK;
import com.squid.kraken.v4.persistence.AppContext;

public class RelationServiceBaseImpl extends GenericServiceImpl<Relation, RelationPK> {

    private static final RelationServiceBaseImpl instance = new RelationServiceBaseImpl();

    public static RelationServiceBaseImpl getInstance() {
        return instance;
    }

    private RelationServiceBaseImpl() {
        // made private for singleton access
        super(Relation.class);
    }

    public List<Relation> readAll(AppContext ctx, String projectId) {
    	// T71
    	try {
    		ProjectPK id = new ProjectPK(ctx.getCustomerId(), projectId);
    		return ProjectManager.INSTANCE.getRelations(ctx,id);
    	} catch (ScopeException e) {
    		throw new ObjectNotFoundAPIException(e.getMessage(), e, false);
    	}
    }
	
    @Override
    public Relation read(AppContext ctx, RelationPK relationPk) {
    	if (!ctx.isDeepRead()) {
	        // override to support T71 - dynamic Relations
	    	try {
				return ProjectManager.INSTANCE.getRelation(ctx, relationPk);
			} catch (ScopeException e) {
				throw new ObjectNotFoundAPIException(e.getMessage(), e, false);
			}
    	} else {
    		return super.read(ctx, relationPk, true);
    	}
    }

    /**
     * special version to filter relations by domain
     * @param userContext
     * @param domainPK
     * @return
     * @throws  
     */
	public List<Relation> readAll(AppContext ctx, DomainPK domainPK) {
		try {
			return ProjectManager.INSTANCE.getRelation(ctx, domainPK);
		} catch (ScopeException e) {
			throw new ObjectNotFoundAPIException(e.getMessage(), e, false);
		}
	}
	
	@Override
	public Relation store(AppContext ctx, Relation relation) {
		// supporting T450
		try {
			Project project = ProjectManager.INSTANCE.getProject(ctx, relation.getId().getParent());
			Universe universe = new Universe(ctx, project);
			if (relation.getJoinExpression()!=null && relation.getJoinExpression().getValue()!=null) {
				ExpressionAST expr = universe.getParser().parse(relation, relation.getJoinExpression().getValue());
				universe.getParser().analyzeExpression(relation.getId(), relation.getJoinExpression(), expr);
			}
			//
			// explicitly turn off dynamic flag if edited
			relation.setDynamic(false);
		} catch (ScopeException e) {
			throw new ObjectNotFoundAPIException(e.getMessage(), e, false);
		}
		return super.store(ctx, relation);
	}

	@Override
    public boolean delete(AppContext ctx, RelationPK objectId) {
		Relation rel = this.read(ctx, objectId);		
    	if (rel == null){
    		throw new ObjectNotFoundAPIException("Invalid relation id " + objectId.toString(), false);
    	}else{
    		boolean shouldHide= rel.hide();
    		if (shouldHide){
    			store(ctx, rel);
    			return true;
    		}else{
    			return super.delete(ctx, objectId);
    		}
    	}
		
	}
	
}
