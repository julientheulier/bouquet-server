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
package com.squid.kraken.v4.core.analysis.scope;

import java.util.List;

import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.DefaultScope;
import com.squid.core.expression.scope.ExpressionScope;
import com.squid.core.expression.scope.IdentifierType;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.ProjectDAO;

/**
 * @author sergefantino
 *
 */
public class GlobalExpressionScope 
extends DefaultScope
{
	
	private AppContext ctx;

	/**
	 * 
	 */
	public GlobalExpressionScope(AppContext ctx) {
		this.ctx = ctx;
	}

	@Override
	public ExpressionScope applyExpression(ExpressionAST expression) {
		if (expression instanceof ProjectExpressionRef) {
			Universe universe = new Universe(ctx, ((ProjectExpressionRef)expression).getProject());
			return new UniverseScope(universe);
		}
		// else
		return null;
	}
	
	/* (non-Javadoc)
	 * @see com.squid.core.expression.scope.DefaultExpressionConstructor#createCompose(com.squid.core.expression.ExpressionAST, com.squid.core.expression.ExpressionAST)
	 */
	@Override
	public ExpressionAST createCompose(ExpressionAST first, ExpressionAST second) throws ScopeException {
		return second;
	}
	
	/* (non-Javadoc)
	 * @see com.squid.core.expression.scope.DefaultScope#createReferringExpression(java.lang.Object)
	 */
	@Override
	public ExpressionAST createReferringExpression(Object object) throws ScopeException {
		if (object instanceof Project) {
			return new ProjectExpressionRef((Project)object);
		}
		return super.createReferringExpression(object);
	}
	
	/* (non-Javadoc)
	 * @see com.squid.core.expression.scope.DefaultScope#lookupObject(com.squid.core.expression.scope.IdentifierType, java.lang.String)
	 */
	@Override
	public Object lookupObject(IdentifierType identifierType, String identifier) throws ScopeException {
		// lookup for a Project in the user scope
		List<Project> projects = ((ProjectDAO) DAOFactory.getDAOFactory().getDAO(Project.class))
				.findByCustomer(ctx, ctx.getCustomerPk());
		for (Project project : projects) {
			if (identifierType.equals(IdentifierType.IDENTIFIER) && project.getId().getProjectId().equals(identifier)) {
				return project;
			} else if (project.getName()!=null && project.getName().equals(identifier)) {
				return project;
			}
		}
		// else
		return super.lookupObject(identifierType, identifier);
	}

}
