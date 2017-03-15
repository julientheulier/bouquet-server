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
package com.squid.kraken.v4.core.expression.scope;

import java.util.List;

import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.DefaultScope;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.IdentifierType;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.expression.reference.ParameterReference;
import com.squid.kraken.v4.persistence.AppContext;

/**
 * support creating references to user attributes
 * @author sergefantino
 *
 */
public class UserContextScope extends DefaultScope {

	private AppContext ctx;
	private boolean checkParameter;

	public UserContextScope(AppContext ctx, boolean checkParameter) {
		this.ctx = ctx;
		this.checkParameter = checkParameter;
	}
	
	@Override
	public Object lookupObject(IdentifierType type, String name) throws ScopeException {
		if (ctx.getUser().getAttributes()!=null && ctx.getUser().getAttributes().containsKey(name)) {
			try {
				ExpressionAST exp = this.parseExpression(ctx.getUser().getAttributes().get(name));
				return exp;				
			} catch (ScopeException se) {
				return ExpressionMaker.CONSTANT(ctx.getUser().getAttributes().get(name));
			}
			//
		} else {
			if (checkParameter) {
				throw new ScopeException("the attribute '"+name+"' is not defined for user '"+ctx.getUser().getLogin()+"'");
			} else {
				return ExpressionMaker.NULL();
			}
		}
	}
	
	@Override
	public void buildDefinitionList(List<Object> definitions) {
		if (ctx.getUser().getAttributes()!=null) {
			for (String key : ctx.getUser().getAttributes().keySet()) {
				definitions.add(new ParameterReference(key,IDomain.STRING));
			}
		}
		super.buildDefinitionList(definitions);
	}
	
	@Override
	public ExpressionAST createReferringExpression(Object object)
			throws ScopeException {
		if (object instanceof ExpressionAST) {
			return ((ExpressionAST)object);
		} else {
			return super.createReferringExpression(object);
		}
	}
	
}
