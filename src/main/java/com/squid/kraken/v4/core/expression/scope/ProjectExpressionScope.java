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
import java.util.concurrent.ExecutionException;

import com.squid.core.database.domain.TableDomain;
import com.squid.core.database.model.Table;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.reference.TableReference;
import com.squid.core.expression.scope.DefaultScope;
import com.squid.core.expression.scope.ExpressionDiagnostic;
import com.squid.core.expression.scope.ExpressionScope;
import com.squid.core.expression.scope.IdentifierType;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.universe.Physics;

public class ProjectExpressionScope extends DefaultScope {
	
	private Physics physics;

	public ProjectExpressionScope(Physics physics) {
		this.physics = physics;
	}

	@Override
	public ExpressionScope applyExpression(ExpressionAST first) throws ScopeException {
		throw new ScopeException("cannot apply the expression "+first.prettyPrint()+"");
	}

	@Override
	public IdentifierType lookupIdentifierType(String image)
			throws ScopeException {
		throw new ScopeException("cannot lookup the image "+image+"");
	}

	@Override
	public Object lookupObject(IdentifierType identifierType, String identifier)
			throws ScopeException {
		try {
			Table table = physics.getTable(identifier);
			if (table==null) {
				throw new ScopeException("cannot lookup the table '"+identifier+"'");
			}
			return table;
		} catch (ExecutionException e) {
			throw new ScopeException("cannot lookup the table '"+identifier+"'");
		}
	}

	@Override
	public ExpressionAST createReferringExpression(Object reference)
			throws ScopeException {
		if (reference instanceof Table) {
			return new TableReference((Table)reference);
		} else
			return super.createReferringExpression(reference);
	}
	
	@Override
	public void buildDefinitionList(List<Object> definitions) {
		super.buildDefinitionList(definitions);
		//
		try {
            List<Table> tables = physics.getTables();
            definitions.addAll(tables);
        } catch (ExecutionException e) {
            // ignore ?
        }
	}
	
	@Override
	public ExpressionDiagnostic validateExpression(ExpressionAST expression) {
		if (expression.getImageDomain().isInstanceOf(TableDomain.DOMAIN)) {
			return ExpressionDiagnostic.IS_VALID;
		} else {
			return new ExpressionDiagnostic("The expression must be a valid table");
		}
	}

}
