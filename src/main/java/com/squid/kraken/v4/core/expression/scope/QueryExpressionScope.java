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

import java.util.Collection;

import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.parser.ExpressionParserImp;
import com.squid.core.expression.parser.Token;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.reference.QueryExpression;
import com.squid.kraken.v4.core.expression.reference.DomainReference;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.ExpressionObject;

public class QueryExpressionScope extends DomainExpressionScope {

	public QueryExpressionScope(Universe universe, Domain domain) throws ScopeException {
		super(universe, domain);
	}

	public QueryExpressionScope(Universe universe, Domain domain, Collection<ExpressionObject<?>> scope)
			throws ScopeException {
		super(universe, domain, scope);
	}
	
	@Override
	public ExpressionAST createCompose(ExpressionAST first, ExpressionAST second, Token operator)
			throws ScopeException {
		if (operator.kind==ExpressionParserImp.FILTER) {
			return createAnalysisExpression(first, operator).filter(second);
		} else if (operator.kind==ExpressionParserImp.FACET) {
			return createAnalysisExpression(first, operator).facet(second);
		} else if (operator.kind==ExpressionParserImp.METRIC) {
			return createAnalysisExpression(first, operator).metric (second);
		} else {
    		throw new ScopeException("composition operator '"+operator.image+"' is not supported in this scope");
		}
	}
	
	private QueryExpression createAnalysisExpression(ExpressionAST first, Token operator) throws ScopeException {
		if (first instanceof DomainReference) {
			DomainReference ref = (DomainReference)first;
			return new QueryExpression(getUniverse(), ref.getDomain());
		} else if (first instanceof QueryExpression) {
			return (QueryExpression)first;
		} else {
			throw new ScopeException("cannot apply operator "+operator.image);
		}
	}

}
