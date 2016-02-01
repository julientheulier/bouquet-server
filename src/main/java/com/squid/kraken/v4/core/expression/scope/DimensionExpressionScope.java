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
import java.util.List;

import com.squid.kraken.v4.core.expression.reference.ParameterReference;
import com.squid.kraken.v4.core.expression.scope.DomainExpressionScope;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.analytics.AnalyticDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ExpressionDiagnostic;
import com.squid.core.expression.scope.ExpressionScope;
import com.squid.core.expression.scope.IdentifierType;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.ExpressionObject;

/**
 * Expression Scope for parsing a Dimension definition
 *
 */
public class DimensionExpressionScope extends DomainExpressionScope {
	
	private Dimension dimension;

	public DimensionExpressionScope(Universe universe, Domain domain) throws ScopeException {
		super(universe, domain);
	}
	
	/**
	 * use this constructor if the expression is attached to an existing dimension
	 * @param universe
	 * @param domain
	 * @param dimension or null
	 * @throws ScopeException
	 */
	public DimensionExpressionScope(Universe universe, Domain domain, Dimension dimension) throws ScopeException {
		super(universe, domain);
		this.dimension = dimension;
	}
	
	public DimensionExpressionScope(Universe universe, Domain domain, Collection<ExpressionObject<?>> scope) throws ScopeException {
		super(universe, domain, scope);
	}
	
	public DimensionExpressionScope(Universe universe, Domain domain, Dimension dimension, Collection<ExpressionObject<?>> scope) throws ScopeException {
		super(universe, domain, scope);
		this.dimension = dimension;
	}

	/**
	 * note: don't need the dimension to validate (because it may not be defined yet - remember)
	 * @param physics
	 * @param domain
	 * @throws ScopeException
	 */
	public DimensionExpressionScope(Universe universe, Domain domain, Space target)
			throws ScopeException {
		super(universe, domain, target);
	}
	
	protected DimensionExpressionScope(Universe universe, Domain domain, Space target, boolean restricted, Collection<ExpressionObject<?>> scope) throws ScopeException {
		super(universe, domain, target, restricted, scope);
	}
	
	@Override
	protected ExpressionScope createScope(Universe universe, Domain domain, Space target) {
		try {
			return new DimensionExpressionScope(universe, domain, target, restrictedScope, scope);// the scope should contains all the available references
		} catch (ScopeException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public ExpressionAST createCompose(ExpressionAST first, ExpressionAST second) throws ScopeException {
		if (second instanceof ParameterReference) {
			ParameterReference ref = (ParameterReference)second;
			if (ref.getReferenceName().equalsIgnoreCase("SELF")) {
				return first;
			}
		}
		// else
		return super.createCompose(first, second);
	}
	
	@Override
	protected boolean checkSelf(Object object) {
		return dimension!=null && object.equals(dimension);
	}
	
	@Override
	public Object lookupObject(IdentifierType identifierType, String identifier)
			throws ScopeException {
		if (getSpace().getParent()!=null && identifierType.equals(IdentifierType.PARAMETER) && identifier.equalsIgnoreCase("SELF")) {
			return new ParameterReference("SELF", getSpace().getImageDomain());
		}
		return super.lookupObject(identifierType, identifier);
	}
	
	@Override
	public void buildDefinitionList(List<Object> definitions) {
		super.buildDefinitionList(definitions);
		//
		// add Self if linked
		if (getSpace().getParent()!=null) {
			definitions.add(new ParameterReference("SELF", getSpace().getImageDomain()));
		}
	}
	
	@Override
	public ExpressionDiagnostic validateExpression(ExpressionAST expression) {
    	IDomain image = expression.getImageDomain();
    	if (image.isInstanceOf(IDomain.AGGREGATE)) {
    		return new ExpressionDiagnostic("invalid dimension type, cannot be an aggregate formula");
    	} else if (image.isInstanceOf(AnalyticDomain.DOMAIN)) {
        		return new ExpressionDiagnostic("invalid dimension type, cannot be an analytic formula");
    	} else {
    		return super.validateExpression(expression);
    	}
	}

}
