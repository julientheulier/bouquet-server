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

import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.kraken.v4.core.expression.scope.DomainExpressionScope;
import com.squid.core.expression.scope.ExpressionDiagnostic;
import com.squid.core.expression.scope.ExpressionScope;
import com.squid.kraken.v4.core.expression.scope.MetricExpressionScope;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.ExpressionObject;
import com.squid.kraken.v4.model.Metric;

/**
 * Expression Scope for parsing a Metric definition
 *
 */
public class MetricExpressionScope extends DomainExpressionScope {
	
	private Metric metric;

	public MetricExpressionScope(Universe universe, Domain domain) throws ScopeException {
		super(universe, domain);
	}
	
	public MetricExpressionScope(Universe universe, Domain domain,
			Collection<ExpressionObject<?>> scope) throws ScopeException {
		super(universe, domain, scope);
	}
	
	public MetricExpressionScope(Universe universe, Domain domain, Metric metric,
			Collection<ExpressionObject<?>> scope) throws ScopeException {
		super(universe, domain, scope);
		this.metric = metric;
	}
	
	public MetricExpressionScope(Universe universe, Domain domain, Metric metric) throws ScopeException {
		super(universe, domain);
		this.metric = metric;
	}

	public MetricExpressionScope(Universe universe, Domain domain, Space target)
			throws ScopeException {
		super(universe, domain, target);
	}
	
	protected MetricExpressionScope(Universe universe, Domain domain, Space target, boolean restricted, Collection<ExpressionObject<?>> scope) throws ScopeException {
		super(universe, domain, target, restricted, scope);
	}

	@Override
	protected ExpressionScope createScope(Universe universe, Domain domain, Space target) throws ScopeException {
		return new MetricExpressionScope(universe, domain, target, restrictedScope, scope);// the scope should contains all the available references
	}
	
	@Override
	public ExpressionAST createCompose(ExpressionAST first, ExpressionAST second)
			throws ScopeException {
		return super.compose(first, second);
	}
	
	@Override
	public ExpressionDiagnostic validateExpression(ExpressionAST expression) {
		IDomain image = expression.getImageDomain();
		if (image.isInstanceOf(IDomain.NUMERIC)||image.isInstanceOf(IDomain.TEMPORAL)) {
			return ExpressionDiagnostic.IS_VALID;
		} else {
			return new ExpressionDiagnostic("the metric must be of type numeric or date");
		}
	}
	
	@Override
	protected boolean checkSelf(Object object) {
		return this.metric!=null && this.metric.equals(metric);
	}

}
