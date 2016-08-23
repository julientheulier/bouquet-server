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

import java.util.HashMap;
import java.util.List;

import com.squid.core.domain.IDomain;
import com.squid.core.expression.ConstantValue;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.DefaultScope;
import com.squid.core.expression.scope.ExpressionDiagnostic;
import com.squid.core.expression.scope.ExpressionScope;
import com.squid.core.expression.scope.IdentifierType;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex.Status;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.model.Intervalle;
import com.squid.kraken.v4.core.expression.reference.ParameterReference;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.persistence.AppContext;

public class DimensionDefaultValueScope extends DefaultScope {

	private AppContext ctx;
	private DimensionIndex index;

	private HashMap<String, IDomain> params = new HashMap<>();

	public DimensionDefaultValueScope(AppContext ctx, DimensionIndex index) {
		super();
		this.ctx = ctx;
		this.index = index;
	}
	
	/**
	 * allow this param to be use in the expression (note that some params are always available)
	 * @param param
	 * @param type == the param type
	 */
	public void addParam(String param, IDomain type) {
		params.put(param.toUpperCase(), type);
	}
	
	@Override
	public ExpressionScope applyExpression(ExpressionAST expression)
			throws ScopeException {
		if (expression instanceof ParameterReference) {
			ParameterReference ref = (ParameterReference)expression;
			if (ref.getParameterName().equals("USER")) {
				return new UserContextScope(ctx);
			}
		} 
		//else
		return null;
	}
	
	@Override
    public ExpressionAST createCompose(ExpressionAST first, ExpressionAST second) throws ScopeException {
		if (first instanceof ParameterReference && second instanceof ConstantValue) {
			ParameterReference check = (ParameterReference)first;
			if (check.getParameterName().equalsIgnoreCase("USER")) {
				return second;
			}
		}
		return super.createCompose(first, second);
    }

	@Override
	public Object lookupObject(IdentifierType type, String name) throws ScopeException {
		// parameters ?
		if (type==IdentifierType.PARAMETER) {
			if (name.equalsIgnoreCase("USER")) {
				return new ParameterReference("USER",IDomain.OBJECT);
			}
			if (name.equalsIgnoreCase("MAX")) {
				if (index.getDimension().getType() == Type.CONTINUOUS) {
					if (index.getStatus()==Status.DONE) {
						List<DimensionMember> members = index.getMembers();
						if (!members.isEmpty()) {
							DimensionMember member = members.get(0);
							Object value = member.getID();
							if (value instanceof Intervalle) {
								Intervalle range = (Intervalle)value;
								return range.getUpperBoundExpression();
							}
						}
					}
				}
				// else - cannot evaluate but it's OK to try
				return new ParameterReference("MAX",index.getAxis().getDefinition().getImageDomain());
			} else if (name.equalsIgnoreCase("MIN")) {
				if (index.getDimension().getType() == Type.CONTINUOUS) {
					if (index.getStatus()==Status.DONE) {
						List<DimensionMember> members = index.getMembers();
						if (!members.isEmpty()) {
							DimensionMember member = members.get(0);
							Object value = member.getID();
							if (value instanceof Intervalle) {
								Intervalle range = (Intervalle)value;
								return range.getLowerBoundExpression();
							}
						}
					}
				}
				// else - cannot evaluate but it's OK to try
				return new ParameterReference("MIN",index.getAxis().getDefinitionSafe().getImageDomain());
			} else if (params.containsKey(name.toUpperCase())) {
				return new ParameterReference(name.toUpperCase(), params.get(name.toUpperCase()));
			}
		}
		// else
		return super.lookupObject(type, name);
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

	@Override
	public ExpressionDiagnostic validateExpression(ExpressionAST expr) {
		return ExpressionDiagnostic.IS_VALID;
	}

}