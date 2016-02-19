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
package com.squid.kraken.v4.core.analysis.engine.query;

import java.util.ArrayList;
import java.util.List;

import com.squid.core.expression.Compose;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.ExpressionLeaf;
import com.squid.core.expression.Operator;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.expression.visitor.LeafVisitor;

/**
 * replace any occurrence of a expression
 * @author sergefantino
 *
 */
public class ReplaceVariables extends LeafVisitor<ExpressionAST> {
	
	private ExpressionAST variable;
	private ExpressionAST replacement;
	
	public ReplaceVariables(ExpressionAST variable, ExpressionAST replacement) {
		super();
		this.variable = variable;
		this.replacement = replacement;
	}

	public ExpressionAST apply(ExpressionAST expression) throws ScopeException {
		return visit(expression,expression);
	}
	
	@Override
	protected ExpressionAST visit(ExpressionAST expression, ExpressionAST value) throws ScopeException {
		if (expression.equals(variable)) {
			return replacement;
		} else {
			return super.visit(expression, value);
		}
	}
	
	@Override
	protected ExpressionAST visit(Compose compose, ExpressionAST value) throws ScopeException {
		return ExpressionMaker.COMPOSE(visit(compose.getTail(),value), visit(compose.getHead(),value));
	}
	
	@Override
	protected ExpressionAST visit(Operator operator, ExpressionAST value) throws ScopeException {
		ArrayList<ExpressionAST> args = new ArrayList<ExpressionAST>();
		for (ExpressionAST argument : operator.getArguments()) {
			args.add(visit(argument, value));
		}
		return ExpressionMaker.op(operator.getOperatorDefinition(), args);
	}
	
	@Override
	protected ExpressionAST visit(ExpressionLeaf leaf, ExpressionAST value) throws ScopeException {
		return leaf;
	}
	
	@Override
	protected ExpressionAST reduce(List<ExpressionAST> map) {
		// not needed
		return null;
	}

}
