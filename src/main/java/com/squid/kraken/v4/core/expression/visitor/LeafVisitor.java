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
package com.squid.kraken.v4.core.expression.visitor;

import java.util.ArrayList;
import java.util.List;

import com.squid.core.expression.Compose;
import com.squid.core.expression.ConstantValue;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.ExpressionLeaf;
import com.squid.core.expression.Operator;
import com.squid.core.expression.scope.ScopeException;

/**
 * visit the leaves of an expression - that is it will ignore the compose path and only visit the head
 * @author sergefantino
 *
 * @param <T>
 */
public abstract class LeafVisitor<T> {

	protected T visit(ExpressionAST expression, T value) throws ScopeException {
		if (expression instanceof Operator) {
			return visit((Operator)expression, value);
		} else 
		if (expression instanceof Compose) {
			return visit((Compose)expression, value);
		} else {
			if (expression instanceof ConstantValue) {
				return visit((ConstantValue)expression, value);
			} else if (expression instanceof ExpressionLeaf) {
				return visit((ExpressionLeaf)expression, value);
			} else {
				return value;
			}
		}
	}
	
	protected T visit(ConstantValue constant, T value) {
		return value;
	}
	
	protected T visit(ExpressionLeaf leaf, T value) throws ScopeException {
		return value;
	}
	
	protected T visit(Compose compose, T value) throws ScopeException {
		return visit(compose.getHead(), value);
	}

	protected T visit(Operator operator, T value) throws ScopeException {
		ArrayList<T> map = new ArrayList<T>();
		for (ExpressionAST argument : operator.getArguments()) {
			map.add(visit(argument, value));
		}
		return reduce(map);
	}

	protected abstract T reduce(List<T> map);

}
