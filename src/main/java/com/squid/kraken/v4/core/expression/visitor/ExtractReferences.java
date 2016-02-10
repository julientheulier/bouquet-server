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
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.ExpressionLeaf;
import com.squid.core.expression.ExpressionRef;
import com.squid.core.expression.scope.ScopeException;

/**
 * extract every references from the expression
 * @author sergefantino
 *
 */
public class ExtractReferences extends LeafVisitor<List<ExpressionRef>> {

	public List<ExpressionRef> apply(ExpressionAST expression) throws ScopeException {
		ArrayList<ExpressionRef> result = new ArrayList<ExpressionRef>();
		visit(expression,result);
		return result;
	}
	
	@Override
	protected List<ExpressionRef> visit(ExpressionLeaf leaf, List<ExpressionRef> value) throws ScopeException {
		if (leaf instanceof ExpressionRef) {
			ExpressionRef ref = (ExpressionRef)leaf;
			value.add(ref);
			return value;
		} else {
			return value;
		}
	}

	/**
	 * visit the full path
	 * @param compose
	 * @param value
	 * @return
	 * @throws ScopeException
	 */
	protected List<ExpressionRef> visit(Compose compose, List<ExpressionRef> value) throws ScopeException {
		for (ExpressionAST argument : compose.getBody()) {
			visit(argument, value);
		}
		return value;
	}

	@Override
	protected List<ExpressionRef> reduce(List<List<ExpressionRef>> map) {
		// just ignore because we are performing side-effects
	    return null;
	}
}
