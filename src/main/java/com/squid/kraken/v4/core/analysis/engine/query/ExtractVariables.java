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
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.expression.visitor.LeafVisitor;

/**
 * for an ExpressionAST f(Vi) where Vi is a composable expression (variable), return the {Vi} list
 * @author sergefantino
 *
 */
public class ExtractVariables extends LeafVisitor<List<ExpressionAST>> {

	public List<ExpressionAST> apply(ExpressionAST expression) throws ScopeException {
		ArrayList<ExpressionAST> result = new ArrayList<>();
		visit(expression,result);
		return result;
	}
	
	@Override
	protected List<ExpressionAST> visit(ExpressionLeaf leaf, List<ExpressionAST> value) throws ScopeException {
		value.add(leaf);
		return value;
	}
	
	@Override
	protected List<ExpressionAST> visit(Compose compose, List<ExpressionAST> value) throws ScopeException {
		value.add(compose);
		return value;
	}

	@Override
	protected List<ExpressionAST> reduce(List<List<ExpressionAST>> map) {
		// TODO Auto-generated method stub
		return null;
	}
	

}
