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

import com.squid.core.database.model.Column;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.ExpressionLeaf;
import com.squid.core.expression.reference.ColumnReference;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.scope.AxisExpression;
import com.squid.kraken.v4.core.analysis.scope.MeasureExpression;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;

public class ExtractColumns extends LeafVisitor<List<Column>> {
	
	public List<Column> apply(ExpressionAST expression) throws ScopeException {
		ArrayList<Column> result = new ArrayList<Column>();
		visit(expression,result);
		return result;
	}
	
	@Override
	protected List<Column> visit(ExpressionLeaf leaf, List<Column> value) throws ScopeException {
		if (leaf instanceof ColumnReference) {
			Column column = ((ColumnReference)leaf).getColumn();
			value.add(column);
			return value;
		} else if (leaf instanceof AxisExpression) {
			Axis axis = ((AxisExpression)leaf).getAxis();
			return visit(axis.getDefinition(),value);
		} else if (leaf instanceof MeasureExpression) {
			Measure measure = ((MeasureExpression)leaf).getMeasure();
			return visit(measure.getDefinition(),value);
		} else {
			return value;
		}
	}

	@Override
	protected List<Column> reduce(List<List<Column>> map) {
	    /*
		ArrayList<Column> flatten = new ArrayList<>();
		for (List<Column> list : map) {
		    flatten.addAll(list);
		}
		return flatten;
		*/
	    return null;
	}

}
