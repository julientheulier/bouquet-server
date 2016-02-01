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
import com.squid.core.database.model.Table;
import com.squid.core.expression.Compose;
import com.squid.core.expression.ConstantValue;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.ExpressionLeaf;
import com.squid.core.expression.Operator;
import com.squid.core.expression.reference.ColumnReference;
import com.squid.core.expression.reference.TableReference;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.scope.AxisExpression;
import com.squid.kraken.v4.core.analysis.scope.MeasureExpression;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.reference.DomainReference;
import com.squid.kraken.v4.core.expression.reference.RelationReference;

/**
 * extract the tables involved in the expression
 * @author sergefantino
 *
 * @param <T>
 */
public class ExtractTables {
    
    private Universe universe;

    /**
     * 
     * @param universe needed to lookup the table
     */
    public ExtractTables(Universe universe) {
        super();
        this.universe = universe;
    }
    
    public List<Table> apply(ExpressionAST expr) throws ScopeException {
        ArrayList<Table> result = new ArrayList<>();
        visit(expr,result);
        return result;
    }

	protected void visit(ExpressionAST expression, List<Table> value) throws ScopeException {
		if (expression instanceof Operator) {
			visit((Operator)expression, value);
		} else 
		if (expression instanceof Compose) {
			visit((Compose)expression, value);
		} else {
			if (expression instanceof ConstantValue) {
				//visit((ConstantValue)expression, value);
			} else if (expression instanceof ExpressionLeaf) {
				visit((ExpressionLeaf)expression, value);
			} else {
				// void
			}
		}
	}
	
	protected void visit(ConstantValue constant, List<Table> value) {
		//
	}
	
	protected void visit(ExpressionLeaf leaf, List<Table> value) throws ScopeException {
	    if (leaf instanceof TableReference) {
	        TableReference table = (TableReference)leaf;
	        value.add(table.getTable());
	    } else if (leaf instanceof DomainReference) {
	        DomainReference domain = (DomainReference)leaf;
	        universe.getTable(domain.getDomain());
	    } else if (leaf instanceof ColumnReference) {
            Column column = ((ColumnReference)leaf).getColumn();
            value.add(column.getTable());
        } else if (leaf instanceof AxisExpression) {
            Axis axis = ((AxisExpression)leaf).getAxis();
            visit(axis.getDefinition(),value);
        } else if (leaf instanceof MeasureExpression) {
            Measure measure = ((MeasureExpression)leaf).getMeasure();
            visit(measure.getDefinition(),value);
        } else if (leaf instanceof RelationReference) {
            RelationReference relation = (RelationReference)leaf;
            ExpressionAST join = universe.getParser().parse(relation.getRelation());
            visit(join,value);
        }
	}
	
	protected void visit(Compose compose, List<Table> value) throws ScopeException {
        for (ExpressionAST expr : compose.getBody()) {
            visit(expr,value);
        }
	}

	protected void visit(Operator operator, List<Table> value) throws ScopeException {
		for (ExpressionAST argument : operator.getArguments()) {
			visit(argument, value);
		}
	}
	
	

}
