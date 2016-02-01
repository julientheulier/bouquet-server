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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.squid.core.domain.operators.Operators;
import com.squid.core.expression.Compose;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.Operator;
import com.squid.core.expression.scope.ScopeException;

/**
 * extract the possible outcomes of the expression, e.g if the expression is case(test,A,B), returns A,B
 * @author sergefantino
 *
 * @param <T>
 */
public class ExtractOutcomes {

    /**
     * 
     */
    public ExtractOutcomes() {
        super();
    }
    
    public List<ExpressionAST> apply(ExpressionAST expr) throws ScopeException {
        return visit(expr);
    }

	protected List<ExpressionAST> visit(ExpressionAST expression) throws ScopeException {
		if (expression instanceof Operator) {
			return visit((Operator)expression);
		} else 
		if (expression instanceof Compose) {
			return visit((Compose)expression);
		} else {
			return Collections.singletonList(expression);
		}
	}
	
	protected List<ExpressionAST> visit(Compose compose) throws ScopeException {
        return visit(compose.getHead());
	}

	protected List<ExpressionAST> visit(Operator operator) throws ScopeException {
	    if (operator.getOperatorDefinition().equals(Operators.CASE)) {
	        if (operator.getArguments().size()>1) {
	            int pos = 0;
                ArrayList<ExpressionAST> flatten = new ArrayList<>();
	            for (ExpressionAST arg : operator.getArguments()) {
	                if (pos%2==1) {
	                    flatten.addAll(visit(arg));
	                } else if (pos+1==operator.getArguments().size()) {
	                    flatten.addAll(visit(arg));// default case
	                }
	                pos++;
	            }
	            return flatten;
	        } else {
                return Collections.singletonList((ExpressionAST)operator);
	        }
	    } else {
	        if (operator.getArguments().size()==1) {
	            return visit(operator.getArguments().get(0));
	        } else {
	            return Collections.singletonList((ExpressionAST)operator);
	        }
	    }
	}
	
	

}
