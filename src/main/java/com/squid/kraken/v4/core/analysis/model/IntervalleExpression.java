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
package com.squid.kraken.v4.core.analysis.model;

import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;


public class IntervalleExpression extends IntervalleAbstract {
	
	private ExpressionAST lower;
	private ExpressionAST upper;
	
	public IntervalleExpression(ExpressionAST lower, ExpressionAST upper) {
		this.lower = lower;
		this.upper = upper;
	}
	
	public IntervalleExpression(Object lower, Object upper) throws ScopeException {
		this.lower = ExpressionMaker.CONSTANT(lower);
		this.upper = ExpressionMaker.CONSTANT(upper);
	}

	@Override
	public Object getLowerBound() {
		return lower;
	}

	@Override
	public Object getUpperBound() {
		return upper;
	}

}
