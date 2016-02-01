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

/**
 * an ExpressionInput associate the user input and the corresponding expression
 * @author sergefantino
 *
 */
public class ExpressionInput {
	
	private String input;
	private ExpressionAST expression;
	
	public ExpressionInput(String input, ExpressionAST expression) {
		super();
		this.input = input;
		this.expression = expression;
	}

	public String getInput() {
		return input;
	}

	public ExpressionAST getExpression() {
		return expression;
	}

	@Override
	public int hashCode() {
		return ((expression == null) ? 0 : expression.hashCode());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ExpressionInput other = (ExpressionInput) obj;
		if (expression == null) {
			if (other.expression != null)
				return false;
		} else if (!expression.equals(other.expression))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ExpressionInput [input=" + input + "]";
	}

}
