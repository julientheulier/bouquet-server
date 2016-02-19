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
package com.squid.kraken.v4.core.analysis.engine.project;

import java.util.List;

import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.query.ExtractVariables;
import com.squid.kraken.v4.core.analysis.engine.query.ReplaceVariables;

/**
 * ExpressionFunctor provides a way to apply an expression to different source IDomain
 * @author sergefantino
 *
 */
public class ExpressionFunctor {
	
	private ExpressionAST definition;
	private List<ExpressionAST> variables;

	public ExpressionFunctor(ExpressionAST definition) throws ScopeException {
		this.definition = definition;
		this.variables = extractVariablesn(definition);
	}
	
	public ExpressionAST getDefinition() {
		return definition;
	}
	
	public List<ExpressionAST> getVariables() {
		return variables;
	}
	
	protected List<ExpressionAST> extractVariablesn(ExpressionAST definition) throws ScopeException {
		ExtractVariables extractor = new ExtractVariables();
		return extractor.apply(definition);
	}

	/**
	 * replace occurrence of the variable in the definition by rebind
	 * @param definition
	 * @param variable
	 * @param rebind
	 * @return
	 * @throws ScopeException 
	 */
	public ExpressionFunctor replace(ExpressionAST variable, ExpressionAST replacement) throws ScopeException {
		ReplaceVariables visitor = new ReplaceVariables(variable, replacement);
		ExpressionAST replaced = visitor.apply(getDefinition());
		return new ExpressionFunctor(replaced);
	}
	
}
