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
package com.squid.kraken.v4.core.analysis.universe;

import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.model.ExpressionObject;

/**
 * A property of a Space
 * @author sergefantino
 *
 */
public interface Property {
	
	// provide a way to communicate the origin of the property
	public enum OriginType {
		USER, 		// created by the user
		ROLLUP, 	// created by the rollup analysis
		COMPARETO	// created by the compareTo analysis
	};
    
    /**
     * the space
     * @return
     */
    public Space getParent();
    
    /**
     * return the definition of the object - this is done by de-referencing it
     * @return
     * @throws ScopeException if cannot parse the object definition
     */
	public ExpressionAST getDefinition() throws ScopeException;

	/**
	 * same as getDefinition() but return UndefinedExpression if cannot parse the definition instead of throwing an error
	 * @return
	 */
	public ExpressionAST getDefinitionSafe();

	/**
	 * return an expression which is a reference to the property
	 * @return
	 */
	public ExpressionAST getReference();
	
	public ExpressionObject<?> getExpressionObject();
	
	/**
	 * return the origin type for this
	 * @return
	 */
	public OriginType getOriginType();

}
