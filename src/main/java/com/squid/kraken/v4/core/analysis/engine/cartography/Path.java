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
package com.squid.kraken.v4.core.analysis.engine.cartography;

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.DomainPK;

/**
 * Path interface describe a composition of relation, and provide the overall cardinality
 */
public interface Path {
	
	public enum Type {
	    ONE_ONE, ONE_MANY, MANY_ONE, MANY_MANY, INFINITE
	}
	
	public Path.Type getType();
	
	/**
	 * apply the path to the root space and construct a new space (if possible)
	 * @param s
	 * @return
	 * @throws ScopeException
	 */
	public Space apply(Space s) throws ScopeException;
	
	/**
	 * the path size
	 * @return
	 */
	public int size();
	
	/**
	 * check if the path pass by some node
	 * @return
	 */
	public boolean contains(DomainPK node);
}