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
package com.squid.kraken.v4.model;

/**
 * extend the Expression to allow adding a name (label)
 * note: this is done that way to be backward compatible with the ProjectAnalysisJob API
 * @author sergefantino
 *
 */
public class FacetExpression extends Expression {

	/**
	 * 
	 */
	private static final long serialVersionUID = -91190929258335779L;
	
	private String name = null;// allow to name the facet - this will be use in the analysis output to reference the facet

	public FacetExpression() {
		super();
	}

	public FacetExpression(String value) {
		super(value);
	}

	public FacetExpression(String value, String name) {
		super(value);
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
}
