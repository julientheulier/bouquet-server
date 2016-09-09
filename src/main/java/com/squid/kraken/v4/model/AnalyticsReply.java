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

import io.swagger.annotations.ApiModelProperty;

/**
 * This is the result of an analysis
 * @author sergefantino
 *
 */
public class AnalyticsReply {
	
	// this is the query that generated this analysis
	private AnalyticsQuery query;
	
	// the analysis selection
	private FacetSelection selection;
	
	// the resulting dataTable
	private Object result;// this may depend on the output format
	
	/**
	 * 
	 */
	public AnalyticsReply() {
		// TODO Auto-generated constructor stub
	}

	public AnalyticsQuery getQuery() {
		return query;
	}

	public void setQuery(AnalyticsQuery query) {
		this.query = query;
	}

	@ApiModelProperty(hidden=true)// only used for LEGACY mode, so don't mess user with it
	public FacetSelection getSelection() {
		return selection;
	}

	public void setSelection(FacetSelection selection) {
		this.selection = selection;
	}

	public Object getResult() {
		return result;
	}

	public void setResult(Object result) {
		this.result = result;
	}

}
