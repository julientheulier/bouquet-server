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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.vegalite.VegaliteSpecs;

/**
 * @author sergefantino
 *
 */
public class ViewReply {
	
	@JsonIgnore
	private Space space;// this is for convenience
	
	private ViewQuery view;
	
	private AnalyticsQuery query;
	
	private ResultInfo resultInfo;
	
	// the analysis selection
	private AnalyticsSelection selection;
	
	private VegaliteSpecs result;
	
	public ViewReply() {
	}
	
	/**
	 * @return the view
	 */
	public ViewQuery getView() {
		return view;
	}
	
	/**
	 * @param view the view to set
	 */
	public void setView(ViewQuery view) {
		this.view = view;
	}

	public ViewReply(Space space) {
		this.space = space;
	}

	public AnalyticsQuery getQuery() {
		return query;
	}

	public void setQuery(AnalyticsQuery query) {
		this.query = query;
	}

	public AnalyticsSelection getSelection() {
		return selection;
	}

	public void setSelection(AnalyticsSelection selection) {
		this.selection = selection;
	}
	
	/**
	 * @return the info
	 */
	public ResultInfo getResultInfo() {
		return resultInfo;
	}
	
	/**
	 * @param info the info to set
	 */
	public void setResultInfo(ResultInfo info) {
		this.resultInfo = info;
	}

	public VegaliteSpecs getResult() {
		return result;
	}

	public void setResult(VegaliteSpecs result) {
		this.result = result;
	}
	
	/**
	 * @return the space
	 */
	public Space getSpace() {
		return space;
	}

}
