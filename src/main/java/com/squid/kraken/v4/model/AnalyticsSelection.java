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

import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * This is the analysis selection
 * @author sergefantino
 *
 */
@JsonDeserialize(as = AnalyticsSelectionImpl.class)
public interface AnalyticsSelection {
	
	/**
	 * get the period expression, used to filter the timeframe.
	 * This must be a valid date or timestamp expression.
	 * @return
	 */
	public String getPeriod();
	
	public void setPeriod(String expression);
	
	public List<String> getTimeframe();
	
	public void setTimeframe(List<String> timeframe);
	
	public List<String> getCompareTo();
	
	public void setCompareTo(List<String> compareframe);

	public List<String> getFilters();

	public void setFilters(List<String> filters);

}
