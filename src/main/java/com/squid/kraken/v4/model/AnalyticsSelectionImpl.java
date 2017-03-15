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

/**
 * @author sergefantino
 *
 */
public class AnalyticsSelectionImpl implements AnalyticsSelection {
	
	private String period;
	
	private List<String> timeframe;
	
	private List<String> compareTo;
	
	private List<String> filters;
	
	/**
	 * 
	 */
	public AnalyticsSelectionImpl() {
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * @param copy
	 */
	public AnalyticsSelectionImpl(AnalyticsQuery copy) {
		this.period = copy.getPeriod();
		this.timeframe = copy.getTimeframe();
		this.compareTo = copy.getCompareTo();
		this.filters = copy.getFilters();
	}

	public String getPeriod() {
		return period;
	}

	public void setPeriod(String period) {
		this.period = period;
	}
	
	public List<String> getTimeframe() {
		return timeframe;
	}

	public void setTimeframe(List<String> timeframe) {
		this.timeframe = timeframe;
	}

	public List<String> getCompareTo() {
		return compareTo;
	}

	public void setCompareTo(List<String> compareTo) {
		this.compareTo = compareTo;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#getFilters()
	 */
	@Override
	public List<String> getFilters() {
		return filters;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#setFilters(java.util.List)
	 */
	@Override
	public void setFilters(List<String> filters) {
		this.filters = filters;
	}

}
