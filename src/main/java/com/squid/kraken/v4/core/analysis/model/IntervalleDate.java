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

import java.util.Calendar;
import java.util.Date;

@Deprecated
public class IntervalleDate extends IntervalleAbstract implements Comparable<IntervalleDate> {
	
	private Date lower_bound;
	private Date upper_bound;

	public IntervalleDate(Date lower_bound, Date upper_bound) {
		this.lower_bound = lower_bound;
		this.upper_bound = upper_bound;
	}

	public static Date date(int year, int month, int day) {
		Calendar c = Calendar.getInstance();
		c.set(year, month-1, day);
		return c.getTime();
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.core.analysis.model.Intervalle#getLowerBound()
	 */
	public Object getLowerBound() {
		return lower_bound;
	}
	
	public Date getLowerBoundDate() {
	    return lower_bound;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.core.analysis.model.Intervalle#getUpperBound()
	 */
	public Object getUpperBound() {
		return upper_bound;
	}
	
	public Date getUpperBoundDate() {
	    return upper_bound;
	}

	/**
	 * compute the same intervalle (same number of days) but for previous year
	 * @return
	 */
	public IntervalleDate previousYear() {
		Calendar lower = null;
		if (lower_bound!=null) {
			lower = Calendar.getInstance();
			lower.setTime(lower_bound);
			lower.add(Calendar.YEAR, -1);
		}
		Calendar upper = null;
		if (upper_bound!=null) {
			upper = Calendar.getInstance();
			upper.setTime(upper_bound);
			upper.add(Calendar.YEAR, -1);
		}
		return new IntervalleDate(lower.getTime(), upper.getTime());
	}
	
	@Override
	public int compareTo(IntervalleDate that) {
		Date this_lower = this.lower_bound;
		Date that_lower = that.lower_bound;
		int compare_lower = this_lower.compareTo(that_lower);
		if (compare_lower==0) {
			return (this.upper_bound).compareTo(that.upper_bound);
		} else {
			return compare_lower;
		}
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IntervalleDate other = (IntervalleDate) obj;
		if (lower_bound == null) {
			if (other.lower_bound != null)
				return false;
		} else if (!lower_bound.equals(other.lower_bound))
			return false;
		if (upper_bound == null) {
			if (other.upper_bound != null)
				return false;
		} else if (!upper_bound.equals(other.upper_bound))
			return false;
		return true;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((lower_bound == null) ? 0 : lower_bound.hashCode());
		result = prime * result
				+ ((upper_bound == null) ? 0 : upper_bound.hashCode());
		return result;
	}

    @Override
    public String toString() {
        return "IntervalleDate [lower_bound=" + lower_bound + ", upper_bound="
                + upper_bound + "]";
    }
		
}
