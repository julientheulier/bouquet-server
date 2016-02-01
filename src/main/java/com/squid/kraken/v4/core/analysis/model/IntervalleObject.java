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

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

public class IntervalleObject extends IntervalleAbstract implements Comparable<IntervalleObject>, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 662637256055226823L;
	
	private Comparable lower_bound;
	private Comparable upper_bound;

    /**
     * create a new interval that merges the given intervals
     * @return
     */
    public static IntervalleObject merge(IntervalleObject first, IntervalleObject second) {
        if (first==null) {
            return second;
        } else if (second==null) {
            return first;
        } else {
            Comparable lower = first.getLowerBound().compareTo(
                    second.getLowerBound()) < 0 ? first
                    .getLowerBound() : second.getLowerBound();
            Comparable upper = first.getUpperBound().compareTo(
                    second.getUpperBound()) > 0 ? first
                    .getUpperBound() : second.getUpperBound();
            return new IntervalleObject(lower, upper);
        }
    }
	
    /**
     * try to create a interval, or return null if it not applicable
     * @param lower
     * @param upper
     * @return
     */
	public static IntervalleObject createInterval(Object lower, Object upper) {
	    try {
	        if (lower==null || upper==null) {
	            return null;
	        } else {
	            return new IntervalleObject((Comparable)lower, (Comparable)upper);
	        }
	    } catch (Exception e) {
	        return null;
	    }
	}

	public IntervalleObject(Comparable lower_bound, Comparable upper_bound) {
		if (lower_bound.compareTo(upper_bound)<0) {
			this.lower_bound = lower_bound;
			this.upper_bound = upper_bound;
		} else {
			this.lower_bound = upper_bound;
			this.upper_bound = lower_bound;
		}
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.core.analysis.model.Intervalle#getLowerBound()
	 */
	public Comparable getLowerBound() {
		return lower_bound;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.core.analysis.model.Intervalle#getUpperBound()
	 */
	public Comparable getUpperBound() {
		return upper_bound;
	}

	public static Date date(int year, int month, int day) {
		Calendar c = Calendar.getInstance();
		c.set(year, month-1, day-1);
		return c.getTime();
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
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IntervalleObject other = (IntervalleObject) obj;
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
	public int compareTo(IntervalleObject that) {
		Comparable this_lower = this.lower_bound;
		Comparable that_lower = that.lower_bound;
		int compare_lower = this_lower.compareTo(that_lower);
		if (compare_lower==0) {
			return (this.upper_bound).compareTo(that.upper_bound);
		} else {
			return compare_lower;
		}
	}
	
	@Override
	public String toString() {
		return "[ "+lower_bound.toString()+" | "+upper_bound.toString()+" ]";
	}
	

}
