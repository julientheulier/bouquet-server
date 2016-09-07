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

public class IntervalleObject extends IntervalleAbstract implements Comparable<IntervalleObject>, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 662637256055226823L;
	
	private Object lower_bound;
	private Object upper_bound;

    /**
     * try to create a interval, or return null if it not applicable
     * @param lower
     * @param upper
     * @return
     */
	public static IntervalleObject createInterval(Object lower, Object upper) {
	    try {
	        if (lower==null && upper==null) {// T1769
	            return null;
	        } else {
	            return new IntervalleObject(lower, upper);
	        }
	    } catch (Exception e) {
	        return null;
	    }
	}

	public IntervalleObject(Object lower_bound, Object upper_bound) {
		if (compareTo(lower_bound, upper_bound)<0) {
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
	public Object getLowerBound() {
		return lower_bound;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.core.analysis.model.Intervalle#getUpperBound()
	 */
	public Object getUpperBound() {
		return upper_bound;
	}

    /**
     * create a new interval that merges the given intervals
     * @return
     */
    public IntervalleObject merge(IntervalleObject with) {
        if (with==null) {
            return this;
        } else {
            Object lower = compareTo(getLowerBound(), with.getLowerBound()) < 0 ? getLowerBound() : with.getLowerBound();
            Object upper = compareTo(getUpperBound(), with.getUpperBound()) > 0 ? getUpperBound() : with.getUpperBound();
            return new IntervalleObject(lower, upper);
        }
    }

    /**
     * extend the interval to include the value
     * @param member
     * @return
     */
	public IntervalleObject include(Object value) {
		if (compareLowerBoundTo(value)>0) {
			return new IntervalleObject(value, upper_bound);
		} else if (compareUpperBoundTo(value)<0) {
			return new IntervalleObject(lower_bound, value);
		} else {
			return this;
		}
	}
    
	@SuppressWarnings("unchecked")
	private <T> int compareTo(T o1, T o2) {
		if (o1==null) {
			return -1;
		} else if (o2==null) {
			return 1;
		}
		else if (o1.getClass().isAssignableFrom(o2.getClass()) && o1 instanceof Comparable<?>) {
    		return ((Comparable<T>)o1).compareTo((T)o2);
    	} else {
    		return o1.toString().compareTo(o2.toString());
    	}
    }
    
    public int compareUpperBoundTo(Object value) {
    	return compareTo(upper_bound, value);
    }

    public int compareLowerBoundTo(Object value) {
    	return compareTo(lower_bound, value);
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
		Object this_lower = this.lower_bound;
		Object that_lower = that.lower_bound;
		int compare_lower = compareTo(this_lower, that_lower);
		if (compare_lower==0) {
			return compareTo(this.upper_bound, that.upper_bound);
		} else {
			return compare_lower;
		}
	}
	
	@Override
	public String toString() {
		return "[ "+lower_bound+" | "+upper_bound+" ]";
	}
	

}
