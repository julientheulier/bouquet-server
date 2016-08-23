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
package com.squid.kraken.v4.core.analysis.engine.hierarchy;

import java.io.Serializable; 

import javax.xml.bind.annotation.XmlRootElement;

/**
 * A member in a dimension
 *
 */
@XmlRootElement
public class DimensionMember 
implements Comparable<DimensionMember>, Serializable
{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 933767227130158281L;

	public static final int NULL = -1;
	
	private int index;
	private Object ID;
	//private DimensionMemberReference reference;
	private Object[] attrs;
	
	public DimensionMember(Object[] raw) {
	    super();
	    this.index = -1;
	    this.ID = raw[0];
	    if (raw.length>1) {
    	    this.attrs = new Object[raw.length-1];
    	    for (int k=1;k<raw.length;k++) {
    	        this.attrs[k-1] = raw[k];
    	    }
	    }
	}
	
    public DimensionMember(int index, Object value, int size) {
		super();
		this.index = index;
		/*
		if (value instanceof DimensionMemberReference) {
			ID = ((DimensionMemberReference)value).getValue();
			reference = (DimensionMemberReference)value;
		} else {
			ID = value;
			reference = new DimensionMemberReference((Comparable)value);
		}
		*/
		ID = value;
		if (size>0) {
			attrs = new Object[size];
		}
	}

    /**
     * this is an index value provided by the store. It is not mandatory, and it is not a unique ID.
     * However if the member value is not known by the store, it should return an index value of -1
     * @return
     */
	public int getIndex() {
		return index;
	}
	
	public void setIndex(int index) {
	    this.index = index;
	}
	
	public String getKey() {
	    return ID.toString();
	}

	public Object getID() {
		return ID;
	}

	public Object[] getAttributes() {
		return attrs;
	}

	public void setAttribute(int i, Object value) {
		attrs[i] = value;
	}

	@Override
    public String toString() {
		if (attrs==null) {
			return "Member [index=" + index + ", ID=" + ID + "]";
		} else {
			return "Member [index=" + index + ", ID=" + ID + ", attrs=" + attrs[0] + "...]";
		}
    }

	public Object getDisplayName() {
		return getID();
	}

	@Override
	public int compareTo(DimensionMember that) {
		if (that==null) return 1;
		if (this==that) return 0;
		if (this.index>=0 && that.index>=0) {
			if (this.index<that.index) return -1; else return 1;
		}
		if (this.ID instanceof Comparable) {
			return ((Comparable)this.ID).compareTo(((Comparable)that.ID));
		}
		return this.ID.toString().compareTo(that.ID.toString());
	}
	
    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ID == null) ? 0 : ID.hashCode());
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
		DimensionMember other = (DimensionMember) obj;
		if (ID == null) {
			if (other.ID != null)
				return false;
		} else if (!ID.equals(other.ID))
			return false;
		return true;
	}

	/**
     * check if the member match a given substring
     * @param
     */
    public boolean match(CharSequence filter) {
        if (ID.toString().toLowerCase().contains(filter)) {
            return true;
        } else if (attrs!=null && attrs.length>0) {
            for (int i=0;i<attrs.length;i++) {
                if (attrs[i].toString().toLowerCase().contains(filter)) {
                    return true;
                }
            }
        }
        // else
        return false;
    }

}
