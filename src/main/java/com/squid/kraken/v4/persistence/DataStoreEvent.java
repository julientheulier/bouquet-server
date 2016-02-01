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
package com.squid.kraken.v4.persistence;

/**
 * An Event triggered when an object is updated/created/deleted.<br>
 * The 'source' object is the modified object instance.<br>
 */
public class DataStoreEvent {

    static public enum Type {
        CREATION, UPDATE, DELETE, INVALIDATE
    };

	private Object origin;
    private final Object source;
    private final boolean isExternal;
    private final Type type;

    public DataStoreEvent(Object source, Type eventType) {
    	this(null, source, eventType);
    }

    public DataStoreEvent(Object origin, Object source, Type eventType) {
    	this(origin, source, eventType, false);
    }
    
    public DataStoreEvent(Object origin, Object source, Type eventType, boolean isExternal) {
    	this.origin = origin;
        this.source = source;
        this.type = eventType;
        this.isExternal = isExternal;
    }

    public Object getOrigin() {
		return origin;
	}

	public Object getSource() {
        return source;
    }

    public Type getType() {
        return type;
    }

    @Override
    public String toString() {
        return "DataStoreEvent [source=" + source + ", type=" + type + "]";
    }

    public boolean isExternal() {
    	return isExternal;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (isExternal ? 1231 : 1237);
		result = prime * result + ((origin == null) ? 0 : origin.hashCode());
		result = prime * result + ((source == null) ? 0 : source.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
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
		DataStoreEvent other = (DataStoreEvent) obj;
		if (isExternal != other.isExternal)
			return false;
		if (origin == null) {
			if (other.origin != null)
				return false;
		} else if (!origin.equals(other.origin))
			return false;
		if (source == null) {
			if (other.source != null)
				return false;
		} else if (!source.equals(other.source))
			return false;
		if (type != other.type)
			return false;
		return true;
	}
    
}
