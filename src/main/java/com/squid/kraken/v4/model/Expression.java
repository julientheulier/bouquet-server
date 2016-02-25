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

import java.io.Serializable;
import java.util.Collection;

import org.mongodb.morphia.annotations.Embedded;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.wordnik.swagger.annotations.ApiModelProperty;

@SuppressWarnings("serial")
@Embedded
public class Expression implements Serializable {
    
    private String value;
    
    // T450
    @JsonIgnore
    @ApiModelProperty(hidden=true)
    private String internal = null;// store the identifier only reference expression
    
    // T446
    @JsonIgnore
    @ApiModelProperty(hidden=true)
    private int level = 0;// level of references (0=no model references, 1=at least one indirect reference, ...)
    
    @JsonIgnore
    @ApiModelProperty(hidden=true)
    private Collection<ReferencePK> references = null;
    
    public Expression() {
    }

    public Expression(String value) {
        this.value = value;
    }

    /**
     * get the public representation of the expression
     * @return
     */
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
    
    /**
     * get the internal representation of the expression (encoded using IDs)
     * @return
     */
    public String getInternal() {
		return internal;
	}
    
    public void setInternal(String expression) {
		this.internal = expression;
	}
    
    @JsonIgnore
    public String getInternalSafe() {
    	if (internal!=null) {
    		return internal;
    	} else {
    		return value;
    	}
    }
    
    public int getLevel() {
		return level;
	}
    
    public void setLevel(int level) {
		this.level = level;
	}
    
    public Collection<ReferencePK> getReferences() {
		return references;
	}
    
    public void setReferences(Collection<ReferencePK> references) {
		this.references = references;
	}

    @Override
    public String toString() {
        return "Expression [value=" + value + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((value == null) ? 0 : value.hashCode());
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
        Expression other = (Expression) obj;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        return true;
    }

}
