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

import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Interval value.
 */
@SuppressWarnings("serial")
@XmlType(namespace = "http://model.v4.kraken.squid.com")
@JsonTypeName("i")
public class FacetMemberInterval implements FacetMember, Serializable {
    
    private String lowerBound;
    
    private String upperBound;

    public FacetMemberInterval() {
        super();
    }

    public FacetMemberInterval(String lowerBound, String upperBound) {
        super();
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }
    
    /**
     * String representation of the lower boundary.<br>
     * If it's a date, the format is ISO 8601 ("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
     */
    public String getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(String lowerBound) {
        this.lowerBound = lowerBound;
    }

    /**
     * String representation of the upper boundary.<br>
     * If it's a date, the format is ISO 8601 ("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
     */
    public String getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(String upperBound) {
        this.upperBound = upperBound;
    }

    @Override
    public String toString() {
        return "[" + lowerBound + ", " + upperBound + "]";
    }
    

}
