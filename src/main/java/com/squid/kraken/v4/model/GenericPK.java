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

import javax.xml.bind.annotation.XmlTransient;

import org.mongodb.morphia.annotations.Embedded;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.squid.kraken.v4.api.core.ServiceUtils;

@Embedded
public interface GenericPK extends Serializable {
    
    public static final char UUID_SEPARATOR = ':';

    /**
     * Return the String representation of this PK. Used by the persistence layer to manipulate simple String-based ids
     * instead of composite ones.
     * 
     * @return String representation of this PK
     */
    public String toUUID();

    /**
     * Check this PK is valid : It must a string of 1-1,024 characters satisfying the regular expression
     * [a-zA-Z][\w]{0,1023}, where \w is any digit, underscore, or upper or lowercase letter.
     * 
     * @see ServiceUtils#validateId(String)
     */
    @XmlTransient
    @JsonIgnore
    public boolean isValid();
    
    /**
     * Get this PK's object id.
     * In case of a composite PK, this will return the top-most object's id.
     * @return
     */
    @XmlTransient
    @JsonIgnore
    public String getObjectId();
    
    /**
     * Set this PK's object id.
     */
    @XmlTransient
    @JsonIgnore
    public void setObjectId(String id);
    
    @XmlTransient
    @JsonIgnore
    public GenericPK getParent();
    
    @XmlTransient
    @JsonIgnore
    public void setParent(GenericPK pk);

}
