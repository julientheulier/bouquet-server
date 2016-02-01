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

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * PK Composed of a Customer Id.
 */
@SuppressWarnings("serial")
@XmlRootElement
public class CustomerPK extends BasePK {
    
    private String customerId;
    
    public CustomerPK() {
    }
    
    public CustomerPK(String customerId) {
        this.customerId = customerId;
    }

    @XmlTransient
    @JsonIgnore
    public String getCustomerId() {
        return customerId;
    }

    @XmlTransient
    @JsonIgnore
    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }
    
    @XmlTransient
    @JsonIgnore
    @Override
    public String getObjectId() {
        return customerId;
    }
    
    @XmlTransient
    @JsonIgnore
    @Override
    public void setObjectId(String id) {
        customerId = id;
    }
    
    @Override
    public String toString() {
        return "CustomerPK [customerId=" + getCustomerId() + "]";
    }

    @Override
    public GenericPK getParent() {
        return null;
    }

    @Override
    public void setParent(GenericPK pk) {
    }

}
