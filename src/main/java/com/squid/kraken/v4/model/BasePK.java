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

import javax.xml.bind.annotation.XmlTransient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.squid.kraken.v4.api.core.ServiceUtils;

@SuppressWarnings("serial")
public abstract class BasePK implements GenericPK {
    
    public BasePK() {
    }

    @XmlTransient
    @JsonIgnore
    @Override
    public final String toUUID() {
        if (getParent() == null) {
            return getObjectId();
        } else {
            return getParent().toUUID() + UUID_SEPARATOR + getObjectId();
        }
    }

    @XmlTransient
    @JsonIgnore
    @Override
    public abstract String getObjectId();

    @XmlTransient
    @JsonIgnore
    @Override
    public abstract void setObjectId(String id);

    @XmlTransient
    @JsonIgnore
    @Override
    public abstract GenericPK getParent();

    @XmlTransient
    @JsonIgnore
    @Override
    public abstract void setParent(GenericPK parent);

    @XmlTransient
    @JsonIgnore
    @Override
    public boolean isValid() {
        return ServiceUtils.getInstance().isValidId(getObjectId());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((getObjectId() == null) ? 0 : getObjectId().hashCode());
        result = prime * result + ((getParent() == null) ? 0 : getParent().hashCode());
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
        BasePK other = (BasePK) obj;
        if (getObjectId() == null) {
            if (other.getObjectId() != null)
                return false;
        } else if (!getObjectId().equals(other.getObjectId()))
            return false;
        if (getParent() == null) {
            if (other.getParent() != null)
                return false;
        } else if (!getParent().equals(other.getParent()))
            return false;
        return true;
    }

}
