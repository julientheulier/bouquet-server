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
public class AccessTokenPK implements GenericPK {

    private String tokenId;
    
    public AccessTokenPK() {
    }

    public AccessTokenPK(String tokenId) {
        this.tokenId = tokenId;
    }

    public String getTokenId() {
        return tokenId;
    }

    public void setTokenId(String tokenId) {
        this.tokenId = tokenId;
    }

    @Override
    public String toUUID() {
        return tokenId;
    }

    @Override
    public String toString() {
        return "AccessTokenPK [tokenId=" + tokenId + "]";
    }
    
    @XmlTransient
    @JsonIgnore
    @Override
    public boolean isValid() {
        return ServiceUtils.getInstance().isValidId(tokenId);
    }
    
    @XmlTransient
    @JsonIgnore
    @Override
    public String getObjectId() {
        return tokenId;
    }
    
    @XmlTransient
    @JsonIgnore
    @Override
    public void setObjectId(String id) {
        tokenId = id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((tokenId == null) ? 0 : tokenId.hashCode());
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
        AccessTokenPK other = (AccessTokenPK) obj;
        if (tokenId == null) {
            if (other.tokenId != null)
                return false;
        } else if (!tokenId.equals(other.tokenId))
            return false;
        return true;
    }
    
    @XmlTransient
    @JsonIgnore
    public GenericPK getParent() {
        return null;
    }

    @XmlTransient
    @JsonIgnore
    public void setParent(GenericPK pk) {
    }
    
}
