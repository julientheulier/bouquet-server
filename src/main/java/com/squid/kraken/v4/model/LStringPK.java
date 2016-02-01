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

/**
 * PK for Localized Strings.
 */
@SuppressWarnings("serial")
public class LStringPK extends CustomerPK {

    private String type;

    private String att;

    private String locale;

    private String refId;

    public LStringPK() {
    }

    public LStringPK(String customerId, String type, String att, String refId, String locale) {
        super(customerId);
        this.type = type;
        this.att = att;
        this.locale = locale;
        this.refId = refId;
    }

    public String getObjectId() {
        return type + UUID_SEPARATOR + att + UUID_SEPARATOR + refId + UUID_SEPARATOR + locale;
    }

    public void setObjectId(String id) {
        // not applicable
    }

    @Override
    public boolean isValid() {
        // not applicable
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((att == null) ? 0 : att.hashCode());
        result = prime * result + ((locale == null) ? 0 : locale.hashCode());
        result = prime * result + ((refId == null) ? 0 : refId.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        LStringPK other = (LStringPK) obj;
        if (att == null) {
            if (other.att != null)
                return false;
        } else if (!att.equals(other.att))
            return false;
        if (locale == null) {
            if (other.locale != null)
                return false;
        } else if (!locale.equals(other.locale))
            return false;
        if (refId == null) {
            if (other.refId != null)
                return false;
        } else if (!refId.equals(other.refId))
            return false;
        if (type == null) {
            if (other.type != null)
                return false;
        } else if (!type.equals(other.type))
            return false;
        return true;
    }

    @Override
    public GenericPK getParent() {
        return new CustomerPK(getCustomerId());
    }

    @Override
    public void setParent(GenericPK pk) {
        setCustomerId(((CustomerPK) pk).getCustomerId());
    }
}
