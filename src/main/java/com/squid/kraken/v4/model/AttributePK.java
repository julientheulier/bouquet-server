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

@SuppressWarnings("serial")
@XmlRootElement
public class AttributePK extends DimensionPK {

    private String attributeId;

    public AttributePK() {
    }

    public AttributePK(DimensionPK parent, String attributeId) {
        this(parent.getCustomerId(), parent.getProjectId(), parent.getDomainId(), parent.getDimensionId(), attributeId);
    }

    public AttributePK(String customerId, String projectId, String domainId, String dimensionId, String attributeId) {
        super(customerId, projectId, domainId, dimensionId);
        this.attributeId = attributeId;
    }

    public String getAttributeId() {
        return attributeId;
    }

    public void setAttributeId(String attributeId) {
        this.attributeId = attributeId;
    }
    
    @Override
    public String getObjectId() {
        return attributeId;
    }

    @Override
    public void setObjectId(String id) {
        this.attributeId = id;
    }

    @Override
    public DimensionPK getParent() {
        return new DimensionPK(getCustomerId(), getProjectId(), getDomainId(), getDimensionId());
    }

    @Override
    public void setParent(GenericPK pk) {
        setCustomerId(((DimensionPK) pk).getCustomerId());
        setProjectId(((DimensionPK) pk).getProjectId());
        setDomainId(((DimensionPK) pk).getDomainId());
        setDimensionId(((DimensionPK) pk).getDimensionId());
    }

	@Override
	public String toString() {
		return "AttributePK [attributeId=" + attributeId
				+ ", getDimensionId()=" + getDimensionId() + ", getDomainId()="
				+ getDomainId() + ", getProjectId()=" + getProjectId()
				+ ", getCustomerId()=" + getCustomerId() + "]";
	}

}
