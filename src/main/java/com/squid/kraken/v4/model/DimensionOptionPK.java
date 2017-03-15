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
 * @author sergefantino
 *
 */
public class DimensionOptionPK extends DimensionPK {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1310603818794844938L;
	


    private String optionId;

    public DimensionOptionPK() {
    }
    
    public DimensionOptionPK(DimensionPK parent) {
        this(parent.getCustomerId(), parent.getProjectId(), parent.getDomainId(), parent.getDimensionId(), null);
    }

    public DimensionOptionPK(DimensionPK parent, String optionId) {
        this(parent.getCustomerId(), parent.getProjectId(), parent.getDomainId(), parent.getDimensionId(), optionId);
    }

    public DimensionOptionPK(String customerId, String projectId, String domainId, String dimensionId, String optionId) {
        super(customerId, projectId, domainId, dimensionId);
        this.optionId = optionId;
    }
    
    /**
	 * @return the optionId
	 */
	public String getOptionId() {
		return optionId;
	}
	
	/**
	 * @param optionId the optionId to set
	 */
	public void setOptionId(String optionId) {
		this.optionId = optionId;
	}
    
    @Override
    public String getObjectId() {
        return optionId;
    }

    @Override
    public void setObjectId(String id) {
        this.optionId = id;
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
        return "DimensionOptionPK [optionId =" + getOptionId() + ", dimensionId=" + getDimensionId() + ", getDomainId()=" + getDomainId()
                + ", getProjectId()=" + getProjectId() + ", getCustomerId()=" + getCustomerId() + "]";
    }

}
