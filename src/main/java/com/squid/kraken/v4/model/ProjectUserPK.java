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
/**
 * As Users are not linked to projects, to store a "last read date" per project we need to create a specific data
 * model.
 * 
 * @author danhddv
 */
public class ProjectUserPK extends ProjectPK {

    /**
     * Id of the user who read the annotation.
     */
    private String userId;

    /**
     * Default constructor.
     */
    public ProjectUserPK() {
    }

    /**
     * Constructor with customerId and projectId.
     * 
     * @param customerId
     *            customer id
     * @param projectId
     *            project id
     */
    public ProjectUserPK(String customerId, String projectId, String userId) {
        super(customerId, projectId);
        this.userId = userId;
    }

    /**
     * Get the "userId" variable.
     * 
     * @return the userId
     */
    public String getUserId() {
        return userId;
    }

    /**
     * Set the "userId" variable.
     * 
     * @param userId
     *            the userId to set
     */
    public void setUserId(String userId) {
        this.userId = userId;
    }

    @Override
    public String toString() {
        return "ProjectUserPK [userId = " + userId + ", customerId = " + getCustomerId() + ", projectId = "
                + getProjectId() + "]";
    }

    @Override
    public String getObjectId() {
        return userId;
    }

    @Override
    public void setObjectId(String id) {
        userId = id;
    }
    
    @Override
    public ProjectPK getParent() {
        return new ProjectPK(getCustomerId(), getProjectId());
    }

    @Override
    public void setParent(GenericPK pk) {
        setCustomerId(((ProjectPK) pk).getCustomerId());
        setProjectId(((ProjectPK) pk).getProjectId());
    }

}
