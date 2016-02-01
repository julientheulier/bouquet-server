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
public class ProjectPK extends CustomerPK {

    private String projectId;

    public ProjectPK() {
    }
    
    public ProjectPK(CustomerPK parent) {
        this(parent.getCustomerId(), null);
    }

    public ProjectPK(CustomerPK parent, String projectId) {
        this(parent.getCustomerId(),projectId);
    }

    public ProjectPK(String customerId, String projectId) {
        super(customerId);
        this.projectId = projectId;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    @Override
    public String toString() {
        return "ProjectPK [projectId=" + getProjectId() + ", customerId=" + getCustomerId() + "]";
    }

    @Override
    public String getObjectId() {
        return projectId;
    }

    @Override
    public void setObjectId(String id) {
        projectId = id;
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
