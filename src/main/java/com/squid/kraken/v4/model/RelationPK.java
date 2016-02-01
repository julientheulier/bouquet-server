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

@SuppressWarnings("serial")
@XmlRootElement
public class RelationPK extends ProjectPK {


    private String relationId;

    public RelationPK() {
    }
    
    public RelationPK(ProjectPK projectId) {
        this(projectId.getCustomerId(), projectId.getProjectId(), null);
    }
    
    public RelationPK(ProjectPK projectId, String relationId) {
        this(projectId.getCustomerId(), projectId.getProjectId(), relationId);
    }
    
    public RelationPK(String customerId, String projectId) {
        this(customerId, projectId, null);
    }

    public RelationPK(String customerId, String projectId, String relationId) {
        super(customerId, projectId);
        setRelationId(relationId);
    }

    public String getRelationId() {
        return relationId;
    }

    public void setRelationId(String relationId) {
        this.relationId = relationId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((relationId == null) ? 0 : relationId.hashCode());
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
        RelationPK other = (RelationPK) obj;
        if (relationId == null) {
            if (other.relationId != null)
                return false;
        } else if (!relationId.equals(other.relationId))
            return false;
        return true;
    }

    @XmlTransient
    @JsonIgnore
    @Override
    public String getObjectId() {
        return relationId;
    }
    
    @Override
    public void setObjectId(String id) {
    	relationId = id;
    }

	@Override
	public String toString() {
		return "RelationPK [relationId=" + relationId + ", getProjectId()="
				+ getProjectId() + ", getCustomerId()=" + getCustomerId() + "]";
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
