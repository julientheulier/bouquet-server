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

import java.util.HashSet;
import java.util.Set;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.osgi.DeepReadView;
import com.wordnik.swagger.annotations.ApiModelProperty;

@SuppressWarnings("serial")
@XmlType(namespace = "http://model.v4.kraken.squid.com")
@XmlSeeAlso( {Customer.class, Attribute.class, Dimension.class, Domain.class, Metric.class,
        Project.class})
@Entity()
public abstract class PersistentBaseImpl<PK extends GenericPK> implements Persistent<PK>, Cloneable {

    /**
     * An internal object id.<br>
     * This is mostly used as a workaround to noSQL DBs such as MongoDB not handling well composite PKs.
     */
    @Id
    private String internalObjectId;

    @Embedded
    protected PK id;
    
    @JsonView(DeepReadView.class)
    private Set<AccessRight> accessRights;
    
    private Role userRole;

	public PersistentBaseImpl(PK id) {
        setId(id);
    }
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

    public PK getId() {
        return id;
    }

    public void setId(PK id) {
        this.id = id;
        // keep the objectId in sync
        if (id != null) {
            this.internalObjectId = id.toUUID();
        }
    }
    
    @XmlTransient
    @JsonProperty
    public String getOid() {
    	if (id != null) {
    		return id.getObjectId();
    	} else {
    		return null;
    	}
    }
    
    @XmlTransient
    @JsonIgnore
    public void setOid(String oid) {
    	// "oid" field in JSON objects shoud be read-only
    }

    @XmlTransient
    @JsonIgnore
    public String getCustomerId() {
        if (id instanceof CustomerPK) {
            return ((CustomerPK) id).getCustomerId();
        } else {
            return null;
        }
    }
    
    @XmlTransient
    @JsonIgnore
    public void setCustomerId(String customerId) {
        if (id instanceof CustomerPK) {
            ((CustomerPK) id).setCustomerId(customerId);
            // keep the objectId in sync
            this.internalObjectId = id.toUUID();
        }
    }

    @XmlElement
    @JsonProperty
    public String getObjectType() {
        return this.getClass().getSimpleName();
    }

    @XmlTransient
    @JsonIgnore
    public void setObjectType(String type) {
        // just here to allow jaxb marshalling
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
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
        PersistentBaseImpl<?> other = (PersistentBaseImpl<?>) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }

    @JsonView(DeepReadView.class)
    @Override
    public Set<AccessRight> getAccessRights() {
        if (accessRights == null) {
            accessRights = new HashSet<AccessRight>();
        }
        return accessRights;
    }

    @Override
    public void setAccessRights(Set<AccessRight> accessRights) {
        this.accessRights = accessRights;
    }

    @ApiModelProperty(value = "The role of the current User (passed in the context) over this object")
    @JsonProperty("_role")
    public Role getUserRole() {
		return userRole;
	}

    @XmlTransient
    @JsonIgnore
	public void setUserRole(Role role) {
		this.userRole = role;
	}
}
