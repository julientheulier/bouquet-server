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

import java.util.Collections;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.squid.kraken.v4.model.visitor.ModelVisitor;
import com.squid.kraken.v4.persistence.AppContext;
import com.wordnik.swagger.annotations.ApiModelProperty;

/**
 * Top-level object of the graph.<br>
 * Container for projects.
 */
@XmlType(namespace = "http://model.v4.kraken.squid.com")
@XmlRootElement
@SuppressWarnings("serial")
public class Customer extends PersistentBaseImpl<CustomerPK> {

    private String name;

    private String defaultLocale;

    private String MD5Salt;
    
    private String AWSClientId;

	
    @Transient
    transient private List<User> users;

    
    @Transient
    transient private List<UserGroup> userGroups;

    
    @Transient
    transient private List<Client> clients;

    
    @Transient
    transient private List<Project> projects;
    
    
    @Transient
    transient private List<Shortcut> shortcuts;
    
    
    @Transient
    transient private List<State> states;

    /**
     * Default constructor (required for jaxb).
     */
    public Customer() {
        super(null);
    }

    public Customer(String name) {
        super(new CustomerPK(new ObjectId().toString()));
        this.name = name;
    }

    /**
     * An informative (non localized) name
     */
    @XmlElement
    @ApiModelProperty(position = 1)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Visitor.
     */
    @XmlTransient
    @JsonIgnore
    public void accept(ModelVisitor visitor) {
    	visitor.visit(this);
        for (UserGroup o : getUserGroups()) {
            o.accept(visitor);
        }
        for (User o : getUsers()) {
            o.accept(visitor);
        }
        for (Client o : getClients()) {
            o.accept(visitor);
        }
        for (Shortcut o : getShortcuts()) {
            o.accept(visitor);
        }
        for (State o : getStates()) {
            o.accept(visitor);
        }
        for (Project o : getProjects()) {
            o.accept(visitor);
        }
    }

    @Override
    public Persistent<?> getParentObject(AppContext ctx) {
        // Customer has no parent object
        return null;
    }

    /**
     * The default locale used by this Customer.<br>
     * Used if no locale is specified in the requests.
     */
    public String getDefaultLocale() {
        return defaultLocale;
    }

    public void setDefaultLocale(String defaultLocale) {
        this.defaultLocale = defaultLocale;
    }

    /**
     * A MD5 Salt String used when hashing passwords for this customers Users.
     */
    @XmlTransient
    @JsonIgnore
    public String getMD5Salt() {
        return MD5Salt;
    }

    public void setMD5Salt(String mD5Salt) {
        MD5Salt = mD5Salt;
    }

    
    public List<User> getUsers() {
        return (users == null) ? Collections.<User> emptyList() : users;
    }

    public void setUsers(List<User> users) {
        this.users = users;
    }

    
    public List<UserGroup> getUserGroups() {
        return (userGroups == null) ? Collections.<UserGroup> emptyList() : userGroups;
    }

    public void setUserGroups(List<UserGroup> userGroups) {
        this.userGroups = userGroups;
    }

    
    public List<Client> getClients() {
        return (clients == null) ? Collections.<Client> emptyList() : clients;
    }

    public void setClients(List<Client> clients) {
        this.clients = clients;
    }

    
    public List<Project> getProjects() {
        return (projects == null) ? Collections.<Project> emptyList() : projects;
    }
    
	public void setProjects(List<Project> projects) {
        this.projects = projects;
    }

	
    public List<Shortcut> getShortcuts() {
    	return (shortcuts == null) ? Collections.<Shortcut> emptyList() : shortcuts;
	}

	public void setShortcuts(List<Shortcut> shortcuts) {
		this.shortcuts = shortcuts;
	}
    
	
    public List<State> getStates() {
		return (states == null) ? Collections.<State> emptyList() : states;
	}

	public void setStates(List<State> states) {
		this.states = states;
	}

	public String getAWSClientId() {
		return AWSClientId;
	}

	public void setAWSClientId(String aWSClientId) {
		AWSClientId = aWSClientId;
	}

}
