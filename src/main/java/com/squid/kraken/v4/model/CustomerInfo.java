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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.wordnik.swagger.annotations.ApiModelProperty;

@XmlType(namespace = "http://model.v4.kraken.squid.com")
@XmlRootElement
@JsonTypeName("Customer")
public class CustomerInfo implements HasAccessRights, HasChildren  {
	
	private static String[] CHILDREN = { "users", "userGroups", "clients",
		"projects", "shortcuts" };

	private String id;

	private String name;

	private String defaultLocale;
	
	private String AWSClientId;
	
    transient private List<User> users;
    
    transient private List<UserGroup> userGroups;
    
    transient private List<Client> clients;
    
    transient private List<Project> projects;
      
    transient private List<Shortcut> shortcuts;
   
    transient private List<State> states;

    private Set<AccessRight> accessRights;
	
    private Role userRole;

	public CustomerInfo() {
	}
	
	public CustomerInfo(Customer customer) {
		super();
		this.name = customer.getName();
		this.defaultLocale = customer.getDefaultLocale();
		this.id = customer.getOid();
		this.accessRights = customer.getAccessRights();
	}

	/**
	 * An informative (non localized) name
	 */
	@ApiModelProperty(position = 1)
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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

	public String getObjectType() {
		return Customer.class.getSimpleName();
	}
	
    public void setObjectType(String type) {
        // just here to allow jaxb marshalling
    }

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	public String getAWSClientId() {
		return AWSClientId;
	}

	public void setAWSClientId(String aWSClientId) {
		AWSClientId = aWSClientId;
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
	
    public List<BookmarkFolder> getBookmarkfolders() {
    	return Collections.<BookmarkFolder> emptyList();
	}

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


    @JsonProperty("_role")
    public Role getUserRole() {
		return userRole;
	}

    @XmlTransient
    @JsonIgnore
	public void setUserRole(Role role) {
		this.userRole = role;
	}
    
	@Override
	public String[] getChildren() {
		return CHILDREN;
	}
}
