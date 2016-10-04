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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.mongodb.morphia.annotations.Field;
import org.mongodb.morphia.annotations.Index;
import org.mongodb.morphia.annotations.IndexOptions;
import org.mongodb.morphia.annotations.Indexes;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.squid.kraken.v4.model.visitor.ModelVisitor;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;

@XmlRootElement
@XmlType(namespace = "http://model.v4.kraken.squid.com")
@SuppressWarnings("serial")
@Indexes({
		@Index(fields = { 
			@Field(value = "login") }),
		@Index(fields = { @Field(value = "id.customerId"),
				@Field(value = "login") }),
		@Index(fields = { @Field(value = "id.customerId"),
				@Field(value = "email") }, options = @IndexOptions(unique = true)),
		@Index(fields = { @Field(value = "id.authId"),
				@Field(value = "authId") }, options = @IndexOptions(unique = true))})
public class User extends PersistentBaseImpl<UserPK> {

	private String login;

	private String email;

	private String password;

	private List<String> groups;
	
	private String authId;

	@XmlTransient
	@JsonIgnore
	private boolean isSuperUser = false;

	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	private Map<String, String> attributes = null;

	public User() {
		super(null);
	}

	public User(UserPK id, String login, String password, String email) {
		super(id);
		this.login = login;
		this.password = password;
		this.email = email;
	}

	public User(UserPK id, String login, String password) {
		this(id, login, password, null);
	}
	
	public User(UserPK id, String login) {
		this(id, login, null, null);
	}

	public String getLogin() {
		return login;
	}

	public void setLogin(String login) {
		this.login = login;
	}

	@XmlElement
	@JsonIgnore
	public String getPassword() {
		return password;
	}

	@XmlTransient
	@JsonProperty
	public void setPassword(String password) {
		this.password = password;
	}

	public List<String> getGroups() {
		if (groups == null) {
			groups = new ArrayList<String>();
		}
		return groups;
	}

	public void setGroups(List<String> groups) {
		this.groups = groups;
	}

	@XmlTransient
	@JsonIgnore
	public boolean isSuperUser() {
		return isSuperUser;
	}

	public void setSuperUser(boolean isSuperUser) {
		this.isSuperUser = isSuperUser;
	}

	/**
	 * Visitor.
	 */
	@XmlTransient
	@JsonIgnore
	public void accept(ModelVisitor visitor) {
		visitor.visit(this);
	}

	@Override
	public String toString() {
		return "User [id=" + id.getUserId() + ", login=" + login
				+ (isSuperUser ? " (SuperUser)" : "") + "]";
	}

	@Override
	public Persistent<?> getParentObject(AppContext ctx) {
		return DAOFactory.getDAOFactory().getDAO(Customer.class)
				.readNotNull(ctx, new CustomerPK(ctx.getCustomerId()));
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getAuthId() {
		return authId;
	}

	public void setAuthId(String authId) {
		this.authId = authId;
	}

	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	public Map<String, String> getAttributes() {
		return attributes;
	}

	public void setAttributes(Map<String, String> attributes) {
		this.attributes = attributes;
	}
}
