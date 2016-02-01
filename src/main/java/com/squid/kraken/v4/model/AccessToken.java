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

import java.util.Set;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Field;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Index;
import org.mongodb.morphia.annotations.Indexes;
import org.mongodb.morphia.annotations.Property;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.visitor.ModelVisitor;
import com.squid.kraken.v4.persistence.AppContext;
import com.wordnik.swagger.annotations.ApiModel;

/**
 * An authentication token.<br>
 * Tokens are linked to a {@link Customer} a {@link User} and a {@link Client}.<br>
 * Token types :
 * <ul>
 * <li>NORMAL : a token used for API calls authentication.</li>
 * <li>RESET_PWD : which allows the user to change its password without having
 * to know its old one.</li>
 * <li>CODE : is a OAuth authentication code.</li>
 * <li>REFRESH : is a OAuth Refresh-Token.</li>
 * </ul>
 */
// Note that it implements Persistent but does not extend PersistentBaseImpl
// since its PK does not contain a customer id
// (because it can be searched regardless of the Customer).
@XmlRootElement
@Entity(value = "token", noClassnameStored = true)
@XmlType(namespace = "http://model.v4.kraken.squid.com")
@SuppressWarnings("serial")
@Indexes({
		@Index(fields = { @Field(value = "type") }),
		@Index(fields = { @Field(value = "customerId"),
				@Field(value = "userId") }),
		@Index(fields = { @Field(value = "customerId"),
				@Field(value = "clientId"), @Field(value = "userId"),
				@Field(value = "type") }) })
@ApiModel
public class AccessToken implements Persistent<AccessTokenPK> {

	static public enum Type {
		NORMAL, RESET_PWD, CODE, REFRESH
	};

	private Type type;

	@Property("exp")
	private Long expirationDateMillis;

	@Id
	private String internalObjectId;

	@Embedded
	private AccessTokenPK id;

	protected String customerId;

	protected String userId;

	protected String clientId;

	protected String refreshToken;

	/**
	 * Default constructor (required for jaxb).
	 */
	public AccessToken() {
	}

	public AccessToken(AccessTokenPK tokenId, String customerId,
			String clientId, Long expirationDateMillis) {
		super();
		setId(tokenId);
		this.customerId = customerId;
		this.clientId = clientId;
		this.expirationDateMillis = expirationDateMillis;
	}

	public AccessTokenPK getId() {
		return id;
	}

	public void setId(AccessTokenPK id) {
		this.id = id;
		internalObjectId = id.toUUID();
	}

	@XmlTransient
	public String getOid() {
		if (id != null) {
			return id.getObjectId();
		} else {
			return null;
		}
	}

	@XmlTransient
	public void setOid(String oid) {
		// keep the objectId in sync
		if (id != null) {
			id.setObjectId(oid);
			this.internalObjectId = id.toUUID();
		}
	}

	@JsonProperty
	public String getCustomerId() {
		return customerId;
	}

	@JsonIgnore
	public void setCustomerId(String customerId) {
		// not allowed
	}

	/**
	 * The {@link User} associated with this token.
	 * 
	 * @return the user's id
	 */
	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	/**
	 * The {@link Client} associated with this token.
	 * 
	 * @return the client's id
	 */
	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getRefreshToken() {
		return refreshToken;
	}

	public void setRefreshToken(String refreshToken) {
		this.refreshToken = refreshToken;
	}

	@XmlElement
	public String getObjectType() {
		return this.getClass().getSimpleName();
	}

	// token members

	/**
	 * The date the token will expire.
	 * 
	 * @return Unix Timestamp (milliseconds since the Epoch).
	 */
	public Long getExpirationDateMillis() {
		return expirationDateMillis;
	}

	public void setExpirationDateMillis(Long expirationDateMillis) {
		this.expirationDateMillis = expirationDateMillis;
	}

	public Type getType() {
		if (type != null) {
			return type;
		} else {
			return Type.NORMAL;
		}
	}

	public void setType(Type type) {
		this.type = type;
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
		AccessToken other = (AccessToken) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	/**
	 * Visitor.
	 */
	@XmlTransient
	@JsonIgnore
	public void accept(ModelVisitor visitor) {
		visitor.visit(this);
	}

	@XmlTransient
	@JsonIgnore
	@Override
	public Set<AccessRight> getAccessRights() {
		// not applicable
		return null;
	}

	@XmlTransient
	@JsonIgnore
	@Override
	public void setAccessRights(Set<AccessRight> accessRights) {
		// not applicable
	}

	@XmlTransient
	@JsonIgnore
	@Override
	public Role getUserRole() {
		// not applicable
		return null;
	}

	@XmlTransient
	@JsonIgnore
	@Override
	public void setUserRole(Role role) {
		// not applicable
	}

	@Override
	public Persistent<?> getParentObject(AppContext ctx) {
		// not applicable
		return null;
	}

	@Override
	public String toString() {
		return "AccessToken [clientId=" + clientId + ", customerId="
				+ customerId + ", expirationDateMillis=" + expirationDateMillis
				+ ", userId=" + userId + ", type" + type + "]";
	}

}
