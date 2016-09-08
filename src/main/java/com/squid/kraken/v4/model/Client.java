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

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.mongodb.morphia.annotations.Field;
import org.mongodb.morphia.annotations.Index;
import org.mongodb.morphia.annotations.Indexes;
import org.mongodb.morphia.annotations.Property;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.squid.kraken.v4.model.visitor.ModelVisitor;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import io.swagger.annotations.ApiModelProperty;

/**
 * A "client" application making protected resource requests on behalf of the
 * resource owner and with its authorization.<br>
 * The term "client" does not imply any particular implementation
 * characteristics (e.g., whether the application executes on a server, a
 * desktop, or other devices)
 */
@XmlRootElement
@XmlType(namespace = "http://model.v4.kraken.squid.com")
@SuppressWarnings("serial")
@Indexes({ @Index(fields = { @Field(value = "id.customerId"),
		@Field(value = "id.clientId") }) })
public class Client extends PersistentBaseImpl<ClientPK> implements HasConfig {

	private String name;

	private String secret;
	
	private String JWTKeyPublic;
	
	private String JWTKeyPrivate;

	private List<String> urls;

	@JsonRawValue
	@Property
	private Object config;

	public Client() {
		super(null);
	}
	
	public Client(ClientPK id, String name) {
		this(id, name, null, null);
	}

	public Client(ClientPK id, String name, String secret, List<String> urls) {
		super(id);
		this.name = name;
		this.secret = secret;
		this.urls = urls;
	}

	/**
	 * The client secret is generated and should not be shared with anyone or
	 * embedded in any code that you will distribute (you should use the
	 * client-side flow for these scenarios)
	 */
	public String getSecret() {
		return secret;
	}

	public void setSecret(String secret) {
		this.secret = secret;
	}

	/**
	 * JSON Web Token public key
	 * @return
	 */
	public String getJWTKeyPublic() {
		return JWTKeyPublic;
	}

	public void setJWTKeyPublic(String jWTKeyPublic) {
		JWTKeyPublic = jWTKeyPublic;
	}

	/**
	 * JSON Web Token private key
	 * @return
	 */
	public String getJWTKeyPrivate() {
		return JWTKeyPrivate;
	}

	public void setJWTKeyPrivate(String jWTKeyPrivate) {
		JWTKeyPrivate = jWTKeyPrivate;
	}

	/**
	 * List of authorized urls.<br>
	 * Used for OAuth redirection validation or when making CORS requests..
	 */
	public List<String> getUrls() {
		if (urls == null) {
			urls = new ArrayList<String>();
		}
		return urls;
	}

	public void setUrls(List<String> urls) {
		this.urls = urls;
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
	public Persistent<?> getParentObject(AppContext ctx) {
		return DAOFactory.getDAOFactory().getDAO(Customer.class)
				.readNotNull(ctx, new CustomerPK(ctx.getCustomerId()));
	}

	@ApiModelProperty(position = 1)
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	/**
	 * An optional configuration (json) object.
	 * 
	 * @return a json String or null
	 */
	public String getConfig() {
		if (this.config != null) {
			return this.config.toString();
		} else {
			return null;
		}
	}

	public void setConfig(JsonNode node) {
		String t;
		if (node != null) {
			t = node.toString();
		} else {
			t = null;
		}
		this.config = t;
	}

	@Override
	public String toString() {
		return "Client [name=" + name + ", id=" + id + "]";
	}

}
