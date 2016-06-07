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

/**
 * Bookmark holds a project configuration
 */
@XmlType(namespace = "http://model.v4.kraken.squid.com")
@SuppressWarnings("serial")
@Indexes({ @Index(fields = { @Field(value = "id.customerId"),
		@Field(value = "id.projectId"), @Field(value = "id.bookmarkId") }),
		@Index(fields = { @Field(value = "id.customerId"),
				@Field(value = "id.projectId"), @Field(value = "path") })})
public class Bookmark extends DynamicObject<BookmarkPK> implements HasConfig {
	
	static final public String SEPARATOR = "/";
	
	static public enum Folder {
		SHARED, USER
	};

	private String name;
	private String description;
	private String path;

	@JsonRawValue
	@Property
	private Object config;

	/**
	 * Default constructor (required for jaxb).
	 */
	public Bookmark() {
		super();
	}

	public Bookmark(BookmarkPK id) {
		super(id);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
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

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
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
		return DAOFactory
				.getDAOFactory()
				.getDAO(Project.class)
				.readNotNull(ctx,
						new ProjectPK(id.getCustomerId(), id.getProjectId()));
	}

	@Override
	public String toString() {
		return "Domain getId()=" + getId() + "]";
	}

}
