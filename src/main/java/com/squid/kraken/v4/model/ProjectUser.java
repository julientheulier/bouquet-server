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
import javax.xml.bind.annotation.XmlType;

import org.mongodb.morphia.annotations.Field;
import org.mongodb.morphia.annotations.Index;
import org.mongodb.morphia.annotations.Indexes;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.squid.kraken.v4.model.visitor.ModelVisitor;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;

@XmlRootElement
@XmlType(namespace = "http://model.v4.kraken.squid.com")
@SuppressWarnings("serial")
@Indexes({ @Index(fields = { @Field(value = "id.customerId"),
		@Field(value = "id.projectId"), @Field(value = "id.userId") }) })
public class ProjectUser extends PersistentBaseImpl<ProjectUserPK> {

	/**
	 * Last date when the annotation is read. It is just the milliseconds.
	 */
	private Long lastAnnotationReadTimestamp;

	/**
	 * Default constructor (required for jaxb).
	 */
	public ProjectUser() {
		super(null);
	}

	public ProjectUser(ProjectUserPK projectUserId) {
		super(projectUserId);
	}

	/**
	 * Get the "lastAnnotationReadTimestamp" variable.
	 * 
	 * @return the lastAnnotationReadTimestamp
	 */
	public Long getLastAnnotationReadTimestamp() {
		return lastAnnotationReadTimestamp;
	}

	/**
	 * Set the "lastAnnotationReadTimestamp" variable.
	 * 
	 * @param lastAnnotationReadTimestamp
	 *            the lastAnnotationReadTimestamp to set
	 */
	public void setLastAnnotationReadTimestamp(Long lastAnnotationReadTimestamp) {
		this.lastAnnotationReadTimestamp = lastAnnotationReadTimestamp;
	}

	@XmlTransient
	@JsonIgnore
	public void accept(ModelVisitor visitor) {
		visitor.visit(this);
	}

	@Override
	public ProjectUserPK getId() {
		return super.getId();
	}

	@Override
	public Persistent<?> getParentObject(AppContext ctx) {
		return DAOFactory
				.getDAOFactory()
				.getDAO(Project.class)
				.readNotNull(
						ctx,
						new ProjectPK(id.getCustomerId(), getId()
								.getProjectId()));
	}

}
