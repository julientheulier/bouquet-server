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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.mongodb.morphia.annotations.Field;
import org.mongodb.morphia.annotations.Index;
import org.mongodb.morphia.annotations.Indexes;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.model.visitor.ModelVisitor;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.ProjectUserDAO;

@XmlRootElement
@XmlType(namespace = "http://model.v4.kraken.squid.com")
@SuppressWarnings("serial")
@Indexes({ @Index(fields = { @Field(value = "id.customerId"),
		@Field(value = "id.projectId"), @Field(value = "id.annotationId") }) })
public class Annotation extends PersistentBaseImpl<AnnotationPK> {

	/**
	 * Key of the User object which creates the annotation.
	 */
	@JsonIgnore
	@XmlTransient
	private UserPK authorId;

	/**
	 * Date used to check if a used has unread annotations. It is just the
	 * milliseconds.
	 */
	@JsonIgnore
	@XmlTransient
	private Long creationTimestamp;

	/**
	 * Date used to order annotations display. It's up to the application to
	 * choose display type. It is just the milliseconds. Note: if this field is
	 * NULL or empty, it is automatically filled in with creationTimestamp.
	 */
	private Long annotationTimestamp;

	/**
	 * Annotation message. Size should not exceed 2kb.
	 */
	private String message;

	/**
	 * Default constructor (required for jaxb).
	 */
	public Annotation() {
		super(null);
	}

	public Annotation(AnnotationPK annotationId) {
		super(annotationId);
	}

	/**
	 * Get the "authorId" variable.
	 * 
	 * @return the authorId
	 */
	@JsonProperty
	@XmlElement
	public UserPK getAuthorId() {
		return authorId;
	}

	/**
	 * Set the "authorId" variable.
	 * 
	 * @param authorId
	 *            the authorId to set
	 */
	@JsonIgnore
	@XmlTransient
	public void setAuthorId(UserPK authorId) {
		this.authorId = authorId;
	}

	/**
	 * Get the "creationTimestamp" variable.
	 * 
	 * @return the creationTimestamp
	 */
	@JsonProperty
	@XmlElement
	public Long getCreationTimestamp() {
		return creationTimestamp;
	}

	/**
	 * Set the "creationTimestamp" variable.
	 * 
	 * @param creationTimestamp
	 *            the creationTimestamp to set
	 */
	@JsonIgnore
	@XmlTransient
	public void setCreationTimestamp(Long creationTimestamp) {
		this.creationTimestamp = creationTimestamp;
	}

	/**
	 * Get the "annotationTimestamp" variable.
	 * 
	 * @return the annotationTimestamp
	 */
	public Long getAnnotationTimestamp() {
		return annotationTimestamp;
	}

	/**
	 * Set the "annotationTimestamp" variable.
	 * 
	 * @param annotationTimestamp
	 *            the annotationTimestamp to set
	 */
	public void setAnnotationTimestamp(Long annotationTimestamp) {
		this.annotationTimestamp = annotationTimestamp;
	}

	/**
	 * Get the "message" variable.
	 * 
	 * @return the message
	 */
	public String getMessage() {
		return message;
	}

	/**
	 * Set the "message" variable.
	 * 
	 * @param message
	 *            the message to set
	 */
	public void setMessage(String message) {
		this.message = message;
	}

	@JsonIgnore
	public User getAuthor() {
		return DAOFactory
				.getDAOFactory()
				.getDAO(User.class)
				.readNotNull(
						ServiceUtils.getInstance().getRootUserContext(
								authorId.getCustomerId()), authorId);
	}

	public String getAuthorLogin() {
		return getAuthor().getLogin();
	}

	@XmlTransient
	@JsonIgnore
	public List<ProjectUser> getProjectUsers(AppContext ctx) {
		List<ProjectUser> result = new ArrayList<ProjectUser>();
		try {
			ProjectUserPK projectUserPk = new ProjectUserPK(
					ctx.getCustomerId(), id.getProjectId(), ctx.getUser()
							.getId().getUserId());
			Optional<ProjectUser> optionalProjectUser = ((ProjectUserDAO) DAOFactory
					.getDAOFactory().getDAO(ProjectUser.class)).read(ctx,
					projectUserPk);
			if (optionalProjectUser.isPresent()) {
				result.add(optionalProjectUser.get());
			}
		} catch (ObjectNotFoundAPIException e) {
			// Object not found
		}
		return result;
	}

	@XmlTransient
	@JsonIgnore
	public void accept(ModelVisitor visitor) {
		visitor.visit(this);
		for (ProjectUser o : getProjectUsers(visitor.getContext())) {
			o.accept(visitor);
		}
	}

	@Override
	public AnnotationPK getId() {
		return super.getId();
	}

	@Override
	public Persistent<?> getParentObject(AppContext ctx) {
		return DAOFactory
				.getDAOFactory()
				.getDAO(Project.class)
				.readNotNull(ctx,
						new ProjectPK(id.getCustomerId(), id.getProjectId()));
	}

}
