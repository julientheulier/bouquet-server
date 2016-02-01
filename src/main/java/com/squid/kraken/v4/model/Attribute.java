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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.squid.kraken.v4.model.visitor.ModelVisitor;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;

/**
 * An Attribute may be used within a Dimension scope to provide additional
 * information
 */
@XmlType(namespace = "http://model.v4.kraken.squid.com")
@SuppressWarnings("serial")
@Indexes({ @Index(fields = { @Field(value = "id.customerId"),
		@Field(value = "id.projectId"), @Field(value = "id.domainId"),
		@Field(value = "id.dimensionId"), @Field(value = "id.attributeId") }) })
public class Attribute extends LzPersistentBaseImpl<AttributePK> {

	private Expression expression;

	/**
	 * Default constructor (required for jaxb).
	 */
	public Attribute() {
		super(null);
	}

	public Attribute(AttributePK attributeId, String name, Expression expression) {
		super(attributeId, name);
		this.expression = expression;
	}

	public Expression getExpression() {
		return expression;
	}

	public void setExpression(Expression expression) {
		this.expression = expression;
	}

	@XmlTransient
	@JsonIgnore
	public void accept(ModelVisitor visitor) {
		visitor.visit(this);
	}

	@Override
	public Persistent<?> getParentObject(AppContext ctx) {
		return DAOFactory
				.getDAOFactory()
				.getDAO(Dimension.class)
				.readNotNull(
						ctx,
						new DimensionPK(id.getCustomerId(), id.getProjectId(),
								id.getDomainId(), id.getDimensionId()));
	}

}
