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

import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.mongodb.morphia.annotations.Field;
import org.mongodb.morphia.annotations.Index;
import org.mongodb.morphia.annotations.Indexes;
import org.mongodb.morphia.annotations.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.squid.core.domain.IDomain;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.model.visitor.ModelVisitor;
import com.squid.kraken.v4.persistence.AppContext;
import com.wordnik.swagger.annotations.ApiModelProperty;

@XmlType(namespace = "http://model.v4.kraken.squid.com")
@SuppressWarnings("serial")
@Indexes({
		@Index(fields = { @Field(value = "id.customerId"),
				@Field(value = "id.projectId"), @Field(value = "id.domainId"),
				@Field(value = "id.dimensionId")}),
		@Index(fields = { @Field(value = "id.customerId"),
				@Field(value = "id.projectId"), @Field(value = "id.projectId") }) })
public class Dimension extends ExpressionObject<DimensionPK> implements Cloneable, HasChildren {

	private static String[] CHILDREN = { "attributes" };
	
	// this is a list of conditional dimension - this is an internal type and
	// should not be used by the meta-model
	static public enum Type {
		CATEGORICAL, CONTINUOUS, INDEX, SEGMENTS
	};

	private Type type;

	private Expression expression;
	
	@JsonInclude(Include.ALWAYS)
	private DimensionPK parentId;

	
	@Transient
	transient private List<Attribute> attributes;

	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	private List<DimensionOption> options = null;

    /**
     * Default constructor (required for jaxb).
     */
    public Dimension() {
        super();
    }
    
    public Dimension(DimensionPK dimensionId) {
        this(dimensionId, null, null, null);
    }

    public Dimension(DimensionPK dimensionId, String name, Type type, Expression expression) {
        this(dimensionId, name, type, expression, false);
    }
    
    public Dimension(DimensionPK dimensionId, String name, Type type, Expression expression, boolean isDynamic) {
        super(dimensionId, name, isDynamic);
        this.expression = expression;
        this.type = type;
    }
    
	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	@ApiModelProperty(position = 2)
	public DimensionPK getParentId() {
		return parentId;
	}
	
	public void setParentId(DimensionPK parentId) {
		this.parentId = parentId;
	}
	
	@ApiModelProperty(position = 1)
	public Type getType() {
		return type;
	}
	
	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * An expression relative to the parent Domain, used to define the
	 * Dimension.
	 */
	@Override
	@ApiModelProperty(position = 3)
	public Expression getExpression() {
		return expression;
	}

	public void setExpression(Expression expression) {
		this.expression = expression;
	}

	/**
	 * Visitor.
	 */
	@XmlTransient
	@JsonIgnore
	public void accept(ModelVisitor visitor) {
		visitor.visit(this);
		for (Attribute o : getAttributes()) {
			o.accept(visitor);
		}
	}

	@Override
	public Domain getParentObject(AppContext ctx) {
		try {
			return ProjectManager.INSTANCE.getDomain(ctx, id.getParent());
		} catch (ScopeException e) {
			throw new ObjectNotFoundAPIException(e.getMessage(), e, false);
		}
	}

	
	public List<Attribute> getAttributes() {
		return (attributes == null) ? Collections.<Attribute> emptyList()
				: attributes;
	}

	public void setAttributes(List<Attribute> attributes) {
		this.attributes = attributes;
	}

	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	public List<DimensionOption> getOptions() {
		return options;
	}

	public void setOptions(List<DimensionOption> options) {
		this.options = options;
	}
	
	public boolean isVisible() {
		return !isDynamic();
	}
	
	@Override
	public String toString() {
		return "Dimension '"+getName()+"'";
	}
	
	@Override
	public String[] getChildren() {
		return CHILDREN;
	}

}
