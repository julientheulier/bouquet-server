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
import org.mongodb.morphia.annotations.IndexOptions;
import org.mongodb.morphia.annotations.Indexes;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.squid.kraken.v4.model.visitor.ModelVisitor;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import io.swagger.annotations.ApiModelProperty;

@XmlType(namespace = "http://model.v4.kraken.squid.com")
@SuppressWarnings("serial")
@Indexes({ @Index(options = @IndexOptions(name = "metric_index"), fields = { @Field(value = "id.customerId"),
		@Field(value = "id.projectId"), @Field(value = "id.domainId"),
		@Field(value = "id.metricId") }) })
public class Metric extends ExpressionObject<MetricPK> implements Cloneable {

    private Expression expression;

    /**
     * Default constructor (required for jaxb).
     */
    public Metric() {
        super();
    }
    
    public Metric(MetricPK metricId) {
        this(metricId, null, null);
    }
    
    public Metric(MetricPK metricId, String name, Expression expression) {
    	this(metricId, name, expression, false);
    }
    
    public Metric(MetricPK metricId, String name, Expression expression, boolean isDynamic) {
        super(metricId, name, isDynamic);
        this.expression = expression;
    }

	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	/**
	 * An expression relative to the Domain, used to define the Analysis.
	 */
	@Override
	@ApiModelProperty
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
	}

	@Override
	public Persistent<?> getParentObject(AppContext ctx) {
		return DAOFactory.getDAOFactory().getDAO(Domain.class)
				.readNotNull(ctx, id.getParent());
	}

	@Override
	@ApiModelProperty
	public String toString() {
		return "Metric [expression=" + expression + ", id=" + id
				+ ", getName()=" + getName() + "]";
	}

	@XmlTransient
	@JsonIgnore
	public void copy(AppContext ctx, GenericPK parentPK) {
		getId().setParent(parentPK);
		DAOFactory.getDAOFactory().getDAO(Metric.class).create(ctx, this);
	}

}
