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
import com.squid.kraken.v4.model.visitor.ModelVisitor;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.wordnik.swagger.annotations.ApiModelProperty;

/**
 * Domain is a structure defining available Dimensions and Metrics which can be
 * used to define an Analysis.<br>
 * A Domain scope is defined by a Subject (a V3 expression).<br>
 * Domains can be arranged in a tree structure via DomainRelation.
 */
@XmlType(namespace = "http://model.v4.kraken.squid.com")
@SuppressWarnings("serial")
@Indexes({ @Index(fields = { @Field(value = "id.customerId"),
		@Field(value = "id.projectId"), @Field(value = "id.domainId") }) })
public class Domain extends DynamicObject<DomainPK> implements Cloneable, HasChildren {
	
	public static final int VERSION_1 = 1;// introducing a new dynamic mode, everything selected by default

	private static String[] CHILDREN = { "metrics", "dimensions" };
	
    private Expression subject;

	// the version is now visible, but cannot be modified (the store method won't allow it)
	// - we need to export the domain version if we want to re-create the domain in a new instance, so the behavior is compliant
	// - if we hide the internalVerion, we won't be able to move an old project/domain
	private Integer internalVersion = null;
    
    @Transient
    transient private List<Metric> metrics;

    @Transient
    transient private List<Dimension> dimensions;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private DomainOption options = new DomainOption();

    /**
     * Default constructor (required for jaxb).
     */
    public Domain() {
        super();
    }

    /**
     * this is the constructor used for dynamic domains
     * @param domainId
     * @param name
     * @param subject
     */
    public Domain(DomainPK domainId, String name, Expression subject) {
        super(domainId, name);
        this.subject = subject;
    }
	
	public Domain(DomainPK domainId) {
		this(domainId, null, null);
	}

    public Domain(DomainPK domainId, String name, Expression subject,
			boolean isDynamic) {
		super(domainId, name, isDynamic);
		this.internalVersion = VERSION_1;
		this.subject = subject;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

    /**
     * An Expression relative to the parent Project, and used to define the Domain mapping.
     */
    @ApiModelProperty(position = 1)
    public Expression getSubject() {
        return subject;
    }

    public void setSubject(Expression subject) {
        this.subject = subject;
    }

	/**
     * Visitor.
     */
    @XmlTransient
    @JsonIgnore
    public void accept(ModelVisitor visitor) {
    	visitor.visit(this);
        for (Dimension o : getDimensions()) {
        	o.accept(visitor);
        }
        for (Metric o : getMetrics()) {
            o.accept(visitor);
        }
    }

    @Override
    public Persistent<?> getParentObject(AppContext ctx) {
        return DAOFactory.getDAOFactory().getDAO(Project.class).readNotNull(ctx,
                new ProjectPK(id.getCustomerId(), id.getProjectId()));
    }

    @Override
    public String toString() {
        return "Domain '"+getName()+"' [subject=" + subject + ", getId()=" + getId() + "]";
    }

    
    public List<Metric> getMetrics() {
        return (metrics == null) ? Collections.<Metric> emptyList() : metrics;
    }

    public void setMetrics(List<Metric> metrics) {
        this.metrics = metrics;
    }

    
    public List<Dimension> getDimensions() {
        return (dimensions == null) ? Collections.<Dimension> emptyList() : dimensions;
    }

    public void setDimensions(List<Dimension> dimensions) {
        this.dimensions = dimensions;
    }


    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public DomainOption getOptions() {
        return options;
    }

    public void setOptions(DomainOption options) {
        this.options = options;
    }
    
    /**
	 * @return the internalVersion
	 */
	public Integer getInternalVersion() {
		return internalVersion;
	}
	
	/**
	 * use the copy internalVersion
	 * @param copy
	 */
	public void copyInternalVersion(Domain copy) {
		this.internalVersion = copy.internalVersion;
	}
    
	@Override
	public String[] getChildren() {
		return CHILDREN;
	}

}
