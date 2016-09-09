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
import com.squid.core.expression.reference.Cardinality;
import com.squid.core.expression.reference.RelationDirection;
import com.squid.kraken.v4.model.visitor.ModelVisitor;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import io.swagger.annotations.ApiModelProperty;

/**
 * A symmetric relation between two domains
 * 
 * @author sfantino
 * 
 */
@XmlType(namespace = "http://model.v4.kraken.squid.com")
@SuppressWarnings("serial")
@Indexes({ @Index(fields = { @Field(value = "id.customerId"),
		@Field(value = "id.projectId"), @Field(value = "id.relationId") }) })
public class Relation extends DynamicObject<RelationPK> implements Cloneable {

    private DomainPK leftId;

    private Cardinality leftCardinality = Cardinality.ONE;

    private DomainPK rightId;

    private Cardinality rightCardinality = Cardinality.MANY;

    private String leftName = null;

    private String rightName = null;

    private Expression joinExpression;
    
    /**
     * Default constructor (required for jaxb).
     */
    public Relation() {
        super();
    }
    
    public Relation(RelationPK id) {
        super(id);
    }

    public Relation(RelationPK id, DomainPK leftId, Cardinality leftCardinality, DomainPK rightId,
            Cardinality rightCardinality, String leftName, String rightName, Expression joinExpression) {
        super(id);
        this.leftId = leftId;
        this.leftCardinality = leftCardinality;
        this.rightId = rightId;
        this.rightCardinality = rightCardinality;
        this.leftName = leftName;
        this.rightName = rightName;
        this.joinExpression = joinExpression;
    }

    public Relation(RelationPK id, DomainPK leftId, Cardinality leftCardinality, DomainPK rightId,
            Cardinality rightCardinality, String leftName, String rightName, Expression joinExpression, boolean isDynamic) {
        super(id, isDynamic);
        this.leftId = leftId;
        this.leftCardinality = leftCardinality;
        this.rightId = rightId;
        this.rightCardinality = rightCardinality;
        this.leftName = leftName;
        this.rightName = rightName;
        this.joinExpression = joinExpression;
    }
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

    /**
     * the source cardinality
     */
    public Cardinality getLeftCardinality() {
        return leftCardinality;
    }

    public void setLeftCardinality(Cardinality leftCardinality) {
        this.leftCardinality = leftCardinality;
    }

    public Cardinality getRightCardinality() {
        return rightCardinality;
    }

    public void setRightCardinality(Cardinality rightCardinality) {
        this.rightCardinality = rightCardinality;
    }

    /**
     * the alternate name for using the relation from target to source
     */
    public String getRightName() {
        return rightName;
    }

    public void setRightName(String rightName) {
        this.rightName = rightName;
    }

    /**
     * The source domain; the join expression can be defined on it and on the target domain
     */
    public DomainPK getLeftId() {
        return leftId;
    }

    public void setLeftId(DomainPK leftId) {
        this.leftId = leftId;
    }
    
    @XmlTransient
    @JsonIgnore
    public Domain getLeftDomain(AppContext ctx) {
        return DAOFactory.getDAOFactory().getDAO(Domain.class).readNotNull(ctx, leftId);
    }
   
    /**
     * The target domain; the join expression can be defined on it and on the source domain
     */
    public DomainPK getRightId() {
        return rightId;
    }

    public void setRightId(DomainPK rightId) {
        this.rightId = rightId;
    }
    
    @XmlTransient
    @JsonIgnore
    public Domain getRightDomain(AppContext ctx) {
        return DAOFactory.getDAOFactory().getDAO(Domain.class).readNotNull(ctx, rightId);
    }

    @XmlTransient
    @JsonIgnore
    /**
     * return the relation direction when applied to that source domain; return NO_WAY if not applicable
     * @param sourceId
     * @return the Direction or NO_WAY of not applicable
     * @throws ScopeException
     */
    public RelationDirection getDirection(DomainPK sourceId) {
		if (sourceId.equals(this.leftId)) {
			return RelationDirection.LEFT_TO_RIGHT;
		} else if (sourceId.equals(this.rightId)) {
			return RelationDirection.RIGHT_TO_LEFT;
		} else {
			return RelationDirection.NO_WAY;
		}
	}

	@XmlTransient
	@JsonIgnore
	@ApiModelProperty
	public String getName() {
		return leftName + "/" + rightName;
	}

	/**
	 * The relation name from source to target
	 */
	public String getLeftName() {
		return leftName;
	}

	public void setLeftName(String leftName) {
		this.leftName = leftName;
	}

	public Expression getJoinExpression() {
		return joinExpression;
	}

	public void setJoinExpression(Expression joinExpression) {
		this.joinExpression = joinExpression;
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
		return "Relation [joinExpression=" + joinExpression + ", name="
				+ leftName + ", oppositeName=" + rightName
				+ ", sourceCardinality=" + leftCardinality + ", sourceId="
				+ leftId + ", targetCardinality=" + rightCardinality
				+ ", targetId=" + rightId + "]";
	}

}
