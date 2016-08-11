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
package com.squid.kraken.v4.core.expression.reference;

import com.squid.core.domain.IDomain; 
import com.squid.core.domain.operators.ExtendedType;
import com.squid.core.domain.set.SetDomain;
import com.squid.core.expression.ExpressionRef;
import com.squid.core.expression.PrettyPrintConstant;
import com.squid.core.expression.reference.RelationDirection;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.core.sql.render.SQLSkin;
import com.squid.kraken.v4.core.model.domain.ProxyDomainDomain;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Relation;
import com.squid.core.expression.reference.Cardinality;

public class RelationReference extends ExpressionRef {
	
	private Relation relation;
	private RelationDirection direction;
	private IDomain source;
	private IDomain image;
	
	public RelationReference(Universe universe, Relation relation, Domain source, Domain image) throws ScopeException {
		super();
		this.relation = relation;
		if (source.getId().equals(relation.getLeftId()) && image.getId().equals(relation.getRightId())) {
			this.direction = RelationDirection.LEFT_TO_RIGHT;
		} else if (source.getId().equals(relation.getRightId()) && image.getId().equals(relation.getLeftId())) {
			this.direction = RelationDirection.RIGHT_TO_LEFT;
		} else {
			throw new ScopeException("Relation is not compatible with source/image domains");
		}
		this.source = new ProxyDomainDomain(universe, source);
		//
		// compute the image cardinality... if the relation is ?->N, it is a SET(Domain)
		this.image = new ProxyDomainDomain(universe, image);
		Cardinality cardinality = this.direction==RelationDirection.LEFT_TO_RIGHT?this.relation.getRightCardinality():this.relation.getLeftCardinality();
		if (cardinality==Cardinality.MANY) {
			this.image = SetDomain.SET.createMetaDomain(this.image);
		}
	}
	
	public RelationDirection getDirection() {
		return direction;
	}
	
	@Override
	public Object getReference() {
		return relation;
	}
	
	public Relation getRelation() {
		return relation;
	}

	public String getDescription() {
		return this.relation.getDescription();
	}

	@Override
	public ExtendedType computeType(SQLSkin skin) {
		return image.computeType(skin);
	}

	@Override
	public IDomain getImageDomain() {
		return image;
	}

	@Override
	public IDomain getSourceDomain() {
		return source;
	}

	@Override
	public String getReferenceName() {
		switch (this.direction) {
		case LEFT_TO_RIGHT:
			return this.relation.getRightName();
		case RIGHT_TO_LEFT:
			return this.relation.getLeftName();
		default:
			return "???";
		}
	}
	
	@Override
	public String prettyPrint() {
		return PrettyPrintConstant.OPEN_IDENT+
				getReferenceName()+
				PrettyPrintConstant.CLOSE_IDENT;
	}
	
	@Override
	public String getReferenceIdentifier() {
		return relation!=null?
				(PrettyPrintConstant.IDENTIFIER_TAG
				+PrettyPrintConstant.OPEN_IDENT
				+relation.getOid()
				+PrettyPrintConstant.CLOSE_IDENT)
			:null;
	}
	
	@Override
	public String toString() {
	    return getReferenceName();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((direction == null) ? 0 : direction.hashCode());
		result = prime * result
				+ ((relation == null) ? 0 : relation.hashCode());
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
		RelationReference other = (RelationReference) obj;
		if (direction != other.direction)
			return false;
		if (relation == null) {
			if (other.relation != null)
				return false;
		} else if (!relation.equals(other.relation))
			return false;
		return true;
	}

}
