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
package com.squid.kraken.v4.core.analysis.engine.cartography;

import java.util.List;

import com.squid.core.expression.reference.Cardinality;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.Relation;

/**
 * A simple path made of a single relation
 */
public class DirectPath implements InternalPath {
	
	private Relation relation;
	private DomainPK source;
	
	public DirectPath(Relation relation, DomainPK source) {
		this.relation = relation;
		this.source = source;
	}
	
	@Override
	public Path.Type getType() {
		if (source.equals(relation.getLeftId())) {
			return getType(relation.getLeftCardinality(),relation.getRightCardinality());
		} else {
			return getType(relation.getRightCardinality(),relation.getLeftCardinality());
		}
	}
	
	private Path.Type getType(Cardinality source, Cardinality target) {
		switch (source) {
		case ONE:
		case ZERO_OR_ONE:
			if (target==Cardinality.MANY) return Path.Type.ONE_MANY; else return Path.Type.ONE_ONE;
		case MANY:
		default:
			if (target==Cardinality.MANY) return Path.Type.MANY_MANY; else return Path.Type.MANY_ONE;
		}
	}
	
	@Override
	public List<Relation> append(List<Relation> append) {
		append.add(relation);
		return append;
	}
	
	@Override
	public Space apply(Space s) throws ScopeException {
		return s.S(relation);
	}
	
	@Override
	public String toString() {
		return relation.getName()+" : "+getType();
	}
	
	@Override
	public int size() {
		return 1;
	}
	
	@Override
	public boolean contains(DomainPK node) {
		return node.equals(relation.getLeftId()) || node.equals(relation.getRightId());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((relation == null) ? 0 : relation.hashCode());
		result = prime * result + ((source == null) ? 0 : source.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass()) {
			if (obj instanceof Space) {
				Space space = (Space)obj;
				if (space.getParent()!=null && space.getParent().getDomain().getId().equals(this.source) && this.relation.equals(space.getRelation())) {
					return true;
				}
			}
			// else
			return false;
		}
		DirectPath other = (DirectPath) obj;
		if (relation == null) {
			if (other.relation != null)
				return false;
		} else if (!relation.equals(other.relation))
			return false;
		if (source == null) {
			if (other.source != null)
				return false;
		} else if (!source.equals(other.source))
			return false;
		return true;
	}
	
}