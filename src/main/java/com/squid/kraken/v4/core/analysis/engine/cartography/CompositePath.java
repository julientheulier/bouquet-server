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

import java.util.ArrayList;
import java.util.List;

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.Relation;

/**
 * Composite path is made of several Relations and can be constructed from two adjacent Path
 */
public class CompositePath implements InternalPath {
	
	private List<Relation> relations;
	private Path.Type type;
	
	public CompositePath(Path source, Path target) {
		this.relations = ((InternalPath)target).append(((InternalPath)source).append(new ArrayList<Relation>()));
		this.type = getType(source.getType(),target.getType());
	}
	
	private Path.Type getType(Path.Type source, Path.Type target) {
		if (source==Path.Type.INFINITE || target==Path.Type.INFINITE) return Path.Type.INFINITE;
		switch (source) {
		case ONE_ONE:
			return target;
		case ONE_MANY:
			if (target==Path.Type.MANY_ONE || target==Type.MANY_MANY) return Path.Type.MANY_MANY; else return source;
		case MANY_ONE:
			if (target==Path.Type.ONE_MANY || target==Type.MANY_MANY) return Path.Type.INFINITE; else return source;
		case MANY_MANY:
			if (target==Type.ONE_MANY || target==Type.MANY_MANY) return Path.Type.INFINITE; else return source;
		case INFINITE:
		default:
			return Type.INFINITE;
		}
	}
	
	@Override
	public int size() {
		return relations.size();
	}

	@Override
	public Path.Type getType() {
		return type;
	}
	
	@Override
	public List<Relation> append(List<Relation> append) {
		append.addAll(relations);
		return append;
	}
	
	@Override
	public Space apply(Space s) throws ScopeException {
		for (Relation relation : relations) {
			s = s.S(relation);
		}
		return s;
	}
	
	@Override
	public boolean contains(DomainPK node) {
		for (Relation relation : relations) {
			if (node.equals(relation.getLeftId()) || node.equals(relation.getRightId())) {
				return true;
			}
		}
		// else
		return false;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((relations == null) ? 0 : relations.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
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
				List<Relation> rels = new ArrayList<Relation>();
				Relation rel = space.getRelation();
				while (rel!=null) {
					rels.add(0, rel);
					space = space.getParent();
					rel = space.getRelation();
				}
				return rels.equals(this.relations);
			}
			// else
			return false;
		}
		CompositePath other = (CompositePath) obj;
		if (relations == null) {
			if (other.relations != null)
				return false;
		} else if (!relations.equals(other.relations))
			return false;
		if (type != other.type)
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuffer buffer = null;
		for (Relation relation : relations) {
			if (buffer==null) {
				buffer = new StringBuffer();
			} else {
				buffer.append(".");
			}
			buffer.append(relation.getName());
		}
		buffer.append(" : ").append(getType());
		return buffer.toString();
	}
	
}