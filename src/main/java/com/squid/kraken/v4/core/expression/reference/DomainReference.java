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
import com.squid.core.expression.ExpressionRef;
import com.squid.core.expression.PrettyPrintConstant;
import com.squid.core.expression.reference.ColumnReference;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.core.expression.scope.IdentifierType;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.core.sql.render.SQLSkin;
import com.squid.kraken.v4.core.model.domain.ProxyDomainDomain;
import com.squid.kraken.v4.model.Domain;

public class DomainReference extends ExpressionRef {
	
	private Domain reference;
	private IDomain image;
	
	public DomainReference(Space space) {
		super();
		this.reference = space.getDomain();
		this.image = new ProxyDomainDomain(space.getUniverse(), this.reference);
	}
	
	public DomainReference(Universe universe, Domain reference) {
		super();
		this.reference = reference;
		this.image = new ProxyDomainDomain(universe, this.reference);
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
		return image;// the domain reference if invisible for the type system...
	}
	
	@Override
	public String getReferenceName() {
		if (reference!=null) {
			return reference.getName();
		} else {
			return "";
		}
	}
	
	@Override
	public String getReferenceIdentifier() {
		return reference!=null?reference.getOid():null;
	}
	
	@Override
	public IdentifierType getReferenceType() {
		return null;
	}

	/**
	 */
	public Domain getDomain() {
		return this.reference;
	}

	public Object getReference() {
		return this.reference;
	}

	public String getDescription() {
		return this.reference.getDescription();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (obj instanceof ColumnReference) {
			DomainReference ref = (DomainReference) obj;
			return this.getReference() != null
					&& this.getReference().equals(ref.getReference());
		} else
			return super.equals(obj);
	}

	@Override
	public int hashCode() {
		return this.getReference() != null ? this.getReference().hashCode() : super
				.hashCode();
	}
	
	@Override
	public String toString() {
		if (this.reference!=null) {
			return "'"+this.reference.getName()+"'";
		} else {
			return "undefinedColumnReference";
		}
	}

}
