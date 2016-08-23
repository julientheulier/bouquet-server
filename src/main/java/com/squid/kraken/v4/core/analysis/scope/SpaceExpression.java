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
package com.squid.kraken.v4.core.analysis.scope;

import com.squid.core.domain.IDomain;
import com.squid.core.expression.PrettyPrintOptions;
import com.squid.core.expression.scope.IdentifierType;
import com.squid.kraken.v4.core.analysis.universe.Space;

public class SpaceExpression 
extends AnalysisExpression
{
	
	private Space value;
	
	public SpaceExpression(Space space) {
		this.value = space;
	}
	
	@Override
	public Object getReference() {
		return value;
	}
	
	public Space getSpace() {
		return value;
	}
	
	@Override
	public String getReferenceName() {
		if (value!=null) {
			return value.getRoot().getName();
		} else {
			return "";
		}
	}
	
	@Override
	public String getReferenceIdentifier() {
		return null;
	}
	
	@Override
	public IdentifierType getReferenceType() {
		return null;
	}
	
	@Override
	public IDomain getSourceDomain() {
	    return value!=null?value.getSourceDomain():IDomain.UNKNOWN;
	}
	
	@Override
	public IDomain getImageDomain() {
	    return value!=null?value.getImageDomain():IDomain.UNKNOWN;
	}

    @Override
    public String toString() {
        return prettyPrint();
    }
    
    @Override
    public String prettyPrint(PrettyPrintOptions options) {
        return value!=null?value.prettyPrint():"{space:undefined}";
    }

	@Override
	public int hashCode() {
		return ((value == null) ? 0 : value.hashCode());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SpaceExpression other = (SpaceExpression) obj;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
    
}
