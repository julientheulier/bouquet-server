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
import com.squid.core.domain.operators.ExtendedType;
import com.squid.core.expression.PrettyPrintConstant;
import com.squid.core.expression.PrettyPrintOptions;
import com.squid.core.expression.scope.IdentifierType;
import com.squid.core.sql.render.SQLSkin;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.Dimension;

public class AxisExpression 
extends AnalysisExpression
{
	
	private Axis value;
	
	public AxisExpression(Axis axis) {
		this.value = axis;
	}
	
	public Axis getAxis() {
		return value;
	}
	
	@Override
	public ExtendedType computeType(SQLSkin skin) {
	    if (value==null) {
            return ExtendedType.UNDEFINED;
        } else {
            return value.getDefinitionSafe().computeType(skin);
        }
	}
    
    @Override
    public IDomain getSourceDomain() {
        if (value==null) {
            return IDomain.UNKNOWN;
        } else {
            return value.getParent().getSourceDomain();
        }
    }
	
	@Override
	public IDomain getImageDomain() {
	    if (value==null) {
            return IDomain.UNKNOWN;
        } else {
            return value.getDefinitionSafe().getImageDomain();
        }
	}
	
	@Override
	public String getReferenceName() {
		if (value!=null && value.getDimension()!=null) {
			return value.getDimension().getName();
		} else {
			return value.getName();
		}
	}
	
	@Override
	public String prettyPrintIdentifier() {
		Dimension dimension = value.getDimension();
		if (dimension!=null && dimension.getId().getDimensionId()!=null) {
			String id = PrettyPrintConstant.IDENTIFIER_TAG
    				+PrettyPrintConstant.OPEN_IDENT
    				+dimension.getOid()
    				+PrettyPrintConstant.CLOSE_IDENT;
			// hide the root parent
			Space parent = value.getParent();
			while (parent.getParent()!=null) {
				id = PrettyPrintConstant.IDENTIFIER_TAG
	    			+PrettyPrintConstant.OPEN_IDENT
	    			+parent.getRelation().getOid()
	    			+PrettyPrintConstant.CLOSE_IDENT
	    			+"."+id;
				parent = parent.getParent();
			}
			return id;
		} else {
			return null;
		}
	}
	
	@Override
	public String getReferenceIdentifier() {
		Dimension dimension = value.getDimension();
		return dimension!=null?dimension.getOid():null;
	}
	
	@Override
	public IdentifierType getReferenceType() {
		return null;
	}
	
	/* (non-Javadoc)
	 * @see com.squid.core.expression.ExpressionRef#prettyPrint()
	 */
	// T1702: this has side effects... but this is truly the right way to handle the axisExpression.prettyPrint().
	@Override
	public String prettyPrint(PrettyPrintOptions options) {
		if (options==null) {
			if (value!=null) return value.prettyPrint(); else return "{axis:undefined}";
		} else {
			if (value!=null) return value.prettyPrint(options); else return "{axis:undefined}";
		}
	}
	
	@Override
	public Object getReference() {
		return value;
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
		AxisExpression other = (AxisExpression) obj;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public String toString() {
		if (value!=null) {
			return value.toString();
		} else {
			return "AXIS(undefined)";
		}
	}

}
