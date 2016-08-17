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
import com.squid.core.sql.render.SQLSkin;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.Metric;

public class MeasureExpression
extends AnalysisExpression
{
	
	private Measure value;
	
	public MeasureExpression(Measure measure) {
		this.value = measure;
	}
	
	@Override
	public Object getReference() {
		return value;
	}
	
	public Measure getMeasure() {
		return value;
	}
	
	@Override
	public String getReferenceName() {
		if (value!=null) {
			return value.getName();
		} else {
			return "";
		}
	}
	
	@Override
	public String getReferenceIdentifier() {
		Metric metric = value.getMetric();
		if (metric!=null && metric.getOid()!=null) {
			String id = PrettyPrintConstant.IDENTIFIER_TAG
    				+PrettyPrintConstant.OPEN_IDENT
    				+metric.getOid()
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
			// if there is no way to reference the expression by ID, just return null
			return null;
		}
	}
	
	@Override
	public ExtendedType computeType(SQLSkin skin) {
		if (value!=null) {
			return value.getDefinitionSafe().computeType(skin);
		} else {
	        return ExtendedType.UNDEFINED;
		}
	}
	
	@Override
	public IDomain getImageDomain() {
		if (value!=null) {
			return value.getDefinitionSafe().getImageDomain();
		} else {
	        return IDomain.UNKNOWN;
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
	
	/* (non-Javadoc)
	 * @see com.squid.core.expression.ExpressionRef#prettyPrint()
	 */
	// T1702: this has side effects... but this is truly the right way to handle the axisExpression.prettyPrint().
	@Override
	public String prettyPrint() {
		if (value!=null) return value.prettyPrint(); else return "{measure:undefined}";
	}

	@Override
	public int hashCode() {
		return value!=null?value.hashCode():0;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MeasureExpression other = (MeasureExpression) obj;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "MEASURE('"+getReferenceName()+"')";
	}

}
