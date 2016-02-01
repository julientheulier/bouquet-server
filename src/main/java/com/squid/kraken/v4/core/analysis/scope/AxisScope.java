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

import com.squid.core.expression.scope.IdentifierType;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.model.Dimension;

public class AxisScope
extends AnalysisScope {
	
	private Axis axis;

	public AxisScope(Axis axis) {
		super();
		this.axis = axis;
	}

	@Override
	public Object lookupObject(IdentifierType identifierType, String name) throws ScopeException {
		// else look for an axis
		if (identifierType==IdentifierType.DEFAULT || identifierType==AXIS) {
			for (Dimension dimension : axis.getParent().getUniverse().getSubDimensions(axis.getDimension())) {
				if (dimension.getName().equals(name)) {
					return axis.A(dimension);
				}
			}
		}
		else if (identifierType==IdentifierType.IDENTIFIER) {
		    // use the ID
            for (Dimension dimension : axis.getParent().getUniverse().getSubDimensions(axis.getDimension())) {
                if (dimension.getOid().equals(name)) {
                    return axis.A(dimension);
                }
            }
		}
		// else
		throw new ScopeException("identifier not found: "+name);
	}

}
