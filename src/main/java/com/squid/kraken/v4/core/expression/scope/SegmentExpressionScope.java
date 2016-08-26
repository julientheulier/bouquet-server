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
package com.squid.kraken.v4.core.expression.scope;

import java.util.List;
import java.util.concurrent.ExecutionException;

import com.squid.core.database.model.Column;
import com.squid.core.database.model.Table;
import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ExpressionDiagnostic;
import com.squid.core.expression.scope.IdentifierType;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.core.analysis.scope.SpaceScope;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.AccessRight.Role;

public class SegmentExpressionScope extends SpaceScope {

    private Table table;

    public SegmentExpressionScope(Space root)
            throws ScopeException {
        super(root);
        this.table = getSpace().getTable();
    }
    
    @Override
    public ExpressionDiagnostic validateExpression(ExpressionAST expression) {
        if (expression.getImageDomain().isInstanceOf(IDomain.CONDITIONAL)) {
            return ExpressionDiagnostic.IS_VALID;
        } else {
            return new ExpressionDiagnostic("invalid segment type, must be a condition");
        }
    }
    
    @Override
    public void buildDefinitionList(List<Object> definitions) {
        //
    	try {
    		definitions.addAll(table.getColumns());
    	} catch (ExecutionException e) {
    		//
    	}
    	//
    	super.buildDefinitionList(definitions);
    }

    @Override
    public Object lookupObject(IdentifierType identifierType, String identifier) throws ScopeException {
        //
        // check for column only if user as privilege
        if (table!=null && AccessRightsUtils.getInstance().hasRole(getSpace().getUniverse().getContext(), getSpace().getDomain(), Role.WRITE)) {
        	try {
				Column col = table.findColumnByName(identifier);
				if (col!=null) {
					return col;
				}
			} catch (ExecutionException e) {
				// ignore
			}
        }
        return super.lookupObject(identifierType, identifier);
    }
    
    /*
    @Override
    public ExpressionAST createReferringExpression(Object reference)
            throws ScopeException {
        if (reference instanceof Column) {
            return new ColumnDomainReference(space,(Column)reference);
        }
        if (reference instanceof Axis) {
            return new AxisExpression((Axis)reference);
        }
        if (reference instanceof Measure) {
            return new MeasureExpression((Measure)reference);
        }
        return super.createReferringExpression(reference);
    }
    */

}
