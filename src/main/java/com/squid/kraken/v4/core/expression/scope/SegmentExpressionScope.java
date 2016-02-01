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
import com.squid.core.expression.scope.DefaultScope;
import com.squid.core.expression.scope.ExpressionDiagnostic;
import com.squid.core.expression.scope.IdentifierType;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.scope.AxisExpression;
import com.squid.kraken.v4.core.analysis.scope.MeasureExpression;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.reference.ColumnDomainReference;
import com.squid.kraken.v4.model.Domain;

public class SegmentExpressionScope extends DefaultScope {

    private Space space;
    private Table table;

    public SegmentExpressionScope(Universe universe, Domain domain)
            throws ScopeException {
        super();
        this.space = universe.S(domain);
        this.table = space.getTable();
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
        definitions.addAll(space.M());
    }
    

    @Override
    public IdentifierType lookupIdentifierType(String image) {
        // TODO Auto-generated method stub
        return IdentifierType.DEFAULT;
    }

    @Override
    public Object lookupObject(IdentifierType identifierType, String identifier) throws ScopeException {
    	// T15 - since the columns are visible, no need to build filter on this
    	Axis axis = space.A(identifier);
    	if (axis!=null) return axis;
        Measure measure = space.M(identifier);
        if (measure!=null) return measure;
        return super.lookupObject(identifierType, identifier);
    }
    
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

}
