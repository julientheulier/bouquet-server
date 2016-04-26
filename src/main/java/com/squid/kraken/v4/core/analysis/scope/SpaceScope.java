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

import java.util.concurrent.ExecutionException;

import com.squid.core.database.model.Column;
import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.kraken.v4.core.expression.reference.ColumnDomainReference;
import com.squid.core.expression.scope.IdentifierType;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.Relation;
import com.squid.kraken.v4.model.RelationPK;

public class SpaceScope extends AnalysisScope {

    private Space space;

    public SpaceScope(Space space) {
        super();
        this.space = space;
    }
    
    @Override
    public ExpressionAST createReferringExpression(Object object)
            throws ScopeException {
        if (object instanceof Column) {
            Column column = (Column)object;
            return new ColumnDomainReference(space.getUniverse(),space.getDomain(),column);
        } else {
            return super.createReferringExpression(object);
        }
    }

    @Override
    public Object lookupObject(IdentifierType identifierType, String name) throws ScopeException {
        // lookup a subdoain
        if (identifierType == IdentifierType.DEFAULT || identifierType == DOMAIN) {
            Relation relation = space.getUniverse().getRelation(space.getDomain(), name);
            if (relation != null) {
                return new Space(space, relation);
            }
        }
        // else look for a measure
        if (identifierType == IdentifierType.DEFAULT || identifierType == MEASURE) {
            for (Metric metric : space.getUniverse().getMetrics(space.getDomain())) {
                if (metric.getName().equals(name)) {
                    return space.M(metric);
                }
            }
        }
        // else look for an axis
        if (identifierType == IdentifierType.DEFAULT || identifierType == AXIS) {
        	try {
	            for (Axis axis : space.A()) {
	            	DimensionIndex index = axis.getIndex();
	            	if (index!=null) {
	            		Dimension dimension = index.getDimension();
		                if (dimension!=null && index.getDimensionName().equals(name)) {
		                	// KRKN-107 : if it's a sub-domain, don't link through the dimension
		                    if (!axis.getDefinitionSafe().getImageDomain().isInstanceOf(IDomain.OBJECT)) {
		                    	return axis;
		                    } else {
		                    	return axis;// give me the good stuff !
		                    }
		                }
	            	}
	            }
        	} catch (InterruptedException | ComputingException e) {
        		//
        	}
        }
        //
        // ticket:3128
        // support referencing a column directly... this may not be what we want
        if (identifierType == IdentifierType.DEFAULT || identifierType == IdentifierType.COLUMN) {
        	try {
	            for (Column column : space.getTable().getColumns()) {
	                if (column.getName().equals(name)) {
	                    return column;
	                }
	            }
        	} catch (ExecutionException e) {
        		throw new ScopeException(e);
        	}
        }
        //
        // krkn-84: support ID lookup
        if (identifierType==IdentifierType.IDENTIFIER) {
        	// KRKN-107 : check for relation first
            // relations
        	{
	        	RelationPK relationPk = new RelationPK(this.space.getUniverse().getProject().getId(), name);
	        	Relation relation = 
	        			ProjectManager.INSTANCE.findRelation(
	        					space.getUniverse().getContext(), 
	        					space.getDomain().getId(),
	        					relationPk);
	        	if (relation!=null) {
	        		if (!space.isComposable(relation)) {
	        			throw new ScopeException("cannot compose relation @'"+relation.getOid()+"' ("+relation.getLeftName()+"/"+relation.getRightName()+") with "+space.toString());
	        		}
	        		return new Space(space, relation);
	        	}
        	}
            // metric
            for (Metric metric : space.getUniverse().getMetrics(space.getDomain())) {
                if (metric.getOid().equals(name)) {
                    return space.M(metric);
                }
            }
            // dimension
            try {
	            for (Axis axis : space.A()) {
	            	DimensionIndex index = axis.getIndex();
	            	if (index!=null) {
	            		Dimension dimension = index.getDimension();
	            		if (dimension!=null && dimension.getOid().equals(name)) {
		                	// KRKN-107 : if it's a sub-domain, don't link through the dimension
		                    if (!axis.getDefinitionSafe().getImageDomain().isInstanceOf(IDomain.OBJECT)) {
		                    	return axis;
		                    } else {
		                    	return axis;
		                    	//throw new ScopeException("dimension '"+index.getDimensionName()+"' definition has invalid type OBJECT");
		                    }
		                }
	            	}
	            }
            } catch (InterruptedException | ComputingException e) {
            	// ignore
            }
        }
        // else
        throw new ScopeException("identifier not found in '"+space.getDomain().getName()+"': " + name);
    }

}
