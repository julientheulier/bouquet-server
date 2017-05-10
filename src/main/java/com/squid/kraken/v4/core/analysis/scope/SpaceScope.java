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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.squid.core.database.model.Column;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.operators.OperatorDefinition;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.IdentifierType;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.expression.reference.ColumnDomainReference;
import com.squid.kraken.v4.core.expression.reference.ParameterReference;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.Relation;
import com.squid.kraken.v4.model.RelationPK;

public class SpaceScope extends AnalysisScope {

    protected Space space;

    public SpaceScope(Space space) {
        super();
        this.space = space;
    }

	private HashMap<String, ExpressionAST> params = new HashMap<>();
	
	/**
	 * allow this param to be use in the expression (note that some params are always available)
	 * @param param
	 * @param type == the param type
	 */
	public void addParam(String param, ExpressionAST definition) {
		params.put(param, definition);
	}
    
    /**
	 * @return the space
	 */
	public Space getSpace() {
		return space;
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
		//
		// SELF
		if (getSpace()!=null && identifierType.equals(IdentifierType.PARAMETER) && name.equalsIgnoreCase("SELF")) {
			return new ParameterReference("SELF", getSpace().getImageDomain());
		}
		//
        // lookup a subdoain
        if (identifierType == IdentifierType.DEFAULT || identifierType == DOMAIN) {
            Relation relation = space.findRelation(name);
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
	        	Relation relation = ProjectManager.INSTANCE.findRelation(space.getUniverse().getContext(), relationPk);
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
            	List<Dimension> dimensions = space.getUniverse().getDomainHierarchy(space.getDomain()).getDimensions(space.getUniverse().getContext());
            	for (Dimension dimension : dimensions) {
            		if (dimension!=null && dimension.getOid().equals(name)) {
            			return space.A(dimension);
            		}
            	}
            	// T1392: the following code is there for compatibility reason while we look for a clean solution
            	// - the problem is to move facetID from one domain to another while preserving the selection
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
		// parameters ?
		if (identifierType==IdentifierType.PARAMETER) {
			if (params.containsKey(name)) {
				return params.get(name);
			}
		}
        // else
        throw new ScopeException("identifier not found in '"+space.getDomain().getName()+"': " + name);
    }
    
    @Override
    public void buildDefinitionList(List<Object> definitions) {
    	super.buildDefinitionList(definitions);
    	//
    	for (Axis axis : space.A(true)) {// only print the visible scope
			try {
				IDomain image = axis.getDefinitionSafe().getImageDomain();
				if (!image.isInstanceOf(IDomain.OBJECT)) {
					definitions.add(axis);
				}
			} catch (Exception e) {
				// ignore
			}
		}
    }
    
    /* (non-Javadoc)
     * @see com.squid.core.expression.scope.DefaultExpressionConstructor#looseLookup(java.lang.String)
     */
    @Override
    public Set<OperatorDefinition> looseLookup(String fun) throws ScopeException {
    	// T3028 - override to filter available functions
    	Set<OperatorDefinition> operators = super.looseLookup(fun);
    	Iterator<OperatorDefinition> iter = operators.iterator();
    	while (iter.hasNext()) {
    		OperatorDefinition opDef = iter.next();
    		if (!space.getUniverse().getDatabase().getSkin().canRender(opDef.getExtendedID())) {
    			iter.remove();
    		}
    	}
    	return operators;
    }

}
