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
package com.squid.kraken.v4.core.analysis.universe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.squid.kraken.v4.core.database.impl.DatabaseServiceImpl;
import com.squid.core.expression.Compose;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.PrettyPrintConstant;
import com.squid.core.expression.PrettyPrintOptions;
import com.squid.core.expression.PrettyPrintOptions.ReferenceStyle;
import com.squid.core.expression.reference.ColumnReference;
import com.squid.core.expression.UndefinedExpression;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchy;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.scope.AxisExpression;
import com.squid.kraken.v4.model.Attribute;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.ExpressionObject;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.LzPersistentBaseImpl;
import com.squid.kraken.v4.model.Dimension.Type;

// AXIS
public class Axis implements Property {

	private Space parent = null;
	private Dimension dimension;
	private String ID = "";
	
	private String name = null;// this can be used to force the axis name
	
	private ExpressionAST def_cache;// cache the axis definition
	
	private OriginType originType = OriginType.USER; // default to User type

	private String description = null;
	private String format = null;
	
	/**
	 * copy constructor
	 * @param copy
	 */
	public Axis(Axis copy) {
		this.parent = copy.parent;
		this.dimension = copy.dimension;
		this.ID = copy.ID;
		this.name = copy.name;
		this.def_cache = copy.def_cache;
	}
	
	protected Axis(Space parent, ExpressionAST expression) {
        this.parent = parent;
	    this.def_cache = expression;
        this.ID =  (parent!=null?parent.getID()+"/":"")+expression.prettyPrint();
	}

	protected Axis(Space parent, Dimension dimension) {
		this.parent = parent;
		this.dimension = dimension;
		this.ID = (parent!=null?parent.getID()+"/":"")+dimension.getId().toUUID();
	}
	
	protected Axis(Axis copy, String ID) {
	    this.parent = copy.parent;
	    this.dimension = copy.dimension;
	    this.def_cache = copy.def_cache;
	    this.ID = ID;
    }
	
	@Override
	public OriginType getOriginType() {
		return originType;
	}
	
	public void setOriginType(OriginType originType) {
		this.originType = originType;
	}

    public Axis withId(String ID) {
	    return new Axis(this, ID);
	}
    
    /**
     * override the standard name
     * @param name
     */
    public void setName(String name) {
		this.name = name;
	}
	
	/**
	 * set this axis name
	 * @param name
	 * @return
	 */
	public Axis withName(String name) {
	    this.name = name;
	    return this;
	}
	
	public Axis withNickname(Axis axis) {
		if (axis.name!=null) {
			this.name = axis.name;
		}
		return this;
	}
	
	/**
	 * check if the Axis has a name
	 * @return
	 */
	public boolean hasName() {
		return name!=null;
	}
    
    public String getName() {
    	if (name!=null) {
    		return name;// use the provided one
    	}
        if (dimension!=null) {
            DimensionIndex index;
            try {
                index = getIndex();
            } catch (Exception e) {
                return ID;
            }
            return index!=null?index.getDimensionName():dimension.getName();
        } else {
        	// kick hack to support column-reference
        	if (this.def_cache!=null && this.def_cache instanceof ColumnReference) {
        		return "#"+((ColumnReference)this.def_cache).getReferenceName();
        	} else {
        		return ID;
        	}
        }
    }
	
	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.core.analysis.universe.Property#getDescription()
	 */
	@Override
	public String getDescription() {
		return this.description!=null?this.description:(this.dimension!=null?this.dimension.getDescription():null);
	}
	
	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}
	
	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.core.analysis.universe.Property#getFormat()
	 */
	@Override
	public String getFormat() {
		return this.format;
	}
	
	/**
	 * @param format the format to set
	 */
	public void setFormat(String format) {
		this.format = format;
	}
	
	public String getId() {
		return ID;
	}
	
	public Space getParent() {
		return parent;
	}
	
	/**
	 * check if this axis is a parent Dimension of the putativeChild
	 * @param putativeChild
	 * @return
	 * @throws InterruptedException 
	 * @throws ComputingException 
	 */
	public boolean isParentDimension(Axis putativeChild) throws ComputingException, InterruptedException {
		// check if the orderBy is a parent of a rollup
		DimensionIndex parentIndex = getIndex();
		DimensionIndex childIndex = putativeChild.getIndex();
		if (parentIndex==null || childIndex==null) {
			return false;// cannot check dynamic axes
		}
		if (parentIndex.getRoot().equals(childIndex.getRoot())) {// same hierarchy
			if (getParent().equals(putativeChild.getParent())) {// same space
				if (parentIndex.equals(childIndex)) {
					return true;// same same
				}
				// else
				while (childIndex.getParent()!=null) {// grand-parent?
					childIndex = childIndex.getParent();
					if (parentIndex.equals(childIndex)) {
						return true;
					}
				}
			}
		}
		// else
		return false;
	}
	
	public DimensionMember getMemberByID(Object ID) {
		DimensionIndex index = null;
		try {
			index = getIndex(true);
		} catch (ComputingException | InterruptedException e) {
			// ignore, assume that index is null
		}
		if (index!=null) {
			return index.getMemberByID(ID);
		} else {
			// create a new one
			return new DimensionMember(-1, ID, 0);
		}
	}
	
	public DimensionIndex getIndex() throws ComputingException, InterruptedException {
		return getIndex(true);
	}

	public DimensionIndex getIndex(boolean wait) throws ComputingException, InterruptedException {
	    Universe universe = parent.getUniverse();
		DomainHierarchy hierarchy = universe.getDomainHierarchy(parent.getRoot(), true);// the index is attached to the root; in case of a sub-dimension, the index is a proxy to the actual dimension. If you think it should be parent.getDomain(), that's not good...
		if (hierarchy!=null) {
			DimensionIndex res =  hierarchy.getDimensionIndex(this);
			return res;
		} else {
			return null;
		}
	}
	
	public Dimension getDimension() {
		return dimension;
	}
	
	@Override
	public ExpressionObject<?> getExpressionObject() {
		return dimension;
	}
	
	public Type getDimensionType() {
		if (dimension!=null) {
			return dimension.getType();
		} else {
			return Type.INDEX;
		}
	}
	
	public List<Attribute> getAttributes() {
		return parent.getUniverse().getAttributes(dimension);
	}
	
	/**
	 * return the Axis definition which is equal to compose(space.getDefinition(),dimension.getDEfinition())
	 * @return
	 * @throws ScopeException
	 */
	@Override
	public ExpressionAST getDefinition() throws ScopeException {
		if (def_cache==null) {
			ExpressionAST first = getParent().getDefinition();
			ExpressionAST second = getParent().getUniverse().getParser().parse(getParent().getDomain(), dimension);
			if (first!=null) {
				def_cache = new Compose(first,second);
			} else {
				def_cache = second;
			}
		}
		//
		return def_cache;
	}
	
	/**
	 * return the axis definition. If the definition is invalid, return an UndefinedExpression wrapping the offending expression
	 * @return
	 */
	@Override
	public ExpressionAST getDefinitionSafe() {
		try {
			return getDefinition();
		} catch (ScopeException e) {
			return new UndefinedExpression(dimension.getExpression() == null ? null : dimension.getExpression().getValue());
		}
	}

	public Axis A(String dimension) throws ScopeException {
		for (Dimension d : parent.getUniverse().getSubDimensions(this.dimension)) {
			if (d.getName().equals(dimension)) {
				return new Axis(parent, d);
			}
		}
		throw new ScopeException("dimension not found: "+dimension);
	}
	
	public Axis A(Dimension dimension) {
		return new Axis(parent, dimension);
	}
	
	/**
	 * return this axis sub-hierarchy (dimension whit parent = this.axis.dimension)
	 * @return
	 */
    public List<Axis> H() {
        if (dimension!=null && dimension.getId().getDimensionId()!=null) {
            ArrayList<Axis> axes = new ArrayList<Axis>();
            for (Dimension dimension : getParent().getUniverse().getSubDimensions(this.dimension)) {
                Axis a = new Axis(parent, dimension);
                axes.add(a);
            }
            return axes;
        } else {
            return Collections.emptyList();
        }
    }
	
    @Deprecated
	public Collection<DimensionMember> find(Object something) throws InterruptedException {
		try {
			return getIndex().simpleLookup(something);
		} catch (ComputingException e) {
			return Collections.emptyList();
		}
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this==obj) {
			return true;
		} 
		else if (obj instanceof Axis) {
			Axis axis = (Axis)obj;
			return this.getId().equals(axis.getId());
		}
		else
			return false;
	}
	
	@Override
	public int hashCode() {
		return this.getId().hashCode();
	}

	
	/**
	 * return a V4 compatible expression attached to the provided scope: that is the expression could be parsed in this scope
	 * @scope the parent scope or null for Universe
	 * @return
	 */
	public String prettyPrint(PrettyPrintOptions options) {
	    if (dimension!=null && dimension.getId().getDimensionId()!=null) {
    		String pp = getParent().prettyPrint(options);
    		if (pp!="") {
    			pp += ".";
    		}
    		// krkn-84 use ID reference
			return pp+prettyPrintObject(getDimension(), options);
	    } else if (dimension!=null && dimension.getId().getDimensionId()==null && def_cache!=null) {
	    	// krkn-93
    		String pp = getParent().prettyPrint(options);
    		if (pp!="") {
    			pp += ".";
    		}
    		return pp+def_cache.prettyPrint(options);
	    } else if (def_cache!=null) {
	        return def_cache.prettyPrint(options);
	    } else {
	        return "**undefined**";
	    }
	}
	
	/**
	 * utility method to pretty-print a object id
	 * @param object
	 * @param options
	 * @return
	 */
	private static String prettyPrintObject(LzPersistentBaseImpl<? extends GenericPK> object, PrettyPrintOptions options) {
		if (options!=null && options.getStyle()==ReferenceStyle.NAME) {
			return PrettyPrintConstant.OPEN_IDENT
	    			+object.getName()
	    			+PrettyPrintConstant.CLOSE_IDENT;
		} else {// krkn-84: default is ID
			return PrettyPrintConstant.IDENTIFIER_TAG
	    			+PrettyPrintConstant.OPEN_IDENT
					+object.getOid()
					+PrettyPrintConstant.CLOSE_IDENT;
		}
	}
	
	/**
	 * return a V4 compatible expression
	 * @return
	 */
	public String prettyPrint() {
		return prettyPrint(null);
	}
	
	@Override
	public String toString() {
		return "Axis/"+getParent().toString()+"."+(dimension!=null?dimension.getName():(def_cache!=null?def_cache.prettyPrint():"???"));
	}

	/**
	 * cut he axis to the direct parent
	 * @return
	 */
	public Axis prune() {
	    if (this.parent.getParent()==null) {
	        return this;
	    } else {
	        Space root = new Space(this.parent.getUniverse(), this.getParent().getDomain());
	        return root.A(this.dimension).withName(name);
	    }
	}
	
	/**
	 * cut the prefix part from the axis
	 * @param prefix
	 * @return
	 * @throws ScopeException 
	 */
	public Axis prune(Space prefix) throws ScopeException {
	    Space pruned = parent.prune(prefix);
        return pruned.A(dimension);
	}
	
	public float getEstimatedSize() {
		try {
			float stats = computeStatistics();
			if (stats>=0) {
				return stats;
			} else {
			    DimensionIndex index = getIndex(false);// prevent deadlock if called from the HierarchyCompute code
			    if (index!=null) {
    				stats = index.getMembers().size();
    				return stats>0?stats:-1;
			    } else {
			        return -1;// we really don't know
			    }
			}
		} catch (Exception e) {
			return -1;
		}
	}
	
	protected float computeStatistics() throws ScopeException, ExecutionException {
		ExpressionAST definition = getDefinition();
		return DatabaseServiceImpl.INSTANCE.computeStatistics(getParent().getUniverse().getProject(), definition);
	}
	
	@Override
	public ExpressionAST getReference() {
		return new AxisExpression(this);
	}
	
}
