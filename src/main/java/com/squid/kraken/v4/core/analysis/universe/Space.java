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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.squid.core.database.model.Table;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.set.SetDomain;
import com.squid.core.expression.Compose;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.PrettyPrintConstant;
import com.squid.core.expression.UndefinedExpression;
import com.squid.kraken.v4.core.expression.reference.RelationReference;
import com.squid.kraken.v4.core.model.domain.ProxyDomainDomain;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchy;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchyManager;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainOption;
import com.squid.kraken.v4.model.ExpressionObject;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.Relation;
import com.squid.core.expression.reference.RelationDirection;

/**
 * A Space identifies a Domain
 * @author sfantino
 *
 */
public class Space {

	private Universe universe;
	private Domain rootDomain;
	private Space parent = null;
	private Domain domain;
	private Relation relation = null;
	private String ID = "";

	private DomainOption domainOptions = null;

	private ExpressionAST def_cache;//cache the space definition
	
	public Space(Universe universe, Domain domain) {
		this.universe = universe;
		this.rootDomain = domain;
		this.domain = domain;
		this.ID = domain.getId().toUUID();
	}
	
	public Space(Space parent, Relation relation) throws ScopeException {
		this.universe = parent.getUniverse();
		this.rootDomain = parent.rootDomain;
		this.parent = parent;
		RelationDirection direction = relation.getDirection(parent.getDomain().getId());
		if (direction==RelationDirection.LEFT_TO_RIGHT) {
			this.domain = universe.getDomain(relation.getRightId());
		} else if (direction==RelationDirection.RIGHT_TO_LEFT) {
			this.domain = universe.getDomain(relation.getLeftId());
		} else {
			throw new ScopeException("Relation '"+relation.getName()+"' is not applicable to source domain '"+parent.getDomain().getName()+"'");
		}
		this.relation = relation;
		this.ID = (parent!=null?parent.ID+"/":"")+relation.getId().toUUID();
	}
	
	/**
	 * check if it is OK to compose this space with the given relation
	 * @param relation
	 * @return
	 */
	public boolean isComposable(Relation relation) {
		RelationDirection direction = relation.getDirection(this.getDomain().getId());
		return direction!=RelationDirection.NO_WAY;
	}
	
	public Space asRootUserContext() throws ScopeException {
	    if (parent==null) {
	        Universe universeAsRoot = universe.asRootUserContext();
	        return new Space(universeAsRoot, rootDomain);
	    } else {
	        Space parentAsRoot = this.parent.asRootUserContext();
	        return new Space(parentAsRoot, this.relation);
	    }
	}
	
	/**
	 * return the space length = number of parents
	 * @return
	 */
	public int length() {
		if (parent==null) {
			return 0;
		} else {
			return 1+parent.length();
		}
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj==this) {
			return true;
		} else
		if (obj instanceof Space) {
			return this.ID.equals(((Space)obj).ID);
		}
		else
			return false;
	}
	
	@Override
	public int hashCode() {
		return this.ID.hashCode();
	}

    public IDomain getSourceDomain() {
        return new ProxyDomainDomain(universe, getRoot());
    }

    public IDomain getImageDomain() {
    	if (getParent()!=null) {
    		// to support SET
    		return getDefinitionSafe().getImageDomain();
    	} else {
    		return new ProxyDomainDomain(universe, getDomain());
    	}
    }
	
	public String getID() {
		return ID;
	}

	public Universe getUniverse() {
		return universe;
	}

	public Domain getDomain() {
		return domain;
	}

	public DomainOption getDomainOption() {
		return domainOptions;
	}

	public Table getTable() throws ScopeException {
		return ProjectManager.INSTANCE.getTable(this);
	}

	public Table getTableSafe() {
		try {
			return universe.getTable(getDomain());
		} catch (ScopeException e) {
			return null;
		}
	}
	
	/**
	 * return an UUID for the associated table
	 * @return
	 */
    public String getTableUUID() {
        try {
            return universe.getTableUUID(getTable());
        } catch (ScopeException e) {
            return null;
        }
    }
	
	public Relation getRelation() {
		return relation;
	}
	
	public String getRelationName() {
		if (this.relation!=null && getParent()!=null && getParent().getDomain()!=null) {
			RelationDirection direction = relation.getDirection(getParent().getDomain().getId());
			if (direction==RelationDirection.LEFT_TO_RIGHT) {
				return relation.getRightName();
			} else if (direction==RelationDirection.RIGHT_TO_LEFT) {
				return relation.getLeftName();
			} else {
				return "__undefined__";
			}
		} else {
			return null;
		}
	}

	/**
	 * return the direct parent or null if top
	 * @return
	 */
	public Space getParent() {
		return parent;
	}

	/**
	 * return the top domain
	 * @return
	 */
	public Domain getRoot() {
		return rootDomain;
	}

	/**
	 * return the chain of domains
	 * @return
	 */
    public List<Domain> getDomains() {
        ArrayList<Domain> path = new ArrayList<>();
        path.add(this.domain);
        Space parent = this.parent;
        while (parent!=null) {
            path.add(parent.getDomain());
            parent = parent.getParent();
        }
        return path;
    }
	
	/**
	 * return the top parent
	 * @return
	 */
	public Space getTop() {
	    Space top = this;
	    while (top.parent!=null) {
	        top = top.parent;
	    }
	    return top;
	}
	
	public Space getLeaf() {
		return new Space(universe, domain);
	}
	
	/**
	 * create a sub-space given its name
	 * @param relation
	 * @return
	 * @throws ScopeException 
	 */
	public Space S(String relationName) throws ScopeException {
		Relation relation = universe.getRelation(domain, relationName);
		if (relation!=null) {
			return new Space(this, relation);
		} else {
			throw new ScopeException("relation '"+relationName+"' not found/applicable for source domain '"+domain.getName()+"'");
		}
	}
	
	public Space S(Space subspace) throws ScopeException {
		if (!subspace.getRoot().equals(getDomain())) {
			throw new ScopeException("the SubSpace '"+subspace.getPath()+"' is incompatible with that Space '"+this.getPath()+"'");
		}
		return relink(subspace);
	}
	
	/**
	 * apply the relation to this space and return the target space
	 * @param relation
	 * @return
	 * @throws ScopeException 
	 */
	public Space S(Relation relation) throws ScopeException {
		return new Space(this, relation);
	}

    
    /**
     * construct a space from a expression
     * <li> the expression image domain must be a Domain Object
     * @param expression
     * @return
     * @throws ScopeException 
     */
    public Space S(ExpressionAST expression) throws ScopeException {
        IDomain image = expression.getImageDomain();
        if (image.isInstanceOf(IDomain.OBJECT)) {
            Object adapter = image.getAdapter(Domain.class);
            if (adapter!=null && adapter instanceof Domain) {
                if (expression instanceof Compose) {
                    Compose compose = (Compose)expression;
                    Space result = this;
                    for (ExpressionAST part : compose.getBody()) {
                        if (part instanceof RelationReference) {
                            RelationReference ref = (RelationReference)part;
                            result = result.S(ref.getRelation());
                        } else {
                            throw new ScopeException("Invalid expression");
                        }
                    }
                    return result;
                } else if (expression instanceof RelationReference) {
                    RelationReference ref = (RelationReference)expression;
                    return S(ref.getRelation());
                } else {
                    throw new ScopeException("Invalid expression");
                }
            }
        } 
        // else
        throw new ScopeException("Invalid expression type, must be a Domain Object");
    }
	
	/**
	 * return all sub-spaces
	 * @return
	 * @throws ScopeException 
	 * @throws ComputingException 
	 */
	public List<Space> S() throws ScopeException, ComputingException {
		return universe.
				getCartography().
				getSubspaces(universe,this);
	}
	
	/**
	 * construct an axis by it's dimension name
	 * @param dimension name
	 * @return the axis or null if not in the scope
	 * @throws ScopeException
	 */
	public Axis A(String dimension) throws ScopeException {
		try {
			DomainHierarchy hierarchy = universe.getDomainHierarchy(domain);
			for (DimensionIndex index : hierarchy.getDimensionIndexes()) {
				if (index.getDimensionName().compareTo(dimension)==0) {
					return A(index.getDimension());
				}
			}
		} catch (ComputingException | InterruptedException e) {
			throw new ScopeException(e);
		}
		// else
		return null;
	}
	
	public Axis A(Dimension dimension) {
		return new Axis(this, dimension);
	}

	/**
	 * combine an Axis with that Space: the Axis root must match this Space
	 * @param a
	 * @throws ScopeException 
	 * @throws ComputingException 
	 * @throws InterruptedException 
	 */
	public Axis A(Axis a) throws ScopeException, ComputingException, InterruptedException {
		if (!a.getParent().getRoot().equals(getDomain())) {
			throw new ScopeException("the Axis '"+a.prettyPrint()+"'is incompatible with that Space '"+prettyPrint()+"'");
		}
		Space x = relink(a.getParent());
		return relink(x,a.prune());// KRKN-27: for sub-sub-domain, the axis contains a complex space
	}

	/**
	 * returns all the axis
	 */
	public List<Axis> A() {
		ArrayList<Axis> axes = new ArrayList<Axis>();
		try {
			for (DimensionIndex index : universe.getDomainHierarchy(domain).getDimensionIndexes(universe.getContext())) {
				axes.add(index.getAxis());
			}
		} catch (ComputingException | InterruptedException e) {
			// ignore
		}
		return axes;
	}
	
	protected Axis relink(Space space, Axis a) throws ScopeException, ComputingException, InterruptedException {
		if (a.getParent().getDomain().equals(space.getDomain())) {
			return space.A(a.getDimension());
		} else {
			throw new ScopeException("cannot relink "+space.toString()+" with "+a.toString());
		}
	}
	
	protected Space relink(Space space) throws ScopeException {
		if (space.getDomain().equals(getDomain())) {
			return this;
		} else if (space.getParent()==null) {
			throw new ScopeException("cannot relink that space");
		} else {
			Space x = relink(space.getParent());
			return x.S(space.getRelation());
		}
	}
	
	/**
	 * prune this space using the given prefix
	 * @param prefix
	 * @return
	 * @throws ScopeException 
	 */
	public Space prune(Space prefix) throws ScopeException {
	    Space x = this;
	    ArrayList<Space> stack = new ArrayList<>();
	    while (x!=null && !x.equals(prefix)) {
	        stack.add(0,x);
	        x = x.getParent();
	    }
	    if (x==null) {
	        throw new ScopeException("cannot prune distinct spaces");
	    } else {
	        Space result = new Space(universe, x.getDomain());
	        for (Space space : stack) {
	            result = result.S(space.getRelation());
	        }
	        return result;
	    }
	}
	
	public Measure M(String metric) throws ScopeException {
		return new Measure(this, metric);
	}
	
	public Measure M(Metric metric) {
		return new Measure(this, metric);
	}
    
    public Measure M(ExpressionAST expr) throws ScopeException {
        return new Measure(this, expr);
    }

	/**
	 * return all the measures
	 * @return
	 */
	public Collection<Measure> M() {
		ArrayList<Measure> measures = new ArrayList<Measure>();
		try {
			List<Metric> metrics = DomainHierarchyManager.INSTANCE.getHierarchy(universe.getProject().getId(), domain).getMetrics(universe.getContext());
			for (Metric m : metrics) {
				measures.add(new Measure(this, m));
			}
		} catch (ComputingException | InterruptedException e) {
			//
		}
		return measures;
	}

	/**
	 * generic method that can create both axis or measure depending on the argument type (dimension or metric)
	 * @param object
	 * @return
	 * @throws ScopeException
	 */
	public Object X(ExpressionObject<?> object) throws ScopeException {
		if (object instanceof Dimension) {
			return A((Dimension)object);
		} else if (object instanceof Metric) {
			return M((Metric)object);
		} else {
			throw new ScopeException("invalid object");
		}
	}
	
	public String getPath() {
	    return ">"+this.domain.getName()+((parent != null)?parent.getPath():"");
	}
	
	/**
	 * return a V4 compatible expression
	 * @return
	 */
	public String prettyPrint() {
		String pp = "";
		Space parent = this;
		while (parent!=null) {
			if (pp!="") {
				pp = "."+pp;
			}
			if (parent.getParent()!=null) {
				pp = PrettyPrintConstant.IDENTIFIER_TAG
	    			+PrettyPrintConstant.OPEN_IDENT
	    			+parent.relation.getOid()
	    			+PrettyPrintConstant.CLOSE_IDENT
	    			+pp;// krkn-84
			} else {
				pp = PrettyPrintConstant.IDENTIFIER_TAG
	    			+PrettyPrintConstant.OPEN_IDENT
	    			+parent.getDomain().getOid()
	    			+PrettyPrintConstant.CLOSE_IDENT
	    			+pp;// krkn-84
			}
			parent = parent.getParent();
		}
		//
		return pp;
	}

	@Override
	public String toString() {
		if (parent!=null) {
			return parent.toString()+"."+getRelationName();
		} else {
			return domain.getName();
		}
	}

	/**
	 * return the space definition. This either a compose if the space has parent, or null if it is a top level space
	 * @return
	 * @throws ScopeException
	 */
	public ExpressionAST getDefinition() throws ScopeException {
		if (def_cache==null) {
			if (getParent()==null) {
				//def_cache = new DomainReference(getDomain());
				return null;
			} else {
				// == parent_def.relation
				ExpressionAST inherit = getParent().getDefinition();
				if (inherit==null) {
					def_cache = new RelationReference(universe, getRelation(), getParent().getDomain(), getDomain());
				} else {
					def_cache = new Compose(inherit, new RelationReference(universe, getRelation(), getParent().getDomain(), getDomain()));
				}
			}
		}
		//
		return def_cache;
	}
	
	public ExpressionAST getDefinitionSafe() {
		try {
			return getDefinition();
		} catch (ScopeException e) {
			return new UndefinedExpression(prettyPrint());
		}
	}

	public Set<Space> computeDirectExtension() throws ScopeException, ComputingException {
		HashSet<Space> explored = new HashSet<Space>();
		explored.add(this);
		return computeDirectExtensionRec(this,explored);
	}
	
	private Set<Space> computeDirectExtensionRec(Space root, Set<Space> explored) throws ScopeException, ComputingException {
		HashSet<Space> frontier = new HashSet<Space>();
		ArrayList<Space> subspaces = new ArrayList<Space>(universe.S(root.getDomain()).S());// ignore path
		for (Space space : subspaces) {
			IDomain image = space.getDefinition().getImageDomain();
			if (!image.isInstanceOf(SetDomain.SET)) {// don't want to slice downward
				if (!explored.contains(space)) {
					frontier.add(space);
					explored.add(space);
				}
			}
		}
		for (Space next : frontier) {
			computeDirectExtensionRec(next, explored);
		}
		return explored;
	}

	public List<Dimension> getDimensions() throws ScopeException  {
		return ProjectManager.INSTANCE.getDomainContent(this).getDimensions();
	}

	public List<Metric> getMetrics() throws ScopeException {
		return ProjectManager.INSTANCE.getDomainContent(this).getMetrics();
	}

	public ExpressionAST compose(ExpressionAST definition) throws ScopeException {
		ExpressionAST def = getDefinition();
		if (def!=null) {
			return ExpressionMaker.COMPOSE(def, definition);
		} else {
			return definition;
		}
	}
	
}
