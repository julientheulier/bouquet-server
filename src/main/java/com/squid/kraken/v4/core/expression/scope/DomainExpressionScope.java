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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.squid.core.database.model.Column;
import com.squid.core.database.model.Table;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.operators.OperatorDefinition;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.PrettyPrintOptions;
import com.squid.core.expression.reference.ColumnReference;
import com.squid.core.expression.reference.RelationDirection;
import com.squid.core.expression.scope.DefaultScope;
import com.squid.core.expression.scope.ExpressionScope;
import com.squid.core.expression.scope.IdentifierType;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.cartography.Path.Type;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.project.DynamicManager;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.scope.AxisExpression;
import com.squid.kraken.v4.core.analysis.scope.MeasureExpression;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.reference.ColumnDomainReference;
import com.squid.kraken.v4.core.expression.reference.ParameterReference;
import com.squid.kraken.v4.core.expression.reference.RelationReference;
import com.squid.kraken.v4.core.model.domain.DomainDomain;
import com.squid.kraken.v4.core.model.domain.DomainDomainImp;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.ExpressionObject;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.Relation;
import com.squid.kraken.v4.model.RelationPK;

/**
 * Expression Scope for a Domain object
 * @author sfantino
 *
 */
public class DomainExpressionScope extends DefaultScope {
	
	public static final IdentifierType METRIC = new IdentifierType("metric");
	public static final IdentifierType DIMENSION = new IdentifierType("dim");
	public static final IdentifierType RELATION = new IdentifierType("rel");

	private Space space = null;

	private Domain domain = null;
	private Table table = null;
	
	private Universe universe;
	
	public static final boolean RESTRICTED = true;// just a alias to keep the code meaningful
	
	/**
	 * allows to restrict the scope to database objects only
	 */
	protected boolean restrictedScope = false;

	protected Collection<ExpressionObject<?>> scope = null;
	protected List<Relation> relationScope = null;

	public DomainExpressionScope(Universe universe, Domain domain) throws ScopeException {
		this(universe, domain, (Space)null);
	}

	public DomainExpressionScope(Universe universe, Domain domain, Collection<ExpressionObject<?>> scope) throws ScopeException {
		this(universe, domain, (Space)null);
		this.scope = scope;
	}

	public DomainExpressionScope(Universe universe, Domain domain, boolean restrictedScope) throws ScopeException {
		this(universe, domain, null, restrictedScope);
	}

	public DomainExpressionScope(Universe universe, Domain domain, Space space) throws ScopeException {
		super();
		this.universe = universe;
		this.domain = domain;
		if (space!=null) {
			this.space = space;
		} else {
			this.space = universe.S(domain);
		}
		// get the mapping
		this.table  = lookupTable(domain);
	}

	public DomainExpressionScope(Universe universe, Domain domain, Space space, boolean restrictedScope) throws ScopeException {
		this(universe, domain, space);
		this.restrictedScope = restrictedScope;
	}

	protected DomainExpressionScope(Universe universe, Domain domain, Space space, boolean restrictedScope, Collection<ExpressionObject<?>> scope) throws ScopeException {
		this(universe, domain, space, restrictedScope);
		this.scope = scope;
	}
	
	public DomainExpressionScope(Universe universe, Domain domain, boolean restrictedScope, List<Relation> relationScope) throws ScopeException {
		this(universe, domain, null, restrictedScope);
		this.relationScope = relationScope;
	}
	
	public DomainExpressionScope(Universe universe, Domain domain, Space space, boolean restrictedScope, List<Relation> relationScope) throws ScopeException {
		this(universe, domain, space, restrictedScope);
		this.relationScope = relationScope;
	}

	@Override
	public OperatorDefinition lookup(String fun) throws ScopeException {
		OperatorDefinition opDef = super.lookup(fun);
		if (!universe.getDatabase().getSkin().canRender(opDef.getExtendedID())) {
			throw new ScopeException("the '"+fun+"' function is not supported by this database: "+universe.getDatabase().getProductFullName());
		} else {
			return opDef;
		}
	}

	protected Space getSpace() {
		return space;
	}

	protected Domain getDomain() {
		return domain;
	}

	private Table lookupTable(Domain domain) throws ScopeException {
		// if the domain subject() is not defined, set the table to null - this is needed for testing
		if (domain.getSubject()==null || domain.getSubject().getValue()==null) {
			return null;
		}
		return universe.getTable(domain);
	}

	@Override
	public ExpressionScope applyExpression(ExpressionAST first) throws ScopeException {
		if (first instanceof ParameterReference) {
			ParameterReference ref = (ParameterReference)first;
			if (ref.getReferenceName().equalsIgnoreCase("SELF")) {
				return this;
			}
		} else if (first.getImageDomain().isInstanceOf(DomainDomain.DOMAIN)) {
			IDomain image = first.getImageDomain();
			Domain domain = DomainDomainImp.getDomain(image);
			try {
				Space target = space.S(first);
				return createScope(universe, domain, target);
			} catch (ScopeException e) {
				throw e;
			}
		}
		// else
		throw new ScopeException("cannot apply "+first.prettyPrint()+" to "+getSpace().prettyPrint());
	}

	protected ExpressionScope createScope(Universe universe, Domain domain, Space target) throws ScopeException {
		try {
			if (relationScope!=null) {
				return new DomainExpressionScope(universe, domain, target, restrictedScope, relationScope);
			} else {
				return new DomainExpressionScope(universe, domain, target, restrictedScope, scope!=null?scope:null);// the scope should contains all the available references
			}
		} catch (ScopeException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Object lookupObject(IdentifierType identifierType, String identifier) throws ScopeException {
		//
		// SELF
		if (getSpace()!=null && identifierType.equals(IdentifierType.PARAMETER) && identifier.equalsIgnoreCase("SELF")) {
			return new ParameterReference("SELF", getSpace().getImageDomain());
		}
		//
		// lookup a column if it is prefixed with #
		if (identifierType==IdentifierType.COLUMN && table!=null) {
			//
			try {
				for (Column c : this.table.getColumns()) {
					if (identifier.equals(c.getName())) {
						return c;
					}
				}
				throw new ScopeException("cannot lookup column '"+identifier+"' in table "+table.toString());
			} catch (ExecutionException e) {
				throw new ScopeException("failed to lookup column '"+identifier+"'", e);
			}
		}
		//
		// lookup by ID
		if (identifierType==IdentifierType.IDENTIFIER) {
			// relation first
			if (relationScope!=null) {
				for (Relation rel : relationScope) {
					if (rel.getOid().equals(identifier)) {
						return rel;
					}
				}
			} else {
				RelationPK relPk = new RelationPK(universe.getProject().getId(), identifier);
				Relation rel = ProjectManager.INSTANCE.findRelation(universe.getContext(), relPk);
				if (rel!=null) {
					return rel;
				}
			}
			// dimensions & metrics
			if (scope!=null) {
				for (ExpressionObject<?> object : scope) {
					if (object.getOid().equals(identifier) 
						&& object.getId().getParent().equals(domain.getId())) {// the scope may span multiple domains
						return object;
					}
				}
			} else {
				try {
					List<Dimension> dimensions = space.getDimensions();
					for (Dimension dimension : dimensions) {
						if (dimension.getOid().equals(identifier)) {
							return dimension;
						}
					}
				} catch (ScopeException e) {
					// ignore
				}
				try {
					List<Metric> metrics = space.getMetrics();
					for (Metric metric : metrics) {
						if (metric.getOid().equals(identifier)) {
							return metric;
						}
					}
				} catch (ScopeException e) {
					// ignore
				}
			}
		}
		//
		// unrestricted
		if (!restrictedScope && scope==null) {
			// check metric
			if (identifierType==IdentifierType.DEFAULT || identifierType==METRIC) {
				for (Metric m : getMetrics(false)) {
					if (m.getName().equals(identifier)) {
						return m;
					}
				}
			}
			//
			// check dimension
			if (identifierType==IdentifierType.DEFAULT || identifierType==DIMENSION) {
				Dimension dimension = lookupDimension(identifier);
				if (dimension!=null) {
					return dimension;
				}
			}
		}
		//
		// legacy: still lookup column
		if (table!=null && identifierType==IdentifierType.DEFAULT) {
			try {
				for (Column c : this.table.getColumns()) {
					if (identifier.equals(c.getName())) {
						return c;
					}
				}
			} catch (ExecutionException e) {
				throw new ScopeException("failed to lookup column '"+identifier+"'", e);
			}
		}
		//
		if (scope!=null && (identifierType==IdentifierType.DEFAULT || identifierType==DIMENSION || identifierType==METRIC)) {
			for (ExpressionObject<?> object : scope) {
				if (object.getName().equals(identifier) 
					&& checkIdentifierType(identifierType,object)
					&& object.getId().getParent().equals(domain.getId()))// the scope may span multiple domains
				{
					return object;
				}
			}
		}
		// lookup for relations
		// !!! also in restricted mode (remember the Proquest master_account_hierarchy!)
		if (identifierType==IdentifierType.DEFAULT || // keep it for compatibility
			identifierType==RELATION) 
		{
			Relation relation = lookupRelation(identifier);
			if (relation!=null) {
				return relation;
			}
		}
		// else
		return super.lookupObject(identifierType, identifier);
	}
	
	@Override
	public Object lookupComposableObject(IdentifierType identifierType,
			String identifier) throws ScopeException {
		// only lookup for relations
		// !!! also in restricted mode (remember the Proquest master_account_hierarchy!)
		if (identifierType==IdentifierType.DEFAULT || identifierType==RELATION) {
			Relation relation = lookupRelation(identifier);
			if (relation!=null) {
				return relation;
			}
		}
		// else
		throw new ScopeException("cannot find composable object '"+identifier+"'");
	}
	
	protected Relation lookupRelation(String identifier) throws ScopeException {
		if (relationScope!=null) {
			for (Relation rel : relationScope) {
				RelationDirection direction = rel.getDirection(domain.getId());
				if (direction==RelationDirection.LEFT_TO_RIGHT && rel.getRightName().compareTo(identifier)==0) {
					// LEFT TO RIGHT
					return rel;
				} else if (direction==RelationDirection.RIGHT_TO_LEFT && rel.getLeftName().compareTo(identifier)==0) {
					return rel;
				} else {
					// ignore
				}
			}
			return null;
		} else {
			return universe.getRelation(domain, identifier);
		}
	}
	
	/**
	 * check if the object is compatible with the identifierType
	 * @param identifierType
	 * @param object
	 * @return
	 * @throws ScopeException
	 */
	private boolean checkIdentifierType(IdentifierType identifierType, ExpressionObject<?> object) throws ScopeException {
		if (identifierType==IdentifierType.DEFAULT) {
			return true;
		} else {
			if (object instanceof Metric && identifierType==METRIC) {
				return true;
			}
			else if (object instanceof Dimension && identifierType==DIMENSION) {
				return true;
			} else {
				return false;
			}
		}
	}

	@Override
	public ExpressionAST createReferringExpression(Object reference) throws ScopeException {
		if (reference instanceof Column) {
			return new ColumnDomainReference(universe, domain, (Column)reference);
		} else if (reference instanceof Axis) {
			return new AxisExpression((Axis)reference);
		} else if (reference instanceof Measure) {
			return new MeasureExpression((Measure)reference);
		} else if (reference instanceof Metric) {
			return new MeasureExpression(universe.S(domain).M((Metric)reference));
		} else if (reference instanceof Dimension) {
			return new AxisExpression(universe.S(domain).A((Dimension)reference));
		} else if (reference instanceof Relation) {
			Relation relation = (Relation)reference;
			return new RelationReference(universe, relation, domain, getRelationTarget(relation));
		} else {
			return super.createReferringExpression(reference);
		}
	}

	private Domain getRelationTarget(Relation relation) throws ScopeException {
		RelationDirection direction = relation.getDirection(domain.getId());
		if (direction==RelationDirection.LEFT_TO_RIGHT) {
			return universe.getRight(relation);
		} else if (direction==RelationDirection.RIGHT_TO_LEFT) {
			return universe.getLeft(relation);
		} else {
			throw new ScopeException("Relation '"+relation.getName()+"' is not applicable to source domain '"+domain.getName()+"'");
		}
	}

	@Override
	public void buildDefinitionList(List<Object> definitions) {
		super.buildDefinitionList(definitions);
		//
		definitions.addAll(getScopeContent());
	}
	
	private List<Object> getScopeContent() {
		ArrayList<Object> content = new ArrayList<Object>();
		if (!restrictedScope && !getSpace().getDomain().isDynamic() && scope==null) {
			//
			HashSet<String> digest = new HashSet<String>();
			//
			// add dimensions
			try {
				Type type = universe.getCartography().computeType(space);
				// exclude SETs but allows MANY_MANY
				if (type.equals(Type.ONE_ONE) || type.equals(Type.MANY_ONE) || type.equals(Type.MANY_MANY)) {
					for (Dimension dimension : getDimensions(true)) {
						content.add(space.A(dimension));
						digest.add(dimension.getId().getDimensionId());
					}
				}
			} catch (ScopeException e) {
				// ignore
			}
			//
			// add metrics
			content.addAll(getMetrics(true));
			//
			// add columns
			if (table!=null && space.getParent()==null) {// list columns only for the home domain
				try {
					String prefix = "dyn_"+getSpace().getDomain().getId().toUUID()+"_dimension:";
					for (Column col : table.getColumns()) {
						ColumnReference ref = new ColumnReference(col);
						String id = DynamicManager.INSTANCE.digest(prefix+ref.prettyPrint());
						if (!digest.contains(id)) {
							content.add(col);
						}
					}
				} catch (ExecutionException e) {
					//ignore
				}
			}
			// add relations
			try {
				List<Space> neighborhood = space.S();
				for (Space neighbor: neighborhood) {
					content.add(neighbor.getRelation());
				}
			} catch (ScopeException | ComputingException e1) {
				//
			}
		} else {
			// list the columns if : editing this domain directly, or the domain is dynamic
			if (table!=null && (space.getParent()==null || space.getDomain().isDynamic())) {// list columns only for the home domain
				try {
					content.addAll(table.getColumns());
				} catch (ExecutionException e) {
					//ignore
				}
			}
			// add relations also in restricted mode (remember the Proquest master_account_hierarchy!)
			// ... but only the non-dynamic ones
			try {
				List<Space> neighborhood = space.S();
				for (Space neighbor: neighborhood) {
					Relation rel = neighbor.getRelation();
					content.add(rel);
				}
			} catch (ScopeException | ComputingException e1) {
				//
			}
		}
		return content;
	}
	
	private List<Dimension> getDimensions(boolean visibleOnly) {
		try {
			List<Dimension> dimensions = space.getDimensions();
			ArrayList<Dimension> filter = new ArrayList<Dimension>();
			for (Dimension dimension : dimensions) {
				if (!dimension.getImageDomain().isInstanceOf(IDomain.OBJECT) && (!visibleOnly || isVisible(dimension))) {
					if (!checkSelf(dimension)) {
						filter.add(dimension);
					}
				}
			}
			return filter;
		} catch (ScopeException e) {
			return Collections.emptyList();
		}
	}
	
	private boolean isVisible(Dimension dimension) {
		return domain.isDynamic() || !dimension.isDynamic();
	}
	
	private Dimension lookupDimension(String identifier) {
		try {
			List<Dimension> dimensions = space.getDimensions();
			for (Dimension dimension : dimensions) {
				if (!checkSelf(dimension)) {
					if (dimension.getName().equals(identifier)) {
						return dimension;
					}
				}
			}
			return null;
		} catch (ScopeException e) {
			return null;
		}
	}
	
	private List<Metric> getMetrics(boolean visibleOnly) {
		try {
			List<Metric> metrics = space.getMetrics();
			ArrayList<Metric> filter = new ArrayList<Metric>();
			for (Metric metric : metrics) {
				if (!visibleOnly || !metric.isDynamic()) {
					if (!checkSelf(metric)) {
						filter.add(metric);
					}
				}
			}
			return filter;
		} catch (ScopeException e) {
			return Collections.emptyList();
		}
	}
	
	/**
	 * check if that object is the scope self main subject
	 * @param object
	 * @return true if that object is self, false otherwise
	 */
	protected boolean checkSelf(Object object) {
		return false;
	}
	
	@Override
	public String prettyPrint(ExpressionAST expression, PrettyPrintOptions options) {
		// check if it is a relation - 
		// in that case add a trailing dot to force applying the current scope
		if (expression instanceof RelationReference) {
			return expression.prettyPrint(options)+".";
		} else {
			return expression.prettyPrint(options);
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
