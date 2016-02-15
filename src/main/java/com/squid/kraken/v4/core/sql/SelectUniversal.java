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
package com.squid.kraken.v4.core.sql;

import java.util.HashMap;

import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.database.model.Column;
import com.squid.core.database.model.Table;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.aggregate.AggregateDomain;
import com.squid.core.domain.analytics.AnalyticDomain;
import com.squid.core.domain.operators.Operators;
import com.squid.core.expression.Compose;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.ExpressionRef;
import com.squid.core.expression.reference.Cardinality;
import com.squid.core.expression.reference.ColumnReference;
import com.squid.core.expression.reference.RelationDirection;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.Context;
import com.squid.core.sql.GroupingInterface;
import com.squid.core.sql.db.render.FromTablePiece;
import com.squid.core.sql.db.statements.DatabaseSelectInterface;
import com.squid.core.sql.model.Aliaser;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.model.Scope;
import com.squid.core.sql.render.IFromPiece;
import com.squid.core.sql.render.IPiece;
import com.squid.core.sql.render.ISelectPiece;
import com.squid.core.sql.render.IWherePiece;
import com.squid.core.sql.render.JoinDecorator;
import com.squid.core.sql.render.JoinDecorator.JoinType;
import com.squid.core.sql.render.OperatorPiece;
import com.squid.core.sql.render.OrderByPiece;
import com.squid.core.sql.render.RenderingException;
import com.squid.core.sql.render.SQLSkin;
import com.squid.core.sql.render.SelectPiece;
import com.squid.core.sql.render.SubSelectReferencePiece;
import com.squid.core.sql.render.WherePiece;
import com.squid.core.sql.render.groupby.IGroupByPiece;
import com.squid.core.sql.statements.SelectStatement;
import com.squid.kraken.v4.core.analysis.engine.project.DynamicColumn;
import com.squid.kraken.v4.core.analysis.engine.project.DynamicTable;
import com.squid.kraken.v4.core.analysis.scope.SpaceExpression;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.database.impl.DatabaseServiceImpl;
import com.squid.kraken.v4.core.database.impl.DatasourceDefinition;
import com.squid.kraken.v4.core.expression.reference.DomainReference;
import com.squid.kraken.v4.core.expression.reference.QueryExpression;
import com.squid.kraken.v4.core.expression.reference.RelationReference;
import com.squid.kraken.v4.core.model.domain.DomainDomainImp;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Relation;


/**
 * knows how to select data from Universe
 *
 */
public class SelectUniversal extends PieceCreator {
	
	private DatabaseSelectInterface select;
	
	private DatasourceDefinition datasource;
	
	private GroupingInterface grouping;
	
	private Analyzer analyzer = null;

	
	class AdaptativeSelectStatement extends SelectStatement {
		
		public AdaptativeSelectStatement() {
			super();
		}

		public AdaptativeSelectStatement(Aliaser aliaser) {
			super(aliaser);
		}
		
		@Override
		protected IGroupByPiece createGroupByPiece() {
			return getGrouping().createGroupByPiece();
		}
		
	}
	public DatasourceDefinition getDatasource() {
		return this.datasource;
	}
	
	public SelectUniversal(Universe universe) throws SQLScopeException {
		super(universe);
		try {
			this.datasource = DatabaseServiceImpl.INSTANCE.getDatasourceDefinition(universe.getProject());
		} catch (DatabaseServiceException e) {
			throw new SQLScopeException(e);
		}

		this.select = new DatabaseSelectInterface() {
			@Override
			protected SelectStatement createStatement(Aliaser aliaser) {
				return new AdaptativeSelectStatement(aliaser);
			}
		};
		this.analyzer = new Analyzer(this);
	}
	
	public SelectUniversal(final SelectUniversal outer) throws SQLScopeException {
		super(outer.getUniverse());
		this.select = new DatabaseSelectInterface(outer.getStatement().getAliaser()) {
			@Override
			protected SelectStatement createStatement(Aliaser aliaser) {
				return new AdaptativeSelectStatement(aliaser);
			}
		};
		this.analyzer = outer.analyzer;
	}
	
	protected SelectUniversal(final SelectUniversal outer, final Scope outerScope) throws SQLScopeException {
		super(outer.getUniverse());
		this.select = new DatabaseSelectInterface(outer.getStatement().getAliaser()) {
			@Override
			protected Scope createMainScope() {
				return outerScope;
			}
			@Override
			protected SelectStatement createStatement(Aliaser aliaser) {
				return new AdaptativeSelectStatement(aliaser);
			}
		};
		this.analyzer = outer.analyzer;
	}
	
	public Analyzer getAnalyzer() {
		return analyzer;
	}

	public void setForceGroupBy(boolean flag) {
		getGrouping().setForceGroupBy(flag);
	}
	
	@Override
	public GroupingInterface getGrouping() {
		if (grouping==null) {
			 grouping = new GroupingInterface(this);
		}
		return grouping;
	}
	
	@Override
	public Scope getScope() {
		return this.select.getScope();
	}
	
	@Override
	public SelectStatement getStatement() {
		return this.select.getStatement();
	}

	/**
	 * add a FROM clause based on the space definition, rooted at the initial scope
	 * @param origin
	 * @return
	 * @throws SQLScopeException
	 * @throws ScopeException
	 */
	public IFromPiece from(Space origin) throws SQLScopeException, ScopeException {
	    return from(Context.SELECT,origin);
	}
	
    public IFromPiece from(Context ctx, Space origin) throws SQLScopeException, ScopeException {
		if (origin.getParent()==null) {
			// ok, this is a root space, just add it to the select if not exists yet (cannot perform a self-cross-product)
			return from(this.select.getScope(),origin.getDomain());
		} else {
			// we will walk to the root, then join the spaces backward...
			Space parent = origin.getParent();
			IFromPiece from = from(parent);
			Relation relation = origin.getRelation();
			return join(ctx,from,relation,parent.getDomain(),origin.getDomain());
		}
	}
	
	@Override
	/**
	 * 
	 */
	public IFromPiece from(Context ctx, Scope scope, ExpressionAST expression)
			throws SQLScopeException, ScopeException {
	    //
	    if (expression instanceof SpaceExpression) {
	        Space space = ((SpaceExpression)expression).getSpace();
	        return from(ctx, space);
	    } else if (expression instanceof DomainReference) {
			Domain domain = ((DomainReference)expression).getDomain();
			Object binding = scope.get(domain);
			if (binding!=null && binding instanceof IFromPiece) {
				return ((IFromPiece)binding);
			} else {
				throw new SQLScopeException("the source domain '"+domain.getName()+"'is not bound");
			}
			//throw new SQLScopeException("Unsupported from-expression: "+expression.toString());
		} else if (expression instanceof RelationReference) {
			RelationReference rr = (RelationReference)expression;
			Relation relation = rr.getRelation();
			if (relation!=null) {
				Domain source = DomainDomainImp.getDomain(rr.getSourceDomain());
				Domain target = DomainDomainImp.getDomain(rr.getImageDomain());
				if (source!=null && target!=null) {
					Object binding = scope.get(source);
					if (binding!=null && binding instanceof IFromPiece) {
						IFromPiece from = (IFromPiece)binding;
						return join(ctx, from, relation, source, target);
					}
				}
			}
			//
			throw new SQLScopeException("Unable to apply from-expression: "+expression.toString());
		} else {
			throw new SQLScopeException("Unsupported from-expression: "+expression.toString());
		}
	}
	
	/**
	 * embed a subselect statement
	 * @param subselect
	 * @return 
	 * @throws SQLScopeException 
	 */
	public FromSelectUniversal from(SelectUniversal subselect) throws SQLScopeException {
		String segmentedAlias = subselect.getStatement().getAliaser().getUniqueAlias();
		FromSelectUniversal fromSt = new FromSelectUniversal(getStatement(), subselect, segmentedAlias);
		this.getStatement().getFromPieces().add(fromSt);
		/*
		if (this.getScope().get(subselect.getMainSubject())==null) {
			this.getScope().put(subselect.getMainSubject(), fromSt);
			this.getScope().put(subselect.getMainSubject().getUnderlyingDatasource(), fromSt);
		}
		*/
		return fromSt;
	}
	
	public ISelectPiece select(IPiece piece) {
		SelectPiece select =  getStatement().createSelectPiece(getScope(), piece, null);
		getStatement().getSelectPieces().add(select);
		return select;
	}
	
	public ISelectPiece select(IPiece piece, String alias) {
		SelectPiece select =  getStatement().createSelectPiece(getScope(), piece, alias);
		getStatement().getSelectPieces().add(select);
		return select;
	}
	
	public ISelectPiece select(ISelectPiece piece) {
		SelectPiece select =  getStatement().createSelectPiece(getScope(), piece, piece.getAlias());
		getStatement().getSelectPieces().add(select);
		return select;
	}
	
	/**
	 * select the given expression in the current scope
	 * @param expr
	 * @return
	 * @throws SQLScopeException 
	 * @throws ScopeException 
	 */
	public ISelectPiece select(ExpressionAST expression) throws SQLScopeException, ScopeException {
		return select(getScope(),expression);
	}
	
	public ISelectPiece select(ExpressionAST expression, String name) throws SQLScopeException, ScopeException {
		return select(getScope(), expression, name, true,true);
	}
	
	public ISelectPiece select(Scope parent, ExpressionAST expression) throws SQLScopeException, ScopeException {
		String baseName = guessExpressionName(expression);
		return select(parent, expression, baseName, true, true);
	}
	
	public ISelectPiece select(Scope parent, ExpressionAST expression, String baseName, boolean useAlias, boolean normalizeAlias) throws SQLScopeException, ScopeException {
		IDomain source = expression.getSourceDomain();
		Object mapping = null;
		if (source.isInstanceOf(IDomain.OBJECT)) {
			// check for availability in the parent scope
			Object object = source.getAdapter(Domain.class);
			if (object!=null && object instanceof Domain) {
				mapping = parent.get(((Domain)object));
			} else {
				object = source.getAdapter(Table.class);
				if (object!=null && object instanceof Table) {
					mapping = parent.get((Table)object);
				}
			}
		}
		if (mapping==null && !source.equals(IDomain.NULL)) { // null domain will be automatically bound to the main scope
			source = expression.getSourceDomain();
			throw new SQLScopeException("the source domain is not bound");
		}
		//
		// check if the expression is already available in the scope
		// SFA: that cause too much side-effect especially with constant expression.
		/*
		Object binding = parent.get(expression);
		if (binding instanceof IPiece) {
			Scope scope = parent.getDefiningScope(expression);
			if (scope==parent) {
				if (binding instanceof ISelectPiece) {
					return (ISelectPiece)binding;
				}
			}
		}
		*/
		//
		// ...else create it
		IPiece piece = createPiece(Context.SELECT, parent, expression);
		String reference = getSqlStyleReferences()?baseName:null;
		SelectPiece select =  getStatement().createSelectPiece(parent, piece, reference, useAlias, normalizeAlias);
		// 
		// it's ok to map the same expression to different column
		//
		// bound the expression
		parent.override(expression, select);
		// bound the IPiece in the MAIN scope -- it must be possible to find it from the statement root
		getScope().put(select, expression);
		getStatement().getSelectPieces().add(select);
		return select;
	}
	
	protected String guessExpressionName(ExpressionAST expression) {
		if (expression instanceof ExpressionRef) {
			ExpressionRef ref = (ExpressionRef)expression;
			return ref.getReferenceName();
		}
		if (expression instanceof Compose) {
			Compose compose = (Compose)expression;
			return guessExpressionName(compose.getHead());
		}
		//
		return null;
	}
	
	private IFromPiece from(Scope parent, Domain domain) throws SQLScopeException, ScopeException {
		Object binding = parent.get(domain);
		if (binding==null) {
			Table table = getUniverse().getTableSafe(domain);
			if (!(table instanceof DynamicTable)) {
				IFromPiece from = from(parent,domain,table);
				parent.put(domain, from);
				return from;
			} else {
				DynamicTable dyn = (DynamicTable)table;
				return from(parent, domain, dyn);
			}
		} else {
			if (binding instanceof IFromPiece) {
				return (IFromPiece)binding;
			} else {
				throw new SQLScopeException("invalid binding for domain '"+domain.getName()+"'");
			}
		}
	}
	
	/**
	 * generalized createFromPiece to support DynamicTable; must work with both from(),createInnerJoin() and createDecoratedJoin()
	 * @param parent
	 * @param domain
	 * @return
	 * @throws SQLScopeException
	 * @throws ScopeException
	 */
	private IFromPiece createFromPieceNew(Scope parent, Domain domain) throws SQLScopeException, ScopeException {
		Table table = getUniverse().getTableSafe(domain);
		if (table instanceof DynamicTable) {
			DynamicTable dyn = (DynamicTable)table;
			return createFromPieceNew(parent, domain, dyn);
		} else {
			FromDomainPiece from = new FromDomainPiece(this,parent,domain,table,getStatement().getAliaser().getUniqueAlias());
			return from;
		}
	}

	/**
	 * support selecting from a DynamicTable == SubQuery
	 * @param parent
	 * @param domain
	 * @param table
	 * @return
	 * @throws SQLScopeException
	 * @throws ScopeException
	 */
	private IFromPiece fromOLD(Scope parent, Domain domain, DynamicTable table) throws SQLScopeException, ScopeException {
		QueryExpression query = table.getLineage();
		// let's have fun...
		// for now we will assume the table is always virtual
		// first create the subselect
		SelectUniversal subselect = new SelectUniversal(this);
		subselect.from(subselect.getScope(), query.getSubject().getDomain());
		FromSelectUniversal from = from(subselect);
		// add filters if any (this is not encoded in the Table)
		for (ExpressionAST filter : query.getFilters()) {
			subselect.where(filter);
		}
		// now compute the virtual table by using the dynamicColumn's lineage
		for (Column col : table.getColumns()) {
			// the column should be a DynamicColumn
			if (col instanceof DynamicColumn) {
				DynamicColumn dyn = (DynamicColumn)col;
				ISelectPiece piece = subselect.select(dyn.getLineage(), dyn.getName());
				// map the definition in the outer scope
				SubSelectReferencePiece ref = new SubSelectReferencePiece(from, piece);
				ColumnReference colref = new ColumnReference(col);
				subselect.getScope().put(colref, ref);// this is to allow joining with subselect
				getScope().put(colref, ref);// this is to import the selected column in outer scope
			} else {
				// error...
			}
		}
		// register the subselect in the parent scope
		parent.put(domain, from);
		parent.put(table, from);
		return from;
	}
	
	private IFromPiece from(Scope parent, Domain domain, DynamicTable table) throws SQLScopeException, ScopeException {
		IFromPiece from = createFromPieceNew(parent, domain, table);
		select.from(parent, table, from);
		// register the subselect in the parent scope
		parent.put(domain, from);
		//parent.put(table, from);
		return from;
	}
	
	/**
	 * This is the new createFromPiece to select a DynamicTable
	 * @param parent
	 * @param domain
	 * @param table
	 * @return
	 * @throws SQLScopeException
	 * @throws ScopeException
	 */
	private IFromPiece createFromPieceNew(Scope parent, Domain domain, DynamicTable table) throws SQLScopeException, ScopeException {
		QueryExpression query = table.getLineage();
		// let's have fun...
		// for now we will assume the table is always virtual
		// first create the subselect
		SelectUniversal subselect = new SelectUniversal(this);
		subselect.from(subselect.getScope(), query.getSubject().getDomain());
		// wrap the subselect into a FROM piece
		String segmentedAlias = subselect.getStatement().getAliaser().getUniqueAlias();
		FromSelectUniversal from = new FromSelectUniversal(getStatement(), subselect, segmentedAlias);
		// add filters if any (this is not encoded in the Table)
		for (ExpressionAST filter : query.getFilters()) {
			subselect.where(filter);
		}
		// now compute the virtual table by using the dynamicColumn's lineage
		for (Column col : table.getColumns()) {
			// the column should be a DynamicColumn
			if (col instanceof DynamicColumn) {
				DynamicColumn dyn = (DynamicColumn)col;
				ISelectPiece piece = subselect.select(dyn.getLineage(), dyn.getName());
				// map the definition in the outer scope
				SubSelectReferencePiece ref = new SubSelectReferencePiece(from, piece);
				ColumnReference colref = new ColumnReference(col);
				subselect.getScope().put(colref, ref);// this is to allow joining with subselect
				parent.put(colref, ref);// this is to import the selected column in outer scope
			} else {
				// error...
			}
		}
		return from;
	}

	private IFromPiece from(Scope parent, Domain domain, Table table) throws SQLScopeException {
		FromDomainPiece from = new FromDomainPiece(this,parent,domain,table,getStatement().getAliaser().getUniqueAlias());
		select.from(parent, table, from);
		parent.put(domain,from);
		return from;
	}
	
	protected IFromPiece createFromPieceOLD(Scope parent, Domain domain) throws SQLScopeException, ScopeException {
		Table table = getTable(domain);
		FromDomainPiece from = new FromDomainPiece(this,parent,domain,table,getStatement().getAliaser().getUniqueAlias());
		parent.put(table, from);
		parent.put(domain,from);
		return from;
	}
	
	/**
	 * now supporting DynamicTable
	 * @param parent
	 * @param domain
	 * @return
	 * @throws SQLScopeException
	 * @throws ScopeException
	 */
	protected IFromPiece createFromPiece(Scope parent, Domain domain) throws SQLScopeException, ScopeException {
		return createFromPieceNew(parent, domain);
	}

	private IFromPiece join(Context ctx, IFromPiece from, Relation relation, Domain source, Domain target) throws SQLScopeException, ScopeException {
		Object binding = from.getScope().get(relation);
		if (binding==null) {
			RelationDirection direction = relation.getDirection(source.getId());
			if (direction==RelationDirection.NO_WAY) {
				throw new ScopeException("Relation '"+relation.getName()+"' is not applicable to source domain '"+source.getName()+"'");
			}
			Cardinality sourceCardinality = (direction==RelationDirection.LEFT_TO_RIGHT)?relation.getLeftCardinality():relation.getRightCardinality();
			Cardinality targetCardinality = (direction==RelationDirection.LEFT_TO_RIGHT)?relation.getRightCardinality():relation.getLeftCardinality();
			JoinType type = computeJoinType(sourceCardinality, targetCardinality);
			//
			// krkn-94: do not change join-type if ctx is WHERE
			//
			// check if we must enforce a natural join => when the from definition is in a different statement
			boolean natural = from.getStatement()!=getStatement();
			IFromPiece to = natural?createNaturalJoin(from, relation, source, target):createJoin(ctx, type, from, relation, source, target);
			from.getScope().put(relation, to);
			to.getScope().put(relation, from);// opposite link
			return to;
		} else {
			// follow existing relation
			if (binding instanceof IFromPiece) {
				return (IFromPiece)binding;
			} else {
				throw new SQLScopeException("ivalid binding for relation '"+relation.getLeftName()+"'");
			}
		}
	}
	
	private IFromPiece createJoin(Context ctx, JoinType type, IFromPiece from, Relation relation, Domain source, Domain target) throws SQLScopeException, ScopeException {
		switch (type) {
		case LEFT:
			return createLeftJoin(ctx, from, relation, source, target);
		case INNER:
			return createInnerJoin(ctx, from, relation, source, target);
		case RIGHT:
			return createRightJoin(ctx, from, relation, source, target);
		default:
			throw new RuntimeException("Unsupported join type");
		}
	}
	
	private JoinType computeJoinType(Cardinality sourceCardinality, Cardinality targetCardinality) {
		switch (targetCardinality) {
		case ZERO_OR_ONE:
			return JoinType.LEFT;
		case ONE:
			return JoinType.INNER;
		case MANY:
			return JoinType.LEFT;
		default:
			throw new RuntimeException("Unexpected cardinality definition");
		}
	}

	// create a simple natural join
	private IFromPiece createNaturalJoin(IFromPiece from, Relation relation, Domain source, Domain target) throws SQLScopeException, ScopeException {
		// just add the target as a regular from
		Scope subscope = new Scope(from.getScope());
		IFromPiece targetFromPiece = from(subscope,target);
		//IFromPiece targetFromPiece = createFromPiece(subscope,target);
		//getStatement().getFromPieces().add(targetFromPiece);
		//
		// define the join condition
		Scope joinScope = new Scope();
		joinScope.put(source, from);
		joinScope.put(getUniverse().getTable(source), from);// register both the domain and the table
		joinScope.put(target, targetFromPiece);
		joinScope.put(getUniverse().getTable(target), targetFromPiece);// register both the domain and the table
		//
		ExpressionAST conditionExpression = getUniverse().getParser().parse(relation);
		where(joinScope, conditionExpression);
		//
		if (analyzer!=null) {
			analyzer.analyzeRelation(joinScope, conditionExpression);
		}
		//
		return targetFromPiece;
	}
	
	private IFromPiece createRightJoin(Context ctx, IFromPiece from, Relation relation, Domain source, Domain target) throws SQLScopeException, ScopeException {
		return createDecoratedJoin(ctx, from, JoinType.RIGHT, relation, source, target);
	}
	
	private IFromPiece createLeftJoin(Context ctx, IFromPiece from, Relation relation, Domain source, Domain target) throws SQLScopeException, ScopeException {
		return createDecoratedJoin(ctx, from, JoinType.LEFT, relation, source, target);
	}
	
	private IFromPiece createInnerJoin(Context ctx, IFromPiece from, Relation relation, Domain source, Domain target) throws SQLScopeException, ScopeException {
		return createDecoratedJoin(ctx, from, JoinType.INNER, relation, source, target);
	}
	
	private IFromPiece createDecoratedJoin(Context ctx, IFromPiece from, JoinType joinType, Relation relation, Domain source, Domain target) throws SQLScopeException, ScopeException {
		// just add the target and link
		Scope subscope = new Scope(from.getScope());
		IFromPiece targetFromPiece = createFromPiece(subscope,target);
		subscope.put(target, targetFromPiece);
		if (targetFromPiece instanceof FromTablePiece) {
			FromTablePiece targetFromTable = (FromTablePiece)targetFromPiece;
			subscope.put(targetFromTable.getTable(), targetFromPiece);
			if (joinType==JoinType.LEFT) {
				((FromTablePiece)targetFromPiece).setDense(false);// LEFT JOIN is NOT DENSE, i.e. it may return NULL records
			}
		}
		//
		Scope joinScope = new Scope();
		joinScope.put(source, from);
		joinScope.put(getUniverse().getTable(source), from);// register both the domain and the table
		joinScope.put(target, targetFromPiece);
		joinScope.put(getUniverse().getTable(target), targetFromPiece);// register both the domain and the table
		//
		ExpressionAST conditionExpression = getUniverse().getParser().parse(relation);
		IPiece conditionPiece = createPiece(ctx, joinScope, conditionExpression);
		//
		JoinDecorator decorator = new JoinDecorator(joinType,targetFromPiece,new WherePiece(conditionPiece));
		from.addJoinDecorator(decorator);
		//
		if (analyzer!=null /*&& (joinType==JoinType.INNER)*/) {
			analyzer.analyzeRelation(joinScope, conditionExpression);
		}
		//
		return targetFromPiece;
	}
	
	public IWherePiece where(ExpressionAST filter) throws SQLScopeException, ScopeException {
		return where(getScope(),filter);
	}
	
	public IWherePiece where(Scope parent, ExpressionAST filter) throws SQLScopeException, ScopeException {
		IDomain image = filter.getImageDomain();
		if (!image.isInstanceOf(IDomain.CONDITIONAL)) {
			@SuppressWarnings("unused")
			IDomain try_again = filter.getImageDomain();
			throw new SQLScopeException("Invalid WHERE expression: it must be a condition ");
		}
		IPiece piece = createPiece(Context.WHERE, parent, filter);// must use the local scope
		IWherePiece where = select.addWherePiece(parent,piece);
		// add support for HAVING and QUALIFY 
		//
		if (image.isInstanceOf(AggregateDomain.DOMAIN)) {
			where.setType(IWherePiece.HAVING);
		} else if (image.isInstanceOf(AnalyticDomain.DOMAIN)) {
			where.setType(IWherePiece.QUALIFY);
		}
		return where;
	}

	public OrderByPiece orderBy(IPiece piece) {
		return this.select.orderBy(piece);
	}
	
	public String render() throws RenderingException {
		if (analyzer!=null) {
			Analyzer copy = analyzer;
			this.analyzer = null;
			copy.apply();
		}
		return this.select.render(getSkin());
	}

	/**
	 * internal method we use when rendering a subselect
	 * @param skin
	 * @return
	 * @throws RenderingException
	 */
	protected String render(SQLSkin skin) throws RenderingException {
		return this.select.render(skin);
	}

    private HashMap<ExpressionAST, SelectUniversal> exists = new HashMap<>();
    
    public SelectUniversal exists(ExpressionAST root) throws ScopeException, SQLScopeException {
        //
        SelectUniversal subselect = exists.get(root);
        if (subselect==null) {
            subselect = createExistsStatement();
            exists.put(root, subselect);
        }
        return subselect;
    }

	/**
	 * Exists will try to create a sub-select linked to the outer (this) select through the space
	 * that you can use to add where statements
	 * @return
	 * @throws SQLScopeException 
	 * @throws ScopeException 
	 */
	protected SelectUniversal createExistsStatement() throws SQLScopeException, ScopeException {
		// check that the space link is already referenced in outer==this
		//IFromPiece from = from(link);
		SelectUniversal inner = new SelectUniversal(this, this.getScope());
		inner.getStatement().addComment("EXISTS");
		// add some constant in the select
		inner.select(ExpressionMaker.CONSTANT(1));
		// insert the exists operator in the outer statement
		IPiece[] pieces = new IPiece[]{new SubSelectPiece(inner)};
		OperatorPiece exists = new OperatorPiece(Operators.EXISTS,pieces);
		this.getStatement().getConditionalPieces().add(new WherePiece(exists));
		// done
		return inner;
	}
	
	@Override
	public String toString() {
	    try {
            return render();
        } catch (RenderingException e) {
            return e.toString();
        }
	}

}
