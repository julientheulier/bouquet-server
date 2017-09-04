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
package com.squid.kraken.v4.core.analysis.engine.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.squid.core.database.model.Column;
import com.squid.core.database.model.ForeignKey;
import com.squid.core.database.model.KeyPair;
import com.squid.core.database.model.Table;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.analytics.AnalyticDomain;
import com.squid.core.domain.operators.IntrinsicOperators;
import com.squid.core.domain.operators.OperatorScope;
import com.squid.core.domain.operators.OrderedAnalyticOperatorDefinition;
import com.squid.core.domain.set.SetDomain;
import com.squid.core.domain.vector.VectorDomain;
import com.squid.core.expression.Compose;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.ExpressionRef;
import com.squid.core.expression.Operator;
import com.squid.core.expression.reference.ColumnReference;
import com.squid.core.expression.reference.ForeignKeyReference;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.Context;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.model.Scope;
import com.squid.core.sql.render.IFromPiece;
import com.squid.core.sql.render.IPiece;
import com.squid.core.sql.render.ISelectPiece;
import com.squid.core.sql.render.IWherePiece;
import com.squid.core.sql.render.OperatorPiece;
import com.squid.core.sql.render.SubSelectReferencePiece;
import com.squid.core.sql.render.WherePiece;
import com.squid.core.sql.statements.FromSelectStatementPiece;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.AxisMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.ContinuousAxisMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.MeasureMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.SimpleMapping;
import com.squid.kraken.v4.core.analysis.engine.query.rollup.IRollupStrategy;
import com.squid.kraken.v4.core.analysis.engine.query.rollup.RollupStrategySelector;
import com.squid.kraken.v4.core.analysis.model.GroupByAxis;
import com.squid.kraken.v4.core.analysis.model.OrderBy;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.reference.RelationReference;
import com.squid.kraken.v4.core.sql.FromSelectUniversal;
import com.squid.kraken.v4.core.sql.SelectUniversal;
import com.squid.kraken.v4.core.sql.script.SQLScript;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Relation;

/**
 * this is the Query for computing analysis, that manages measures, axes and
 * filters KRKN-59: now also support rollup
 *
 * @author sfantino
 *
 */
public class SimpleQuery extends BaseQuery {

	private Domain subject;

	// krkn-59: rollup support
	private List<GroupByAxis> rollup = null;
	private boolean rollupGrandTotal = false;
	private GroupByAxis precomputedRollupAxis = null;

	private boolean isAnalytics;

	public SimpleQuery(Space subject) throws ScopeException, SQLScopeException {
		super(subject.getUniverse(), subject.getRoot());
		this.subject = subject.getRoot();
	}

	public SimpleQuery(Universe universe, Domain subject, SelectUniversal select) throws SQLScopeException, ScopeException {
		super(universe, select);
		this.subject = subject;
	}

	public Domain getSubject() {
		return subject;
	}

	public void select(Measure measure, ExpressionAST expr) throws SQLScopeException, ScopeException {
		try {
			if (expr.getImageDomain().isInstanceOf(AnalyticDomain.DOMAIN)) {
				isAnalytics = true;
			}
			ISelectPiece piece = select.select(expr, measure.getName());
			String name = measure.getName();
			piece.addComment(name + " (Metric)");
			setComment("\ncomputing KPI '" + name + "'");
			select.getScope().put(measure, expr);// register the measure in the
			// select context
			MeasureMapping kx = new MeasureMapping(piece, measure);
			add(kx);
		} catch (ScopeException e) {
			throw new ScopeException(
					"error while parsing Metric '" + measure.getName() + "'\ncaused by: " + e.getLocalizedMessage(), e);
		} catch (SQLScopeException e) {
			throw new ScopeException(
					"error while using Metric '" + measure.getName() + "'\ncaused by: " + e.getLocalizedMessage(), e);
		}
	}

	public ISelectPiece select(Axis axis) throws ScopeException, SQLScopeException {
		try {
			// check axis
			Domain a_domain = axis.getParent().getRoot();
			if (!this.subject.equals(a_domain)) {
				throw new ScopeException("cannot select '" + axis.prettyPrint() + "': domain '" + a_domain.getName()
				+ "' does not match select domain '" + this.subject.getName() + "'");
			}
			ExpressionAST expr = axis.getDefinition();
			IDomain image = expr.getImageDomain();
			if (image.isInstanceOf(SetDomain.DOMAIN)) {
				// throw new ScopeException("unsupported operation: cannot slice
				// downward using axis: "+axis.toString());
			}
			if (image.isInstanceOf(VectorDomain.DOMAIN)) {
				throw new SQLScopeException(
						"cannot select '" + axis.getDimension().getName() + "': unsupported VECTOR expression");
			}
			/*
			 * JTH 20170524 : remove this ugly Nautilus thing
			 * https://phab.squidsolutions.com/T3110
			// ticket:3014 - handles predicate as case(P,true,null): we don't
			// want to index false values
			if (image.isInstanceOf(IDomain.CONDITIONAL)) {
				// transform expr into case(expr,true,null)
				expr = ExpressionMaker.CASE(expr, ExpressionMaker.TRUE(), ExpressionMaker.NULL());
			}
			 */
			//
			if (image.isInstanceOf(IDomain.OBJECT)) {
				// ok, try to identify the foreign-key...
				expr = extractRelation(axis, expr);
			}
			if (image.isInstanceOf(AnalyticDomain.DOMAIN)) {
				throw new SQLScopeException(
						"cannot select '" + axis.getName() + "' as a groupBy: must be selected as a metric");
			}
			AxisMapping ax = getMapper().find(axis);
			if (ax != null) {// if there is already a slice, discard it
				return ax.getPiece();
			} else {
				return selectDimension(axis, expr);
			}
		} catch (ScopeException e) {
			throw new ScopeException(
					"error while parsing Dimension '" + axis.getName() + "'\ncaused by: " + e.getLocalizedMessage(), e);
		}
	}

	private ExpressionAST extractRelation(Axis axis, ExpressionAST expr) throws ScopeException, SQLScopeException {
		if (expr instanceof RelationReference) {
			Relation rel = ((RelationReference) expr).getRelation();
			Column exported = getExportedKey(axis, rel);
			return new ColumnReference(exported);
		}
		if (expr instanceof Compose) {
			Compose compose = (Compose) expr;
			ExpressionAST exported = extractRelation(axis, compose.getHead());
			return new Compose(compose.getTail(), exported);// relink exported
			// key
		} else {
			throw new SQLScopeException("Domain dimension not supported: " + axis.getName());
		}
	}

	private Column getExportedKey(Axis axis, Relation rel) throws ScopeException, SQLScopeException {
		ExpressionAST join = axis.getParent().getUniverse().getParser().parse(rel);
		if (join instanceof ForeignKeyReference) {
			ForeignKey fk = ((ForeignKeyReference) join).getForeignKey();
			if (fk.getKeys().size() != 1) {
				throw new SQLScopeException("Domain dimension not supported: " + axis.getName());
			}
			KeyPair first = fk.getKeys().get(0);
			Column exported = first.getExported();
			return exported;
		} else {
			// need to try harder...
			throw new SQLScopeException("Domain dimension not supported: " + axis.getName());
		}
	}

	protected ISelectPiece selectDimension(Axis axis, ExpressionAST expr) throws ScopeException, SQLScopeException {
		ISelectPiece piece = select.select(expr, axis.getName());
		select.getScope().put(axis, expr);// register the axis in the select
		// context
		AxisMapping ax = new AxisMapping(piece, axis);
		add(ax);
		//
		piece.addComment(axis.getName() + " (Dimension)");
		return piece;
	}

	public List<ISelectPiece> selectIntervalle(Axis axis, ExpressionAST expr) throws ScopeException, SQLScopeException {
		ExpressionAST min = ExpressionMaker.MIN(expr);// universe.measure("MIN("+axis.prettyPrint()+")");
		ExpressionAST max = ExpressionMaker.MAX(expr);// universe.measure("MAX("+axis.prettyPrint()+")");
		ISelectPiece select_min = select.select(min);
		ISelectPiece select_max = select.select(max);
		ContinuousAxisMapping ax = new ContinuousAxisMapping(axis, new SimpleMapping(select_min),
				new SimpleMapping(select_max));
		add(ax);
		ArrayList<ISelectPiece> result = new ArrayList<ISelectPiece>();
		result.add(select_min);
		select_min.addComment("Filtering on " + axis.getName());
		result.add(select_max);
		return result;
	}

	@Override
	public void where(Axis axis, Collection<DimensionMember> filters) throws ScopeException, SQLScopeException {
		// check axis
		Domain a_domain = axis.getParent().getRoot();
		if (!this.subject.equals(a_domain)) {
			throw new ScopeException("cannot select '" + axis.getDimension().getName() + "': domain '"
					+ a_domain.getName() + "'does not match select ('" + this.subject.getName() + "')");
		}
		IDomain image = axis.getDefinition().getImageDomain();
		if (image.isInstanceOf(SetDomain.DOMAIN)) {
			// check if the axis is already selected
			SpaceScope selected = isSelected(axis.getParent());
			if (selected != null) {
				// prune the axis
				Axis prune = axis.prune(selected.space);
				IDomain prune_image = prune.getDefinition().getImageDomain();
				if (prune_image.isInstanceOf(SetDomain.DOMAIN)) {
					// TODO:
					super.exists(axis, filters);
				} else {
					// no need to use exists
					super.where(axis, filters);
				}
			} else {
				// we need to use EXISTS operator
				super.exists(axis, filters);
			}
		} else {
			super.where(axis, filters);
		}
	}

	/**
	 * test if the space is already selected, i.e. included in the FROM
	 * statement; if so will return the selected part as a Space
	 *
	 * @param space
	 * @return
	 */
	private SpaceScope isSelected(Space space) {
		if (space.getParent() == null) {
			// ok, let's check the domain
			Object rel = this.select.getScope().get(space.getDomain());
			if (rel != null && rel instanceof IFromPiece) {
				IFromPiece from = (IFromPiece) rel;
				return new SpaceScope(space, from.getScope());
			} else {
				// domain mismatch
				return null;
			}
		} else {
			// check parent
			SpaceScope check = isSelected(space.getParent());
			if (check == null) {
				return null;
			} else if (check.space.equals(space)) {
				// completely selected
				return check;
			} else {
				// check incoming relation
				Object rel = check.scope.get(space.getRelation());
				if (rel != null && rel instanceof IFromPiece) {
					IFromPiece from = (IFromPiece) rel;
					return new SpaceScope(space, from.getScope());
				} else {
					// cannot go further
					return check;
				}
			}
		}
	}

	class SpaceScope {
		public Space space;
		public Scope scope;

		public SpaceScope(Space space, Scope scope) {
			super();
			this.space = space;
			this.scope = scope;
		}
	}

	public FromSelectStatementPiece join(Axis axis, SimpleQuery inner) throws SQLScopeException, ScopeException {
		FromSelectStatementPiece from = select.from(inner.select);
		//
		AxisMapping m = inner.getMapper().find(axis);
		if (m != null) {
			ISelectPiece source = m.getPiece();
			ExpressionAST expr = axis.getDefinition();// ExpressionResolver.resolve(select.getMainSubject(),x);
			IPiece target = select.createPiece(Context.WHERE, expr);
			//
			// compute the join
			IPiece[] p = new IPiece[2];
			p[0] = new SubSelectReferencePiece(from, source);
			p[1] = target;
			IPiece where = new OperatorPiece(OperatorScope.getDefault().lookupByID(IntrinsicOperators.EQUAL), p);
			select.getStatement().getConditionalPieces().add(new WherePiece(where));
			//
			// import the inner measure in the outer scope
			for (MeasureMapping mm : inner.getMapper().getMeasureMapping()) {
				SubSelectReferencePiece reference_piece = new SubSelectReferencePiece(from, mm.getPiece());
				ExpressionAST mexpr = (ExpressionAST) inner.select.getScope().get(mm.getMapping());
				select.getScope().put(mexpr, reference_piece);
			}
			//
			return from;
		} else {
			throw new SQLScopeException("cannot join the measure using that axis");
		}
	}

	/**
	 * join this query with the inner SimpleQuery based on the axes equi- join
	 *
	 * @param axes
	 * @param inner
	 * @return
	 * @throws SQLScopeException
	 * @throws ScopeException
	 */
	public FromSelectStatementPiece join(List<Axis> axes, SimpleQuery inner) throws SQLScopeException, ScopeException {
		FromSelectStatementPiece from = select.from(inner.select);
		//
		for (Axis axis : axes) {
			AxisMapping m = inner.getMapper().find(axis);
			if (m != null) {
				ISelectPiece source = m.getPiece();
				ExpressionAST expr = axis.getDefinition();// ExpressionResolver.resolve(select.getMainSubject(),x);
				IPiece target = select.createPiece(Context.WHERE, expr);
				//
				// compute the join
				IPiece[] p = new IPiece[2];
				p[0] = new SubSelectReferencePiece(from, source);
				p[1] = target;
				IPiece where = new OperatorPiece(OperatorScope.getDefault().lookupByID(IntrinsicOperators.EQUAL), p);
				select.getStatement().getConditionalPieces().add(new WherePiece(where));
			} else {
				throw new SQLScopeException("cannot join the queries using that axis");
			}
		}
		//
		return from;
	}

	public void select(Measure measure) throws ScopeException, SQLScopeException {
		select(measure, measure.getDefinition());
	}

	/**
	 * define a rollup
	 *
	 * @param axis
	 * @throws ScopeException
	 * @throws SQLScopeException
	 */
	public void rollUp(List<GroupByAxis> axis, boolean grandTotal) throws ScopeException, SQLScopeException {
		if (rollup == null) {
			rollup = new ArrayList<>();
		}
		rollupGrandTotal = grandTotal;
		for (GroupByAxis groupBy : axis) {
			// check
			ExpressionAST expr = groupBy.getAxis().getDefinition();
			IDomain source = expr.getSourceDomain();
			Object mapping = null;
			if (source.isInstanceOf(IDomain.OBJECT)) {
				// check for availability in the parent scope
				Object object = source.getAdapter(Domain.class);
				if (object != null && object instanceof Domain) {
					mapping = select.getScope().get((object));
				} else {
					object = source.getAdapter(Table.class);
					if (object != null && object instanceof Table) {
						mapping = select.getScope().get(object);
					}
				}
			}
			if (mapping == null && !source.equals(IDomain.NULL)) { // null
				// domain
				// will be
				// automatically
				// bound to
				// the main
				// scope
				throw new SQLScopeException("the source domain is not bound");
			}
			//
			rollup.add(groupBy);
		}
	}

	@Override
	public SQLScript generateScript() throws SQLScopeException {
		if (isAnalytics) {
			if (rollupGrandTotal || (rollup != null && !rollup.isEmpty())) {
				throw new SQLScopeException("mixing rollup and analytics feature is not supported");
			}
			return generateQualifyScript();
			// krkn-59: rollup support
		} else if (precomputedRollupAxis!= null || rollupGrandTotal || (rollup != null && !rollup.isEmpty())) {
			IRollupStrategy strategy = RollupStrategySelector.selectStrategy(this, select, rollup, rollupGrandTotal,
					getMapper());
			return strategy.generateScript();
		} else {
			// just use the plain select
			return super.generateScript();
		}
	}

	@Override
	protected SQLScript generateQualifyScript() throws SQLScopeException {
		try {
			// create a groub-by-free subselect where we can evaluate the analytics functions
			// this one is derived from this.select statement
			SelectUniversal inner = new SelectUniversal(getUniverse());
			inner.from(getUniverse().S(getSubject()));
			// create the outer select
			SelectUniversal main = new SelectUniversal(getUniverse());
			FromSelectUniversal from = main.from(inner);
			main.getScope().put(getSubject(), from);
			// add the axis
			for (AxisMapping ax : getMapper().getAxisMapping()) {
				ISelectPiece piece = inner.select(ax.getAxis().getDefinition(),ax.getPiece().getAlias());
				SubSelectReferencePiece ref = new SubSelectReferencePiece(from, piece);
				// make sure to use the same alias
				ISelectPiece sel = main.select(ref, ax.getPiece().getAlias());
				ax.setPiece(sel);// update the mapping... that's dangerous!
			}
			// handle the QUALIFY conditions
			for (ExpressionAST condition : getConditions()) {
				if (condition.getImageDomain().isInstanceOf(AnalyticDomain.DOMAIN)) {
					try {
						ExpressionAST rewrite = rewriteQualifyCondition(condition, main, from);
						IWherePiece where = main.where(rewrite);
						// override QUALIFY
						where.setType(IWherePiece.WHERE);
						where.addComment("QUALIFY clause: " + condition.prettyPrint());
					} catch (SQLScopeException | ScopeException e) {
						throw new SQLScopeException("cannot rewrite QuUALIFY condition: " + condition.prettyPrint()
						+ " caused by:\n" + e.getLocalizedMessage());
					}
				} else if (condition.getImageDomain().isInstanceOf(IDomain.AGGREGATE)) {
					IWherePiece where = main.where(condition);
					// override QUALIFY
					where.setType(IWherePiece.HAVING);
					where.addComment("HAVING clause: " + condition.prettyPrint());
				} else {
					inner.where(condition);
				}
			}
			// standard filters
			for (Filter filter : getFilters()) {
				filter.applyFilter(inner);
			}
			// select the measures
			for (MeasureMapping mx : getMapper().getMeasureMapping()) {
				ExpressionAST measure = mx.getMapping().getDefinition();
				if (measure.getImageDomain().isInstanceOf(AnalyticDomain.DOMAIN)) {
					try {
						ExpressionAST rewrite = rewriteQualifyCondition(measure, main, from);
						ISelectPiece piece = main.select(rewrite, mx.getPiece().getAlias());
						mx.setPiece(piece);// update the mapping... that's dangerous!
					} catch (SQLScopeException | ScopeException e) {
						throw new SQLScopeException("cannot rewrite analytics measure: " + measure.prettyPrint()
						+ " caused by:\n" + e.getLocalizedMessage());
					}
				} else {
					ISelectPiece piece = main.select(mx.getMapping().getDefinition(),mx.getPiece().getAlias());
					piece.addComment(mx.getMapping().getName() + " (Metric)");
					mx.setPiece(piece);// update the mapping... that's dangerous!
				}
			}
			// orderBy
			// -- resubmit the orderBy list
			ArrayList<OrderBy> copy = new ArrayList<>(getOrderBy());
			getOrderBy().clear();
			orderBy(copy, getMapper(), main);
			// limit & offset
			if (select.getStatement().hasLimitValue()) {
				main.getStatement().setLimitValue(select.getStatement().getLimitValue());
				main.getStatement().setOffsetValue(select.getStatement().getOffsetValue());
			}
			//
			return new SQLScript(main, getMapper());
		} catch (ScopeException e) {
			throw new SQLScopeException("Failed to generate the QUALIFY clause, caused by:\n" + e.getLocalizedMessage(),
					e);
		}
	}

	protected SQLScript generateQualifyScriptDeprecated() throws SQLScopeException {
		try {
			//
			SelectUniversal main = new SelectUniversal(getUniverse());
			FromSelectUniversal from = main.from(select);
			from.getWrapperSelectInterface().getGrouping().setForceGroupBy(false);// cannot use group-by!!!
			main.getStatement().addComment(select.getStatement().getComment());
			main.getStatement().addComment("!! QUALIFY strategy: external query to filter on analytic features");
			select.getStatement().addComment("!! QUALIFY strategy: compute analytic features");
			// select the dimensions
			for (AxisMapping ax : getMapper().getAxisMapping()) {
				SubSelectReferencePiece pieceRef = new SubSelectReferencePiece(from, ax.getPiece());
				ISelectPiece piece = main.select(pieceRef, ax.getPiece().getAlias());// make
				// sure
				// to
				// use
				// the
				// same
				// alias
				piece.addComment("copy of " + ax.getAxis().getName() + " (Dimension)");
				ax.setPiece(piece);// update the mapping... that's dangerous!
			}
			// handle the QUALIFY conditions
			for (ExpressionAST condition : getConditions()) {
				if (condition.getImageDomain().isInstanceOf(AnalyticDomain.DOMAIN)) {
					try {
						ExpressionAST rewrite = rewriteQualifyCondition(condition, main, from);
						IWherePiece where = main.where(rewrite);
						// override QUALIFY
						where.setType(IWherePiece.WHERE);
						where.addComment("QUALIFY clause: " + condition.prettyPrint());
					} catch (SQLScopeException | ScopeException e) {
						throw new SQLScopeException("cannot rewrite QuUALIFY condition: " + condition.prettyPrint()
						+ " caused by:\n" + e.getLocalizedMessage());
					}
				}
			}
			//
			// select the measures
			for (MeasureMapping mx : getMapper().getMeasureMapping()) {
				SubSelectReferencePiece pieceRef = new SubSelectReferencePiece(from, mx.getPiece());
				ISelectPiece piece = main.select(pieceRef, mx.getPiece().getAlias());// make
				// sure
				// to
				// use
				// the
				// same
				// alias
				piece.addComment("copy of " + mx.getMapping().getName() + " (Metric)");
				mx.setPiece(piece);// update the mapping... that's dangerous!
			}
			return new SQLScript(main, getMapper());
		} catch (SQLScopeException e) {
			throw new SQLScopeException("Failed to generate the analytics features, caused by:\n" + e.getLocalizedMessage(),
					e);
		}
	}

	private ExpressionAST rewriteQualifyCondition(ExpressionAST expression, SelectUniversal mainSelect,
			FromSelectUniversal from) throws SQLScopeException, ScopeException {
		if (expression.getImageDomain().isInstanceOf(AnalyticDomain.DOMAIN)) {
			// the idea is to extract the analytic operator, generate measure
			// for each, and replace by a reference to the measure
			if (expression instanceof Operator) {
				Operator op = (Operator) expression;
				if (op.getOperatorDefinition() instanceof OrderedAnalyticOperatorDefinition) {
					Object binding = mainSelect.getScope().get(op);
					if (binding!=null) {
						if (!(binding instanceof SubSelectReferencePiece)) {
							// ok, already bound but by someone else?
							throw new SQLScopeException("cannot rewrite analytics feature: " + expression.prettyPrint());
						}
					} else {
						// compute the analytic operator in the sub query
						ISelectPiece select = from.getWrapperSelectInterface().select(op);
						// create a reference
						SubSelectReferencePiece ref = new SubSelectReferencePiece(from, select);
						// register the reference in the outer scope
						mainSelect.getScope().put(op, ref);
						// return the operator that will be overriden by the scope
					}
					return ExpressionMaker.GROUP(op);// add a group around it to force overriding... (if not it may fail if it is the top level expression)
				} else {
					// continue...
					Operator rewrite = new Operator(op.getOperatorDefinition());
					for (ExpressionAST argument : op.getArguments()) {
						ExpressionAST x = rewriteQualifyCondition(argument, mainSelect, from);
						rewrite.add(x);
					}
					return rewrite;
				}
			} else if (expression instanceof ExpressionRef) {
				ExpressionRef ref = (ExpressionRef) expression;
				Object obj = ref.getReference();
				if (obj instanceof Measure) {
					Measure measure = (Measure) obj;
					// select the measure
					ISelectPiece piece = select.select(measure.getDefinition(), measure.getName());
					piece.addComment(measure.getName() + " (Qualify)");
					select.getScope().put(measure, measure.getDefinitionSafe());// register
					// the
					// measure
					// in
					// the
					// select
					// context
					// register the piece in the main select
					SubSelectReferencePiece pieceRef = new SubSelectReferencePiece(from, piece);
					mainSelect.getScope().put(ref, pieceRef);
					return ref;
				}
			}
			// else
			throw new SQLScopeException("cannot rewrite analytics feature: " + expression.prettyPrint());
		} else {
			// do nothing
			return expression;
		}
	}

	@Override
	/**
	 * override to add subject
	 */
	public List<String> computeDependencies() {
		List<String> deps = super.computeDependencies();
		deps.add(subject.getId().toUUID());
		return deps;
	}

	public boolean hasRollups() {
		return rollupGrandTotal || (rollup != null && !rollup.isEmpty());
	}

	public GroupByAxis getPrecomputedRollupAxis() {
		return precomputedRollupAxis;
	}

	public void setPrecomputedRollupAxis(GroupByAxis precomputedRollupAxis) {
		this.precomputedRollupAxis = precomputedRollupAxis;
	}
}
