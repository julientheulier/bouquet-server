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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.database.model.Column;
import com.squid.core.database.model.Database;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.aggregate.AggregateDomain;
import com.squid.core.domain.analytics.AnalyticDomain;
import com.squid.core.domain.sort.SortOperatorDefinition;
import com.squid.core.expression.ConstantValue;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.ExpressionLeaf;
import com.squid.core.expression.Operator;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.Context;
import com.squid.core.sql.db.features.QualifySupport;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.IOrderByPiece;
import com.squid.core.sql.render.IOrderByPiece.ORDERING;
import com.squid.core.sql.render.IPiece;
import com.squid.core.sql.render.ISelectPiece;
import com.squid.core.sql.render.ISkinFeatureSupport;
import com.squid.core.sql.render.IWherePiece;
import com.squid.core.sql.render.RenderingException;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.engine.processor.AxisListExtractor;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.processor.DataMatrixTransform;
import com.squid.kraken.v4.core.analysis.engine.processor.DateExpressionAssociativeTransformationExtractor;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.AxisMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.MeasureMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.QueryMapper;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.SimpleMapping;
import com.squid.kraken.v4.core.analysis.model.Intervalle;
import com.squid.kraken.v4.core.analysis.model.OrderBy;
import com.squid.kraken.v4.core.analysis.scope.AxisExpression;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.database.impl.DatasourceDefinition;
import com.squid.kraken.v4.core.sql.SelectUniversal;
import com.squid.kraken.v4.core.sql.script.SQLScript;
import com.squid.kraken.v4.model.Domain;

/**
 * Implements the IQuery interface on top of the SelctMapping
 * 
 * @author sfantino
 *
 */
public class BaseQuery implements IQuery {

	static final Logger logger = LoggerFactory.getLogger(BaseQuery.class);

	private Universe universe;
	protected SelectUniversal select;

	private QueryMapper mapper = new QueryMapper();
	
	private List<DataMatrixTransform> postProcessing = new ArrayList<>();

	public BaseQuery() {
		//
	}

	public BaseQuery(Universe universe, SelectUniversal select) throws SQLScopeException {
		this();
		this.universe = universe;
		this.select = select;
	}

	public BaseQuery(Universe universe) throws SQLScopeException {
		this();
		this.universe = universe;
		this.select = new SelectUniversal(universe);
	}

	public BaseQuery(Universe universe, Domain subject) throws SQLScopeException, ScopeException {
		this(universe);
		this.select.from(universe.S(subject));
	}

	/**
	 * Add a postProcessing step for latter use
	 * @param dataMatrixTransformOrderBy
	 */
	public void addPostProcessing(DataMatrixTransform dataMatrixTransform) {
		this.postProcessing.add(dataMatrixTransform);
	}
	
	/**
	 * Get all the defined postProcessing steps
	 * @return the postProcessing
	 */
	public List<DataMatrixTransform> getPostProcessing() {
		return postProcessing;
	}

	public SelectUniversal getSelect() {
		return select;
	}

	public DatasourceDefinition getDatasource() {
		return select.getDatasource();
	}

	public Universe getUniverse() {
		return universe;
	}

	public QueryMapper getMapper() {
		return mapper;
	}

	protected void add(AxisMapping axis) {
		mapper.add(axis);
	}

	protected void add(int index, AxisMapping axis) {
		mapper.add(index, axis);
	}

	protected void add(MeasureMapping measure) {
		mapper.add(measure);
	}

	private List<OrderBy> orderBy = new ArrayList<OrderBy>();

	/**
	 * add an global order by clause to the select indexes are column indexes
	 * 
	 * @param indexes
	 * @throws SQLScopeException
	 * @throws ScopeException
	 */
	public void orderBy(List<OrderBy> orders) throws ScopeException, SQLScopeException {
		boolean hasSimpleOrders = true;
		for (OrderBy order : orders) {
			orderBy.add(order);
			SimpleMapping m = mapper.find(order.getExpression());
			if (m != null) {
				m.setOrdering(order.getOrdering());
				select.orderBy(m.getPiece()).setOrdering(order.getOrdering());
			} else {
				if (checkAllowOrderBy(order)) {
					// the order by will imply a new group by, but because it is
					// a parent of an existing one that won't change the results
					// so you don't have to display the added column if not
					// already present
					// e.g. if group by city and I want to orderBy country, I
					// d'ont have to display the country
					IPiece piece = select.createPiece(Context.ORDERBY, order.getExpression());
					select.orderBy(piece).setOrdering(order.getOrdering());
					hasSimpleOrders = hasSimpleOrders && this.isSimpleOrderBy(order);
				} else {
					// throw new ScopeException("invalid orderBy expression
					// "+order.getExpression().prettyPrint() + ": you must
					// select it (or a child dimension) as a facet");
					logger.warn("invalid orderBy expression " + order.getExpression().prettyPrint()
							+ ": you must select it (or a child dimension) as a facet");
				}
			}
		}
		select.getStatement().setHasSimpleOrderBys(hasSimpleOrders);
	}

	/**
	 * Check if it's ok to orderBy an expression which is not yet selected, but
	 * only if a child of the dimension is selected
	 * 
	 * @param order
	 * @return
	 * @throws ScopeException
	 * @throws SQLScopeException
	 */
	private boolean checkAllowOrderBy(OrderBy order) throws ScopeException, SQLScopeException {
		//
		ExpressionAST expr = order.getExpression();
		IDomain image = expr.getImageDomain();
		if (image.isInstanceOf(AggregateDomain.DOMAIN)) {
			return true;
		} else {
			// the order expression is not yet in the scope
			Axis axis = universe.asAxis(order.getExpression());
			if (axis == null) {
				return false;
			} else {				
				for (AxisMapping ax : getMapper().getAxisMapping()) {
					try {
						if (axis.isParentDimension(ax.getAxis())) {
							return true;
						} else {
							// check if there are the same after transformation
							DateExpressionAssociativeTransformationExtractor ex = new DateExpressionAssociativeTransformationExtractor();
							ExpressionAST naked1 = ex.eval(axis.getDefinitionSafe());
							ExpressionAST naked2 = ex.eval(ax.getAxis().getDefinitionSafe());
							if( naked1.equals(naked2))
								return true;
						}
					} catch (ComputingException | InterruptedException e) {
						// ignore
					}
				}				
			}
			AxisListExtractor ex = new AxisListExtractor();
			Set<Axis> orderByAxes = ex.eval(expr);
			if (orderByAxes.isEmpty()){
				return false;
			}
			Set<Axis> expressionAxes = new HashSet<Axis>();
			for (AxisMapping ax : getMapper().getAxisMapping()) {
				expressionAxes.add(ax.getAxis());
			}
			return (expressionAxes.containsAll(orderByAxes));						
		}
	}
	
	private boolean isSimpleOrderBy(OrderBy order) throws ScopeException{
		ExpressionAST expr = order.getExpression();
		IDomain image = expr.getImageDomain();
		if (image.isInstanceOf(AggregateDomain.DOMAIN)) {
			return true;
		}
		Axis axis = universe.asAxis(order.getExpression());
		if (axis == null){
			return false;
		}		
		if (expr instanceof AxisExpression){
			return true;
		}
		if (expr instanceof SortOperatorDefinition){
			Operator operator = (Operator) expr;
			if (operator.getArguments().size() != 1){
				return false;
			}else{
				return operator.getArguments().get(0) instanceof AxisExpression  ;
			}						
		}	
		return false;
	}
	
	public List<OrderBy> getOrderBy() {
		return orderBy;
	}

	public void orderBy(ISelectPiece piece, ORDERING ordering) {
		this.select.orderBy(piece).setOrdering(ordering);
	}

	public void limit(long limit) {
		this.select.getStatement().setLimitValue(limit);
	}

	public void offset(long limit) {
		this.select.getStatement().setOffsetValue(limit);
	}


	@Override
	public String viewSQL() {
		try {
			String SQL = select.render();
			return SQL;
		} catch (Exception e) {
			return e.toString();
		}
	}

	@Override
	public SQLScript generateScript() throws SQLScopeException {
		if (isQualifyRequired()
				&& select.getSkin().getFeatureSupport(QualifySupport.ID) == ISkinFeatureSupport.IS_NOT_SUPPORTED) {
			return generateQualifyScript();
		} else {
			return new SQLScript(select, getMapper());
		}
	}

	public String render() throws RenderingException {
		try {
			return generateScript().render();
		} catch (SQLScopeException e) {
			throw new RenderingException(e);
		}
	}

	public String renderNoLimitNoOrderBy() throws RenderingException {
		long limit = this.select.getStatement().getLimitValue();
		ArrayList<IOrderByPiece> orderByPieces = (ArrayList<IOrderByPiece>) this.select.getStatement()
				.getOrderByPieces();

		this.select.getStatement().setLimitValue(-1);
		this.select.getStatement().setOrderByPieces(new ArrayList<IOrderByPiece>());

		String sqlNoLimit = select.render();

		this.select.getStatement().setLimitValue(limit);
		this.select.getStatement().setOrderByPieces(orderByPieces);

		return sqlNoLimit;
	}

	protected SQLScript generateQualifyScript() throws SQLScopeException {
		throw new RuntimeException("QUALIFY clause is not supported for that request");
	}

	/**
	 * compute the dependencies for the query
	 * 
	 * @param deps
	 */
	public List<String> computeDependencies() {
		List<String> deps = new ArrayList<>();
		deps.add(getUniverse().getProject().getId().toUUID());
		return deps;
	}
	
	protected DataMatrix computeDataMatrix(Database database, RawMatrix rawMatrix) throws ScopeException {
		return new DataMatrix( database,  rawMatrix, mapper);
	}

	//
	/**
	 * helper method that construct the formula: (expr>intervalle.min and
	 * expr<intervalle.max)
	 * 
	 * @param expr
	 * @param intervalle
	 * @return
	 * @throws ScopeException
	 */
	protected ExpressionAST where(ExpressionAST expr, Intervalle intervalle) throws ScopeException {
		ExpressionAST where = null;
		ExpressionAST lower = intervalle.getLowerBoundExpression();
		ExpressionAST upper = intervalle.getUpperBoundExpression();
		where = createIntervalle(expr, expr, lower, upper);
		return where != null ? ExpressionMaker.GROUP(where) : null;
	}

	protected ExpressionAST createIntervalle(ExpressionAST start, ExpressionAST end, ExpressionAST lower,
			ExpressionAST upper) {
		if (lower != null && upper != null) {
			return ExpressionMaker.AND(ExpressionMaker.GREATER(start, lower, false),
					ExpressionMaker.LESS(end, upper, false));
		} else if (lower != null) {
			return ExpressionMaker.GREATER(start, lower, false);
		} else if (upper != null) {
			return ExpressionMaker.LESS(end, upper, false);
		} else {
			return null;
		}
	}

	public enum FilterType {
		WHERE, EXISTS
	}

	public class Filter {

		private FilterType type;
		private Axis axis;
		private Collection<DimensionMember> filters;

		public Filter(FilterType type, Axis axis, Collection<DimensionMember> filters) {
			super();
			this.type = type;
			this.axis = axis;
			this.filters = filters;
		}

		public FilterType getType() {
			return type;
		}

		public Axis getAxis() {
			return axis;
		}

		public Collection<DimensionMember> getFilters() {
			return filters;
		}

		@Override
		public String toString() {
			return "Filter [" + type + " " + axis + "=" + filters + "]";
		}

		public void applyFilter(SelectUniversal select) throws ScopeException, SQLScopeException {
			switch (getType()) {
			case WHERE:
				where(select, getAxis(), getFilters());
				break;
			case EXISTS:
				SelectUniversal subselect = select.exists(getAxis());
				where(subselect, getAxis(), getFilters());
				break;
			}
		}

	}

	private List<Filter> filters = null;

	private void addFilter(Filter filter) {
		if (filters == null) {
			filters = new ArrayList<>();
		}
		filters.add(filter);
	}

	public List<Filter> getFilters() {
		if (filters == null) {
			return Collections.emptyList();
		} else {
			return filters;
		}
	}

	public void exists(Axis axis, Collection<DimensionMember> filters) throws ScopeException, SQLScopeException {
		SelectUniversal subselect = select.exists(axis);
		if (where(subselect, axis, filters)) {
			addFilter(new Filter(FilterType.EXISTS, axis, filters));
		}
	}

	public void where(Axis axis, Collection<DimensionMember> filters) throws ScopeException, SQLScopeException {
		if (where(this.select, axis, filters)) {
			addFilter(new Filter(FilterType.WHERE, axis, filters));
		}
	}

	private List<ExpressionAST> conditions = null;

	private boolean isQualify = false;// check if any condition implies a
										// Qualify

	public List<ExpressionAST> getConditions() {
		if (conditions == null) {
			return Collections.emptyList();
		} else {
			return conditions;
		}
	}

	public void where(ExpressionAST condition) throws ScopeException, SQLScopeException {
		if (conditions == null) {
			conditions = new ArrayList<>();
		}
		conditions.add(condition);
		if (condition.getImageDomain().isInstanceOf(AnalyticDomain.DOMAIN)) {
			isQualify = true;
		}
		this.select.where(condition);
	}

	public boolean isQualifyRequired() {
		return isQualify;
	}

	/**
	 * apply the filters to the axis and modify the select accordingly. Return
	 * true if the select has been actually modified
	 * 
	 * @param select
	 * @param axis
	 * @param filters
	 * @return
	 * @throws ScopeException
	 * @throws SQLScopeException
	 */
	protected boolean where(SelectUniversal select, Axis axis, Collection<DimensionMember> filters)
			throws ScopeException, SQLScopeException {
		//
		ExpressionAST expr = axis.getDefinition();
		// ticket:3014 - handles predicates
		if (expr.getImageDomain().isInstanceOf(IDomain.CONDITIONAL)) {
			// ok, apply the predicate only if filters == [true]
			if (filters.size() == 1) {
				// get the first
				Iterator<DimensionMember> iter = filters.iterator();
				DimensionMember member = iter.next();
				if (member.getID() instanceof Boolean && ((Boolean) member.getID()).booleanValue()) {
					// if true, add the predicate
					IWherePiece piece = select.where(expr);
					piece.addComment("filtering on: " + axis.getName());
					return true;
				}
			}
			// else
			return false;
		}
		boolean filter_by_null = false;// T1198
		List<Object> filter_by_members = new ArrayList<Object>();
		ExpressionAST filter_by_intervalle = null;
		for (DimensionMember filter : filters) {
			Object value = filter.getID();
			// check if the member is an interval
			if (value instanceof Intervalle) {
				ExpressionAST where = where(expr, (Intervalle) value);
				if (filter_by_intervalle == null) {
					filter_by_intervalle = where;
				} else if (where != null) {
					filter_by_intervalle = ExpressionMaker.OR(filter_by_intervalle, where);
				}
			} else {
				if (filter.getID()==null || filter.getID().toString()=="") {
					filter_by_null = true;
				} else {
					filter_by_members.add((filter).getID());
				}
			}
		}
		ExpressionAST filterALL = null;
		if (!filter_by_members.isEmpty()) {
			if (filter_by_members.size() == 1) {
				ConstantValue value = ExpressionMaker.CONSTANT(filter_by_members.get(0));
				filterALL = ExpressionMaker.EQUAL(expr, value);
			} else {
				filterALL = ExpressionMaker.IN(expr, ExpressionMaker.CONSTANTS(filter_by_members));
			}
		}
		if (filter_by_null) {
			ExpressionAST filterNULL = ExpressionMaker.ISNULL(expr);
			filterALL = (filterALL == null) ? filterNULL : ExpressionMaker.OR(filterALL, filterNULL);
		}
		if (filter_by_intervalle != null) {
			filterALL = (filterALL == null) ? filter_by_intervalle : ExpressionMaker.OR(filterALL, filter_by_intervalle);
		}
		if (filterALL != null) {
			IWherePiece piece = select.where(filterALL);
			piece.addComment("filtering on: " + axis.getName());
		}
		//
		// handling constraint propagation...
		if (select.getAnalyzer() != null) {
			Column c = select.getAnalyzer().factorDimension(axis.getDefinition());
			if (c != null) {
				// Detect Time Constraint again (slight opti possible by putting
				// in the above code)
				// T129
				DimensionMember timeFilter = null;
				for (DimensionMember filter : filters) {
					if (filter.getID() instanceof Intervalle) {
						timeFilter = filter;
					}
				}
				if (timeFilter != null) {
					// make a copy, do not modify filters => it's global !
					ArrayList<DimensionMember> copy = new ArrayList<DimensionMember>(filters);
					copy.remove(timeFilter);
					select.getAnalyzer().addConstraint(c, copy, timeFilter);
				} else {
					select.getAnalyzer().addConstraint(c, filters);
				}

			}
		}
		//
		return filterALL != null;
	}

	public void setComment(String comment) {
		this.select.getStatement().addComment(comment);
	}

	/**
	 * check that all the metrics are associative. That allows to compute rollup
	 * in two steps for instance
	 *
	 */
	public boolean isAssociative() {
		// T136: check if query metrics are all associative
		for (MeasureMapping mp : mapper.getMeasureMapping()) {
			if (!mp.getMapping().isAssociative()) {
				return false;
			}
		}
		//
		return true;
	}

}
