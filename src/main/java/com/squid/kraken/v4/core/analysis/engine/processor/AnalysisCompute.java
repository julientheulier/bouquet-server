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
package com.squid.kraken.v4.core.analysis.engine.processor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.Months;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.domain.IDomain;
import com.squid.core.domain.extensions.date.DateTruncateOperatorDefinition;
import com.squid.core.domain.extensions.date.DateTruncateShortcutsOperatorDefinition;
import com.squid.core.domain.operators.ExtendedType;
import com.squid.core.domain.operators.OperatorDefinition;
import com.squid.core.domain.operators.Operators;
import com.squid.core.domain.set.SetDomain;
import com.squid.core.expression.Compose;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.Operator;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.model.IAlias;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.ExpressionListPiece;
import com.squid.core.sql.render.IOrderByPiece;
import com.squid.core.sql.render.IOrderByPiece.NULLS_ORDERING;
import com.squid.core.sql.render.IOrderByPiece.ORDERING;
import com.squid.core.sql.render.IPiece;
import com.squid.core.sql.render.ISelectPiece;
import com.squid.core.sql.render.ITypedPiece;
import com.squid.core.sql.render.OperatorPiece;
import com.squid.core.sql.render.RenderingException;
import com.squid.core.sql.render.SelectPiece;
import com.squid.core.sql.render.SelectPieceReference;
import com.squid.core.sql.render.SimpleConstantValuePiece;
import com.squid.core.sql.render.SubSelectReferencePiece;
import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.api.core.SQLStats;
import com.squid.kraken.v4.api.core.attribute.AttributeServiceBaseImpl;
import com.squid.kraken.v4.caching.NotInCacheException;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.engine.query.QueryRunner;
import com.squid.kraken.v4.core.analysis.engine.query.SimpleQuery;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.AxisMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.MeasureMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.QueryMapper;
import com.squid.kraken.v4.core.analysis.model.Dashboard;
import com.squid.kraken.v4.core.analysis.model.DashboardAnalysis;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.model.DomainSelection;
import com.squid.kraken.v4.core.analysis.model.ExpressionInput;
import com.squid.kraken.v4.core.analysis.model.GroupByAxis;
import com.squid.kraken.v4.core.analysis.model.Intervalle;
import com.squid.kraken.v4.core.analysis.model.IntervalleObject;
import com.squid.kraken.v4.core.analysis.model.MeasureGroup;
import com.squid.kraken.v4.core.analysis.model.OrderBy;
import com.squid.kraken.v4.core.analysis.model.OrderByGrowth;
import com.squid.kraken.v4.core.analysis.scope.AxisExpression;
import com.squid.kraken.v4.core.analysis.scope.MeasureExpression;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Property.OriginType;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.sql.FromSelectUniversal;
import com.squid.kraken.v4.core.sql.SelectUniversal;
import com.squid.kraken.v4.model.Attribute;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.writers.PreviewWriter;
import com.squid.kraken.v4.writers.QueryWriter;

/**
 * this is where the actual computations take place, in relation with a given
 * GBall/Universe
 *
 * @author sfantino
 *
 */
public class AnalysisCompute {

	private Universe universe;
	private boolean mandatory_link = false;

	static final Logger logger = LoggerFactory.getLogger(AnalysisCompute.class);

	public static final boolean SUPPORT_SMART_CACHE = new Boolean(
			KrakenConfig.getProperty("feature.smartcache", "false"));

	// turn to true to support soft-filter optimization
	private static final boolean SUPPORT_SOFT_FILTERS = new Boolean(
			KrakenConfig.getProperty("feature.softfilters", "false"));

	public AnalysisCompute(Universe universe) {
		this.universe = universe;
	}

	public List<SimpleQuery> reinject(DashboardAnalysis analysis)
			throws ComputingException, ScopeException, SQLScopeException, InterruptedException, RenderingException {
		List<MeasureGroup> groups = analysis.getGroups();
		List<SimpleQuery> queries = new ArrayList<SimpleQuery>();
		if (groups.isEmpty()) {
			SimpleQuery query = this.genSimpleQuery(analysis);
			queries.add(query);
		} else {
			// StringBuilder result = new StringBuilder();
			boolean optimize = false;
			for (MeasureGroup group : analysis.getGroups()) {
				SimpleQuery query = this.genAnalysisQuery(analysis, group, optimize);
				queries.add(query);
			}
		}
		return queries;
	}

	public String viewSQL(DashboardAnalysis analysis)
			throws ComputingException, ScopeException, SQLScopeException, InterruptedException, RenderingException {
		List<MeasureGroup> groups = analysis.getGroups();
		if (groups.isEmpty()) {
			SimpleQuery query = this.genSimpleQuery(analysis);
			return query.render();
		} else {
			StringBuilder result = new StringBuilder();
			boolean optimize = false;
			for (MeasureGroup group : analysis.getGroups()) {
				SimpleQuery query = this.genAnalysisQuery(analysis, group, optimize);
				result.append(query.render());
				result.append("\n\n");
			}
			return result.toString();
		}
	}

	public DataMatrix computeAnalysis(DashboardAnalysis analysis)
			throws ScopeException, SQLScopeException, ComputingException, InterruptedException, RenderingException {
		//
		List<MeasureGroup> groups = analysis.getGroups();
		if (groups.isEmpty()) {
			SimpleQuery query = this.genSimpleQuery(analysis);
			PreviewWriter qw = new PreviewWriter();
			QueryRunner runner = new QueryRunner(universe.getContext(), query, analysis.isLazy(), qw,
					analysis.getJobId());
			runner.run();

			DataMatrix dm = qw.getDataMatrix();
			if (dm != null) {
				for (DataMatrixTransform transform : query.getPostProcessing()) {
					dm = transform.apply(dm);
				}
			}
			return dm;
		} else {
			// disable the optimizing when using the limit feature
			boolean optimize = SUPPORT_SOFT_FILTERS && !analysis.hasLimit() && !analysis.hasOffset()
					&& !analysis.hasRollup();
			return computeAnalysisSimple(analysis, optimize);
		}
	}

	protected ExpressionAST convertToInterval(ExpressionAST expr, Intervalle intervalle) throws ScopeException {
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

	protected ExpressionAST createMetricOffset(ExpressionAST kpiExpr, ExpressionAST offsetExpression) {
		if (kpiExpr instanceof Operator) {
			Operator op = (Operator) kpiExpr;
			List<ExpressionAST> exprs = op.getArguments();
			List<ExpressionAST> newExprs = new ArrayList<ExpressionAST>();
			if (exprs.size()>0) {
				for (ExpressionAST expr:exprs) {
					if (op.getOperatorDefinition().getDomain().isInstanceOf(IDomain.AGGREGATE)) {
						newExprs.add(ExpressionMaker.CASE(offsetExpression, expr));
					} else{
						newExprs.add(createMetricOffset(expr, offsetExpression));
					}
				}
			} else {
				//No args, it is a count(*) like expression
				newExprs.add(ExpressionMaker.CASE(offsetExpression,ExpressionMaker.CONSTANT(1)));
			}
			return ExpressionMaker.op(op.getOperatorDefinition(),newExprs);
		} else if (kpiExpr instanceof MeasureExpression) {
			return createMetricOffset(((MeasureExpression) kpiExpr).getMeasure().getDefinitionSafe(), offsetExpression);
		} else if (kpiExpr instanceof Compose) {
			return createMetricOffset(((Compose) kpiExpr).getHead(), offsetExpression);
		}
		return kpiExpr;
	}

	// handle compare T947
	public DashboardAnalysis generateAnalysisCompareTo(final DashboardAnalysis currentAnalysis, boolean hasOverlap, boolean isCompareOverlap)
			throws ScopeException, ComputingException, SQLScopeException, InterruptedException, RenderingException {
		// preparing the selection
		DashboardSelection presentSelection = currentAnalysis.getSelection();
		DomainSelection compare = presentSelection.getCompareToSelection();
		Axis joinAxis = null;
		IntervalleObject presentInterval = null;
		IntervalleObject pastInterval = null;
		// compute the joinAxis if exists, i.e. if one of the groupBy dimension is part of the comparison

		// list the dimensions
		ArrayList<ExpressionAST> queue = new ArrayList<>();// order matter
		for (GroupByAxis group : currentAnalysis.getGrouping()) {
			queue.add(group.getAxis().getReference());
		}
		// check the explicit orderBy
		List<OrderBy> originalOrders = currentAnalysis.getOrders();
		for (OrderBy order : originalOrders) {
			if (!queue.contains(order.getExpression())) {
				// Else if axis doesn't exist yet, add it
				if (order.getExpression() instanceof AxisExpression) {
					GroupByAxis group = new GroupByAxis(((AxisExpression)order.getExpression()).getAxis());
					currentAnalysis.getGrouping().add(group);
					queue.add(group.getAxis().getReference());
				}
			}
		}

		for (Axis filter : compare.getFilters()) {
			// check if the filter is a join
			GroupByAxis groupBy = findGroupingJoin(filter, currentAnalysis);
			if (groupBy != null) {
				if (joinAxis != null) {
					throw new ScopeException("only one join axis supported");
				}
				joinAxis = groupBy.getAxis();
				// compute the min & max for present (if it's an interval)
				Collection<DimensionMember> members = presentSelection.getMembers(filter);
				presentInterval = computeMinMax(members);
			}
		}
		//
		// handling orderBy in the proper way...
		// rebuild the full orderBy specs
		final TreeMap<Integer,OrderBy> comparedOrder = new TreeMap<Integer,OrderBy>();

		List<OrderBy> remaining = new ArrayList<>();
		int i = 0;

		// use this list to compute the dimension index
		ArrayList<ExpressionAST> dimensionIndexes = new ArrayList<>(queue);
		// will hold in which order to merge
		int[] mergeOrder = new int[dimensionIndexes.size()];
		//

		ArrayList<OrderBy> fixed = new ArrayList<OrderBy>();
		// if there is a joinAxis, it must appear as the first orderBy
		if (joinAxis != null) {
			OrderBy order = null;
			if (originalOrders!=null && !originalOrders.isEmpty()) {
				// look for the real spec
				for (OrderBy check : originalOrders) {
					if (check.getExpression().equals(joinAxis.getReference())) {
						order = check;
						break;// quit the loop
					}
				}
			}
			if (order==null) {
				// not defined, create a default one
				order = new OrderBy(0, joinAxis.getReference(), ORDERING.DESCENT);
			}
			fixed.add(new OrderBy(i, order.getExpression(), order.getOrdering()));
			mergeOrder[i++] = dimensionIndexes.indexOf(order.getExpression());
		}
		//
		// check the explicit orderBy
		for (OrderBy order : originalOrders) {
			// check if it is a dimension
			if (queue.contains(order.getExpression())) {
				// is it the joinAxis ?
				if (joinAxis!=null && order.getExpression().equals(joinAxis.getReference())) {
					// we already added it, just remove from the queue
					queue.remove(order.getExpression());
				} else {
					// ok, just add it
					fixed.add(new OrderBy(i, order.getExpression(), order.getOrdering()));
					mergeOrder[i++] = dimensionIndexes.indexOf(order.getExpression());
					// and remove the dimension from the list
					queue.remove(order.getExpression());
				}
			} else {
				// assuming it is a metric or something else, keep it but at the
				// end
				remaining.add(order);// don't know the position yet
			}
		}
		// handling the dimensions not sorted
		if (!queue.isEmpty()) {
			for (ExpressionAST dim : queue) {
				if (joinAxis == null || !joinAxis.getReference().equals(dim)) {
					// check the best order
					IDomain image = dim.getImageDomain();
					fixed.add(new OrderBy(i, dim, image.isInstanceOf(IDomain.TEMPORAL)?ORDERING.DESCENT:ORDERING.ASCENT));
					mergeOrder[i++] = dimensionIndexes.indexOf(dim);
				}
			}
		}
		// add non-dimensions
		if (!remaining.isEmpty()) {
			for (OrderBy order : remaining) {
				fixed.add(new OrderBy(i++, order.getExpression(), order.getOrdering()));
			}
		}
		// T1890 - need to be careful if there is a limit
		// in that case we should always have an explicit orderBy
		if (!hasOverlap && originalOrders.isEmpty() && currentAnalysis.getGrouping().size()>=1) {
			// no orderBy specified, but there is a limit.
			// In order to keep results consistent between each call we need to add an orderBy
			// so we can apply the fixed list which is never empty
			if (fixed.isEmpty()) {
				throw new ScopeException("invalid compareTo specification, unable to define ordering");
			} else {
				currentAnalysis.setOrders(fixed);
			}
		}

		//
		// compute the past version
		DashboardAnalysis compareToAnalysis = new DashboardAnalysis(universe);

		// copy stuff
		if (currentAnalysis.hasLimit())
			compareToAnalysis.limit(currentAnalysis.getLimit());
		if (currentAnalysis.hasOffset())
			compareToAnalysis.offset(currentAnalysis.getOffset());
		if (currentAnalysis.isRollupGrandTotal())
			compareToAnalysis.setRollupGrandTotal(true);
		if (currentAnalysis.hasRollup())
			compareToAnalysis.setRollup(currentAnalysis.getRollup());
		compareToAnalysis.setOrders(currentAnalysis.getOrders());// copy the modified one
		// copy the selection and replace with compare filters
		DashboardSelection pastSelection = new DashboardSelection(presentSelection);
		String compareToWhat = "";
		ExpressionAST pastExpression = null;
		ExpressionAST presentExpression = null;
		for (Axis filter : compare.getFilters()) {
			Collection<DimensionMember> presentPeriod = presentSelection.getMembers(filter);
			Collection<DimensionMember> cols = new ArrayList<DimensionMember>();
			if (!isCompareOverlap) {
				cols.addAll(presentPeriod);
			}
			cols.addAll(compare.getMembers(filter));
			pastSelection.clear(filter);
			pastSelection.add(filter, cols);
			if (joinAxis != null && compareAxis(filter, joinAxis)) {
				pastInterval = computeMinMax(compare.getMembers(filter));
				IntervalleObject alignedPastInterval = this.alignPastInterval(presentInterval, pastInterval, joinAxis);
				if (!alignedPastInterval.equals(pastInterval)) {
					logger.info(pastInterval.toString() + " realigned to " + alignedPastInterval.toString());
					pastSelection.clear(filter);
					pastSelection.add(filter, alignedPastInterval);
				}
			}
			//
			presentPeriod.removeAll(compare.getMembers(filter));
			presentInterval = computeMinMax(presentPeriod);
			pastInterval = computeMinMax(compare.getMembers(filter));
			pastExpression = convertToInterval(filter.getDefinitionSafe(), pastInterval);
			presentExpression = convertToInterval(filter.getDefinitionSafe(), presentInterval);
			if (!compareToWhat.equals(""))
				compareToWhat += " and ";
			if (pastInterval != null) {
				compareToWhat += pastInterval.toString();
			} else {
				compareToWhat += "[" + (compare.getMembers(filter)).toString() + "]";
			}
		}
		DateTime startPresent = new DateTime(presentInterval.getLowerBound());
		DateTime endPresent = new DateTime(presentInterval.getUpperBound());
		DateTime startPast = new DateTime(pastInterval.getLowerBound());
		DateTime endPast = new DateTime(pastInterval.getUpperBound());

		// copy dimensions
		/*
		 * ArrayList<GroupByAxis> compareBeyondLimit =
		 * currentAnalysis.hasBeyondLimit() ? new ArrayList<GroupByAxis>() :
		 * null;
		 */
		int ij = 0;

		ArrayList<GroupByAxis> compareBeyondLimit = currentAnalysis.hasBeyondLimit() ? new ArrayList<GroupByAxis>()
				: null;
		for (GroupByAxis groupBy : currentAnalysis.getGrouping()) {
			GroupByAxis newGroupBy = null;
			if (groupBy.getAxis().equals(joinAxis) && (!hasOverlap || isCompareOverlap)) {
				boolean offsetByDay = (Days.daysBetween(startPresent, endPresent).getDays() == Days.daysBetween(startPast, endPast).getDays());
				int nrMonths = Months.monthsBetween(startPast, startPresent).getMonths();
				//AddMonths is handling properly last day offset for months with different days
				if (startPresent.equals(startPast.plusMonths(nrMonths)) && (endPresent.equals(endPast.plusMonths(nrMonths)) || endPresent.isAfter(endPast.plusMonths(nrMonths)))) {
					offsetByDay = false;
				}
				ExpressionAST groupByExpr = groupBy.getAxis().getDefinitionSafe();
				ExpressionMaker.MINUS(ExpressionMaker.CONSTANT(presentInterval.getLowerBound(), IDomain.DATE), ExpressionMaker.CONSTANT(pastInterval.getLowerBound(), IDomain.DATE));
				int nrDays = Days.daysBetween(startPresent, startPast).getDays();
				OperatorDefinition dateOperator = nrDays < 0 ? Operators.DATE_ADD: Operators.DATE_SUB;
				if (groupByExpr instanceof Operator && (((Operator) groupByExpr).getOperatorDefinition() instanceof DateTruncateShortcutsOperatorDefinition
						||((Operator) groupByExpr).getOperatorDefinition() instanceof DateTruncateOperatorDefinition)) {
					Operator op = (Operator) groupByExpr;
					List<ExpressionAST>  rootAxes = op.getArguments();
					List<ExpressionAST>  rootAxesWithOffset = new ArrayList<ExpressionAST>();
					int index = 0;
					for (ExpressionAST expr: rootAxes) {
						if (index==0) { //This handles DateTruncate function / shortcut, date is the first/only arg
							if (offsetByDay) {
								rootAxesWithOffset.add(ExpressionMaker.CASE(pastExpression, ExpressionMaker.op(dateOperator, expr, ExpressionMaker.CONSTANT(Math.abs(nrDays)), ExpressionMaker.CONSTANT("DAY")), expr));
							} else {
								rootAxesWithOffset.add(ExpressionMaker.CASE(pastExpression, ExpressionMaker.ADD_MONTHS(expr, ExpressionMaker.CONSTANT(nrMonths)), expr));
							}
						} else {
							rootAxesWithOffset.add(expr);
						}
						index++;
					}
					groupByExpr = ExpressionMaker.op(op.getOperatorDefinition(),rootAxesWithOffset);
				} else {
					if (offsetByDay) {
						groupByExpr = ExpressionMaker.CASE(pastExpression, ExpressionMaker.op(dateOperator, groupByExpr, ExpressionMaker.CONSTANT(Math.abs(nrDays)), ExpressionMaker.CONSTANT("DAY")), groupByExpr);
					} else {
						groupByExpr = ExpressionMaker.CASE(pastExpression, ExpressionMaker.ADD_MONTHS(groupByExpr, ExpressionMaker.CONSTANT(nrMonths)), groupByExpr);
					}
				}
				Axis compareToAxis = new Axis(groupBy.getAxis(), groupByExpr);

				compareToAxis.setOriginType(OriginType.COMPARETO);

				newGroupBy = compareToAnalysis.add(compareToAxis, groupBy.isRollup());
				newGroupBy.setRollupPosition(groupBy.getRollupPosition());
				// update the beyondLimit
				if (compareBeyondLimit != null && currentAnalysis.getBeyondLimit().contains(groupBy)) {
					compareBeyondLimit.add(newGroupBy);
				}
			} else {
				compareToAnalysis.add(groupBy);
				// update the beyondLimit
				if (compareBeyondLimit != null && currentAnalysis.getBeyondLimit().contains(groupBy)) {
					compareBeyondLimit.add(groupBy);
				}
				newGroupBy = groupBy;
			}
		}
		if (hasOverlap) {
			ExpressionAST overlapType = ExpressionMaker.CONSTANT(isCompareOverlap?false:true);
			Axis overlapAxis= new Axis(currentAnalysis.getMainDomain(), overlapType);
			overlapAxis.setOriginType(OriginType.USER);
			overlapAxis.setName("__ispresentperiod");
			compareToAnalysis.add(overlapAxis);
		}
		if (!hasOverlap || isCompareOverlap) {
			compareToAnalysis.setSelection(pastSelection);
		} else {
			compareToAnalysis.setSelection(presentSelection);
		}
		// T1890
		// if (currentAnalysis.hasBeyondLimit()) {// T1042: handling beyondLimit
		compareToAnalysis.setBeyondLimit(compareBeyondLimit);

		Object computeGrowthOption = currentAnalysis.getOption(DashboardAnalysis.COMPUTE_GROWTH_OPTION_KEY);
		boolean computeGrowth = computeGrowthOption != null && computeGrowthOption.equals(true);

		// copy metrics (do it after in order to be able to use the
		// pastInterval)
		for (Measure kpi : currentAnalysis.getKpis()) {
			ExpressionAST kpiExpr = kpi.getDefinitionSafe();
			//Measure presentKpi = new Measure(kpi.getParent(), createMetricOffset(kpiExpr, presentExpression), kpi.getMetric().getId().getObjectId());
			Measure presentKpi = new Measure(kpi);
			if (!hasOverlap) {
				presentKpi.setDefinition(createMetricOffset(kpiExpr, presentExpression));
			}
			presentKpi.setOriginType(kpi.getOriginType());
			presentKpi.setName(kpi.getName());
			presentKpi.setDescription(kpi.getDescription());
			compareToAnalysis.add(presentKpi);
			if (!hasOverlap) {
				//Measure compareToKpi = new Measure(kpi.getParent(), createMetricOffset(kpiExpr, pastExpression), kpi.getMetric().getId().getObjectId()+"_compare");
				Measure compareToKpi = new Measure(kpi);
				compareToKpi.setDefinition(createMetricOffset(kpiExpr, pastExpression));
				compareToKpi.setOriginType(OriginType.COMPARETO);
				compareToKpi.setName(kpi.getName() + " [compare]");
				compareToKpi.setDescription(kpi.getName() + " comparison on " + compareToWhat);
				compareToAnalysis.add(compareToKpi);
				Measure growth = null;
				if (computeGrowth) {
					// add the growth definition...
					growth = new Measure(kpi);
					growth.setDefinition(ExpressionMaker.DIV(ExpressionMaker.MINUS(presentKpi.getDefinitionSafe(), compareToKpi.getDefinitionSafe()), ExpressionMaker.DIV(compareToKpi.getDefinitionSafe(), ExpressionMaker.CONSTANT(100))));
					growth.setOriginType(OriginType.GROWTH);
					growth.setName(kpi.getName() + " [growth%]");
					growth.setFormat("%.2f%%");
					compareToAnalysis.add(growth);
				}
			}
		}
		if (!hasOverlap) {
			for (int ix=0; ix<currentAnalysis.getOrders().size(); ix++) {
				OrderBy orderBy = currentAnalysis.getOrders().get(ix);
				if (orderBy.getExpression() instanceof AxisExpression) {
					for (GroupByAxis groupBy: compareToAnalysis.getGrouping()) {
						if (orderBy.getExpression().equals(new AxisExpression(groupBy.getAxis()))) {
							comparedOrder.put(ij++, new OrderBy(orderBy.getPos(), new AxisExpression(groupBy.getAxis()), orderBy.getOrdering()));
						}
					}
				} else if (orderBy.getExpression() instanceof MeasureExpression) {
					Measure kpi = ((MeasureExpression) orderBy.getExpression()).getMeasure();
					ExpressionAST kpiExpr = kpi.getDefinitionSafe();
					Measure presentKpi = new Measure(kpi);
					presentKpi.setDefinition(createMetricOffset(kpiExpr, presentExpression));
					presentKpi.setOriginType(kpi.getOriginType());
					presentKpi.setName(kpi.getName());
					presentKpi.setDescription(kpi.getDescription());
					//Measure compareToKpi = new Measure(kpi.getParent(), createMetricOffset(kpiExpr, pastExpression), kpi.getMetric().getId().getObjectId()+"_compare");
					Measure compareToKpi = new Measure(kpi);
					compareToKpi.setDefinition(createMetricOffset(kpiExpr, pastExpression));
					compareToKpi.setOriginType(OriginType.COMPARETO);
					compareToKpi.setName(kpi.getName() + " [compare]");
					compareToKpi.setDescription(kpi.getName() + " comparison on " + compareToWhat);
					Measure growth = null;
					if (computeGrowth) {
						// add the growth definition...
						growth = new Measure(kpi);
						growth.setDefinition(ExpressionMaker.DIV(ExpressionMaker.MINUS(presentKpi.getDefinitionSafe(), compareToKpi.getDefinitionSafe()), ExpressionMaker.DIV(compareToKpi.getDefinitionSafe(), ExpressionMaker.CONSTANT(100))));
						growth.setOriginType(OriginType.GROWTH);
						growth.setName(kpi.getName() + " [growth%]");
						growth.setFormat("%.2f%%");
					}

					if (orderBy instanceof OrderByGrowth && ((OrderByGrowth) orderBy).expr.getValue().startsWith("growth(")) {
						comparedOrder.put(ij++, new OrderBy(orderBy.getPos(), new MeasureExpression(growth), orderBy.getOrdering(), NULLS_ORDERING.NULLS_LAST));
						comparedOrder.put(ij++, new OrderBy(orderBy.getPos(), new MeasureExpression(presentKpi), orderBy.getOrdering(), NULLS_ORDERING.NULLS_LAST));
						comparedOrder.put(ij++, new OrderBy(orderBy.getPos(), new MeasureExpression(compareToKpi), orderBy.getOrdering(), NULLS_ORDERING.NULLS_LAST));
					} else if (orderBy instanceof OrderByGrowth && ((OrderByGrowth) orderBy).expr.getValue().startsWith("compareTo(")) {
						comparedOrder.put(ij++, new OrderBy(orderBy.getPos(), new MeasureExpression(compareToKpi), orderBy.getOrdering(), NULLS_ORDERING.NULLS_LAST));
						comparedOrder.put(ij++, new OrderBy(orderBy.getPos(), new MeasureExpression(presentKpi), orderBy.getOrdering(), NULLS_ORDERING.NULLS_LAST));
					} else {
						comparedOrder.put(ij++, new OrderBy(orderBy.getPos(), new MeasureExpression(presentKpi), orderBy.getOrdering(), NULLS_ORDERING.NULLS_LAST));
						comparedOrder.put(ij++, new OrderBy(orderBy.getPos(), new MeasureExpression(compareToKpi), orderBy.getOrdering(), NULLS_ORDERING.NULLS_LAST));
					}
				} else {
					throw new RenderingException("Could not rentder result set ordering");
				}

			}
			compareToAnalysis.setOrders(new ArrayList<OrderBy>(comparedOrder.values()));
		}
		return compareToAnalysis;
	}

	private IntervalleObject alignPastInterval(IntervalleObject presentInterval, IntervalleObject pastInterval,
			Axis joinAxis) throws ScopeException {

		if (joinAxis != null && presentInterval != null && pastInterval != null) {

			Object lowerPresent = presentInterval.getLowerBound();
			Object lowerPast = pastInterval.getLowerBound();
			Object upperPresent = presentInterval.getUpperBound();
			Object upperPast = pastInterval.getUpperBound();
			//
			IDomain image = joinAxis.getDefinition().getImageDomain();
			if (lowerPresent instanceof Date && lowerPast instanceof Date) {

				DateTime lowerPastDT = new DateTime(lowerPast);
				DateTime lowerPresentDT = new DateTime(lowerPresent);
				DateTime upperPresentDT = new DateTime(upperPresent);
				DateTime upperPastDT = new DateTime(upperPast);

				// realign
				if (image.isInstanceOf(IDomain.YEARLY)) {
					// check if present is an exact number of years
					if (lowerPresentDT.getDayOfYear() == 1
							&& upperPresentDT.getDayOfYear() == upperPresentDT.dayOfYear().getMaximumValue()) {
						// check of both periods have the same number of days
						Period presentPeriod = new Period(new LocalDate(lowerPresent), (new LocalDate(upperPresent)),
								PeriodType.days());
						Period pastPeriod = new Period(new LocalDate(lowerPast), (new LocalDate(upperPast)),
								PeriodType.days());
						if (presentPeriod.getDays() == pastPeriod.getDays()) {
							presentPeriod = new Period(new LocalDate(lowerPresent),
									(new LocalDate(upperPresent)).plusDays(1), PeriodType.years());
							pastPeriod = new Period(new LocalDate(lowerPast), (new LocalDate(upperPast)).plusDays(1),
									PeriodType.years());

							// realign
							if (presentPeriod.getYears() > pastPeriod.getYears()) {
								// some days are missing to align the periods
								if (lowerPastDT.getDayOfYear() != 1) {
									// previous period
									Date newLowerPast = new DateTime(upperPastDT.getYear(), 1, 1, 0, 0).toDate();
									return new IntervalleObject(newLowerPast, upperPast);
								}
								if (upperPastDT.getDayOfYear() != upperPastDT.dayOfYear().getMaximumValue()) {
									// year over year
									Date newUpperPast = new DateTime(upperPastDT.getYear(), 12, 31, 23, 59).toDate();
									return new IntervalleObject(lowerPast, newUpperPast);
								}
							} else {
								// either already aligned, or some days should
								// be removed

								if (upperPastDT.getDayOfYear() != upperPastDT.dayOfYear().getMaximumValue()) {
									// year over Year
									Date newUpperPast = new DateTime(upperPastDT.getYear() - 1, 12, 31, 23, 59)
											.toDate();
									return new IntervalleObject(lowerPast, newUpperPast);

								}
								if (lowerPastDT.getDayOfYear() != 1) {
									// previous period
									Date newLowerPast = new DateTime(lowerPastDT.getYear() + 1, 1, 1, 0, 0).toDate();
									return new IntervalleObject(newLowerPast, upperPast);
								}

							}
						}
					}
				} else if (image.isInstanceOf(IDomain.QUARTERLY) || image.isInstanceOf(IDomain.MONTHLY)) {
					// check if present is an exact number of month
					if (lowerPresentDT.getDayOfMonth() == 1
							&& upperPresentDT.getDayOfMonth() == upperPresentDT.dayOfMonth().getMaximumValue()) {
						// check of both periods have the same number of days
						Period presentPeriod = new Period(new LocalDate(lowerPresent), new LocalDate(upperPresent),
								PeriodType.days());
						Period pastPeriod = new Period(new LocalDate(lowerPast), new LocalDate(upperPast),
								PeriodType.days());
						if (presentPeriod.getDays() == pastPeriod.getDays()) {
							// realign
							presentPeriod = new Period(new LocalDate(lowerPresent),
									(new LocalDate(upperPresent)).plusDays(1), PeriodType.months());
							pastPeriod = new Period(new LocalDate(lowerPast), (new LocalDate(upperPast)).plusDays(1),
									PeriodType.months());
							if (presentPeriod.getMonths() > pastPeriod.getMonths()) {
								// some days are missing

								if (upperPastDT.getDayOfMonth() != upperPastDT.dayOfMonth().getMaximumValue()) {
									// month over month
									Date newUpperPast = new DateTime(upperPastDT.getYear(),
											upperPastDT.getMonthOfYear(), upperPastDT.dayOfMonth().getMaximumValue(),
											23, 59).toDate();
									return new IntervalleObject(lowerPast, newUpperPast);
								}

								if (lowerPastDT.getDayOfMonth() != 1) {
									// previous period
									Date newLowerPast = new DateTime(lowerPastDT.getYear(),
											lowerPastDT.getMonthOfYear(), 1, 0, 0).toDate();
									return new IntervalleObject(newLowerPast, upperPast);

								}

							} else {
								// either already aligned, of some days should
								// be removed
								if (upperPastDT.getDayOfMonth() != upperPastDT.dayOfMonth().getMaximumValue()) {
									/// month over month
									if (upperPastDT.getMonthOfYear() == 1) {
										Date newUpperPast = new DateTime(upperPastDT.getYear() - 1, 12, 31, 23, 59)
												.toDate();
										return new IntervalleObject(lowerPast, newUpperPast);

									} else {

										upperPastDT = upperPastDT.minusMonths(1);
										Date newUpperPast = new DateTime(upperPastDT.getYear(),
												upperPastDT.getMonthOfYear(),
												upperPastDT.dayOfMonth().getMaximumValue(), 23, 59).toDate();
										return new IntervalleObject(lowerPast, newUpperPast);
									}
								}
								if (lowerPastDT.getDayOfMonth() != 1) {
									// previous period
									if (lowerPastDT.getMonthOfYear() == 12) {
										Date newLowerPast = new DateTime(lowerPastDT.getYear() + 1, 1, 1, 0, 0)
												.toDate();
										return new IntervalleObject(newLowerPast, upperPast);

									} else {
										lowerPastDT = lowerPastDT.plusMonths(1);
										Date newLowerPast= new DateTime(lowerPastDT.getYear(), lowerPastDT.getMonthOfYear(), 1, 0,0).toDate();
										return new IntervalleObject(newLowerPast, upperPast);

									}

								}

							}
						}
					}
				}
			}
		}
		return pastInterval;
	}

	private boolean compareAxis(Axis x1, Axis x2) {
		DateExpressionAssociativeTransformationExtractor checker = new DateExpressionAssociativeTransformationExtractor();
		ExpressionAST naked1 = checker.eval(x1.getDimension() != null ? x1.getReference() : x1.getDefinitionSafe());
		ExpressionAST naked2 = checker.eval(x2.getDimension() != null ? x2.getReference() : x2.getDefinitionSafe());
		return naked1.equals(naked2);
	}

	private GroupByAxis findGroupingJoin(Axis join, DashboardAnalysis from) {
		DateExpressionAssociativeTransformationExtractor checker = new DateExpressionAssociativeTransformationExtractor();
		ExpressionAST naked1 = checker
				.eval(join.getDimension() != null ? join.getReference() : join.getDefinitionSafe());
		IDomain d1 = join.getDefinitionSafe().getImageDomain();
		for (GroupByAxis groupBy : from.getGrouping()) {
			IDomain d2 = groupBy.getAxis().getDefinitionSafe().getImageDomain();
			if (d1.isInstanceOf(IDomain.TEMPORAL) && d2.isInstanceOf(IDomain.TEMPORAL)) {
				// if 2 dates, try harder...
				// => the groupBy can be a associative transformation of the
				// filter
				ExpressionAST naked2 = checker.eval(groupBy.getAxis().getDefinitionSafe());
				if (naked1.equals(naked2)) {
					return groupBy;
				}
			} else if (join.equals(groupBy.getAxis())) {
				return groupBy;
			}
		}
		// else
		return null;
	}

	private IntervalleObject computeMinMax(Collection<DimensionMember> members) throws ScopeException {
		IntervalleObject result = null;
		for (DimensionMember member : members) {
			Object value = member.getID();
			if (value instanceof IntervalleObject) {
				if (result == null) {
					result = (IntervalleObject) value;
				} else {
					result = result.merge((IntervalleObject) value);
				}
			} else {
				if (result == null) {
					result = new IntervalleObject(value, value);
				} else {
					result = result.include(value);
				}
			}
		}
		return result;
	}

	/**
	 * This method expect to compute a "simple" analysis, that is not requiring
	 * a compareTo operation It supports the BeyondLimit parameter.
	 *
	 * @param analysis
	 * @param optimize
	 * @return
	 * @throws ScopeException
	 * @throws ComputingException
	 * @throws SQLScopeException
	 * @throws InterruptedException
	 * @throws RenderingException
	 */
	private DataMatrix computeAnalysisSimple(DashboardAnalysis analysis, boolean optimize, boolean forceBeyondLimit)
			throws ScopeException, ComputingException, SQLScopeException, InterruptedException, RenderingException {
		// select with one or several KPI groups
		DataMatrix result = null;
		for (int i= 0; i< analysis.getGroups().size(); i++) {
			//
			DataMatrix dm = computeAnalysisSimpleForGroup(analysis, i, optimize, forceBeyondLimit);
			if (dm != null) {

				// merge if needed
				if (result == null) {
					result = dm;
				} else {
					result = result.merge(dm);
				}
			}
		}
		return result;
	}

	private DataMatrix computeAnalysisSimple(DashboardAnalysis analysis, boolean optimize)
			throws ScopeException, ComputingException, SQLScopeException, InterruptedException, RenderingException {
		return computeAnalysisSimple(analysis, optimize, false);
	}

	private DataMatrix runQuery(SimpleQuery query, boolean lazy, DashboardAnalysis analysis, PreviewWriter qw)
			throws ComputingException {
		QueryRunner runner = new QueryRunner(universe.getContext(), query, lazy, qw, analysis.getJobId());
		runner.run();
		return qw.getDataMatrix();
	}

	protected DataMatrix computeAnalysisSimpleForGroupFromSmartCache(DashboardAnalysis analysis, SimpleQuery query,
			AnalysisSmartCacheRequest request, PreviewWriter qw, boolean optimize) throws NotInCacheException {
		// try the smart cache
		long start = System.currentTimeMillis();
		AnalysisSmartCacheMatch match = AnalysisSmartCache.INSTANCE.checkMatch(universe, request);
		if (match != null) {
			boolean lazy = true;
			if (match.getSignature().getRowCount() < 0) {
				// if the DM is not yet available, run a standard query to wait
				lazy = false;
			}
			// need to setup the postprocessing somewhere...
			// restore the query for the match
			try {
				SimpleQuery queryBis = this.genAnalysisQueryCachable(match.getAnalysis(), match.getMeasures(), optimize,
						false);
				try{
					runQuery(queryBis, lazy, analysis, qw);
				}catch(NotInCacheException e){
					logger.info("Could not retrieve matrix, Genkey must be stale");
					AnalysisSmartCache.INSTANCE.remove(match.getSignature());
					throw e;
				}
				if (!lazy) {
					// check that the DM is not too big
					if (!qw.getDataMatrix().isFullset()) {
						throw new NotInCacheException("cannot use this cached datamatrix");
					}
				}
				// add postprocessing
				for (DataMatrixTransform transform : match.getPostProcessing()) {
					queryBis.addPostProcessing(transform);
				}


				// run the postprocessing now so it can fails
				DataMatrix dm = qw.getDataMatrix();
				queryBis.addPostProcessing(new DataMatrixTransformReorganiseColumns(queryBis.getMapper(), query.getMapper()));
				if (dm != null) {
					for (DataMatrixTransform transform : queryBis.getPostProcessing()) {
						dm = transform.apply(dm);
					}
				}
				long end = System.currentTimeMillis();
				logger.info("Smart Cache: HIT! get analysis from Smart Cache in " + (end - start) + "ms");
				// set the smart cache flag
				dm.setFromSmartCache(true);
				return dm;
			} catch (Exception ee) {
				// catch all, we don't want to fail here !
			}
		} else {
			long end = System.currentTimeMillis();
			logger.info("Smart Cache: no Hit: consumed: " + (end - start) + "ms");
		}
		// else
		throw new NotInCacheException("cannot use this cached datamatrix");// for
		// any
		// reason
	}

	protected DataMatrix computeAnalysisSimpleForGroup(DashboardAnalysis analysis, int measureGroupIdx, boolean optimize,
			boolean forceBeyondLimit)
					throws ScopeException, SQLScopeException, ComputingException, InterruptedException, RenderingException {
		SimpleQuery query = null;
		if (analysis.getSelection().hasCompareToSelection()) {

			//Check if compare overlaps
			DashboardSelection presentSelection = analysis.getSelection();
			DomainSelection compare = presentSelection.getCompareToSelection();
			IntervalleObject presentInterval = null;
			IntervalleObject pastInterval = null;
			// compute the joinAxis if exists, i.e. if one of the groupBy dimension is part of the comparison

			for (Axis filter : compare.getFilters()) {
				pastInterval = computeMinMax(compare.getMembers(filter));
				// check if the filter is a join
				Collection<DimensionMember> presentPeriod = presentSelection.getMembers(filter);
				presentPeriod.removeAll(compare.getMembers(filter));
				presentInterval = computeMinMax(presentPeriod);
			}

			Object lowerPresent = presentInterval.getLowerBound();
			Object lowerPast = pastInterval.getLowerBound();
			Object upperPresent = presentInterval.getUpperBound();
			Object upperPast = pastInterval.getUpperBound();
			//
			boolean overlaps = false;
			if (lowerPresent instanceof Date && lowerPast instanceof Date) {
				DateTime lowerPastDT = new DateTime(lowerPast);
				DateTime lowerPresentDT = new DateTime(lowerPresent);
				DateTime upperPresentDT = new DateTime(upperPresent);
				DateTime upperPastDT = new DateTime(upperPast);
				if (upperPastDT.compareTo(lowerPresentDT)>=0 || lowerPastDT.compareTo(upperPresentDT)>=0) {
					overlaps = true;
				}
			} else {
				throw new RenderingException ("Comparison doesn't support non date dimension");
			}
			DashboardAnalysis compareAnalysis = null;
			compareAnalysis = generateAnalysisCompareTo(analysis, false, false);
			query = this.genAnalysisQueryCachable(compareAnalysis, compareAnalysis.getGroups().get(measureGroupIdx), optimize, forceBeyondLimit);
			if (overlaps) {
				query = generateCompareQuery(analysis, measureGroupIdx,  optimize, forceBeyondLimit, compareAnalysis, query.getMapper());
			}
			return computeAnalysisSimpleForGroup(compareAnalysis,  query,  analysis.getGroups().get(measureGroupIdx),  optimize, forceBeyondLimit);
		} else {
			query = this.genAnalysisQueryCachable(analysis, analysis.getGroups().get(measureGroupIdx), optimize, forceBeyondLimit);
			return computeAnalysisSimpleForGroup(analysis,  query,  analysis.getGroups().get(measureGroupIdx),  optimize, forceBeyondLimit);
		}
	}

	protected SimpleQuery generateCompareQuery(final DashboardAnalysis currentAnalysis, int measureGroupIdx, boolean optimize, boolean forceBeyondLimit, final DashboardAnalysis compareAnalysis, QueryMapper qm) throws ScopeException, SQLScopeException, ComputingException, InterruptedException, RenderingException {
		DashboardAnalysis innerAnalysis = generateAnalysisCompareTo(currentAnalysis, true, false);
		DashboardAnalysis innerCompareAnalysis = generateAnalysisCompareTo(currentAnalysis, true, true);
		SimpleQuery inner = this.genAnalysisQueryCachable(innerAnalysis, innerAnalysis.getGroups().get(measureGroupIdx), optimize, forceBeyondLimit);
		SimpleQuery innerCompare = this.genAnalysisQueryCachable(innerCompareAnalysis, innerCompareAnalysis.getGroups().get(measureGroupIdx), optimize, forceBeyondLimit);

		Object computeGrowthOption = currentAnalysis.getOption(DashboardAnalysis.COMPUTE_GROWTH_OPTION_KEY);
		boolean computeGrowth = computeGrowthOption != null && computeGrowthOption.equals(true);

		long limit = inner.getSelect().getStatement().getLimitValue();
		long offset = inner.getSelect().getStatement().getOffsetValue();
		//List<IOrderByPiece> orders = inner.getSelect().getStatement().getOrderByPieces();
		inner.limit(-1);
		inner.getSelect().getStatement().setOrderByPieces(new ArrayList<IOrderByPiece>());
		innerCompare.limit(-1);
		innerCompare.getSelect().getStatement().setOrderByPieces(new ArrayList<IOrderByPiece>());

		SelectUniversal main = new SelectUniversal(inner.getUniverse());
		FromSelectUniversal from = main.from(inner.getSelect());
		from.getSelect().add(innerCompare.getSelect().getStatement());
		main.getScope().put(inner.getSubject(), from);
		main.getStatement().setLimitValue(limit);
		main.getStatement().setOffsetValue(offset);
		main.setForceGroupBy(true);

		List<AxisMapping> axisMappings = new ArrayList<AxisMapping>();
		AxisMapping offsetMapping = null;
		for (AxisMapping ax : inner.getMapper().getAxisMapping()) {
			//ISelectPiece piece = main.select(ax.getAxis().getDefinition(),ax.getPiece().getAlias());
			ISelectPiece piece = ax.getPiece();
			if ("__ispresentperiod".equals(piece.getAlias())) {
				offsetMapping = ax;
			} else {
				SubSelectReferencePiece ref = new SubSelectReferencePiece(from, piece);
				// make sure to use the same alias
				ISelectPiece sel = main.select(ref, piece.getAlias());
				ax.setPiece(sel);// update the mapping... that's dangerous!
				AxisMapping compareAx = qm.find(ax.getAxis());
				axisMappings.add(new AxisMapping(sel, compareAx.getAxis()));
			}
		}
		if (offsetMapping == null) {
			throw new RenderingException("Offset column can't be found");
		}
		IAlias tempTableAlias = new IAlias() {
			@Override
			public String getAlias() {
				return from.getAlias();
			}
		};
		OperatorDefinition opDef = Operators.SUM;
		OperatorDefinition caseDef = Operators.CASE;
		OperatorDefinition notDef = Operators.EQUAL;
		ISelectPiece offsetMappingPiece = offsetMapping.getPiece();
		IPiece notOffsetPiece =new SelectPiece(from.getScope(), new OperatorPiece(notDef,  new IPiece[]{ new SelectPieceReference(tempTableAlias, offsetMappingPiece), new SimpleConstantValuePiece(true,ExtendedType.BOOLEAN)}), null);
		IPiece offsetPiece = new SelectPiece(from.getScope(), new OperatorPiece(notDef,  new IPiece[]{ new SelectPieceReference(tempTableAlias, offsetMappingPiece), new SimpleConstantValuePiece(false,ExtendedType.BOOLEAN)}), null);

		List<MeasureMapping> measureMappings = new ArrayList<MeasureMapping>();
		for (MeasureMapping mx : inner.getMapper().getMeasureMapping()) {
			ExtendedType extendedType = ExtendedType.UNDEFINED;
			if (mx.getPiece() instanceof ITypedPiece) {
				extendedType = ((ITypedPiece)mx.getPiece()).getType();
			}
			IPiece measurePiece =new SelectPieceReference(tempTableAlias, mx.getPiece());
			IPiece[] currentArgs = new IPiece[]{notOffsetPiece, measurePiece};
			IPiece currentPiece = new OperatorPiece(opDef, new IPiece[]{new OperatorPiece(caseDef, currentArgs)}, new ExtendedType[] {extendedType});
			IPiece[] previousArgs = new IPiece[]{offsetPiece, measurePiece};
			IPiece previousPiece = new OperatorPiece(opDef, new IPiece[]{new OperatorPiece(caseDef, previousArgs)}, new ExtendedType[] {extendedType});
			ISelectPiece sel = main.select(currentPiece, mx.getPiece().getAlias());
			ISelectPiece compareSel = main.select(previousPiece, mx.getPiece().getAlias()+"_compare");
			mx.setPiece(sel);
			MeasureMapping compareAx = qm.find(mx.getMapping());
			measureMappings.add(new MeasureMapping(sel, compareAx.getMapping()));
			for (MeasureMapping m:qm.getMeasureMapping()) {
				if (m.getMapping().getId().equals(compareAx.getMapping().getId())) {
					if (m.getMapping().getOriginType() == OriginType.COMPARETO) {
						measureMappings.add(new MeasureMapping(compareSel, m.getMapping()));
					}
				}
			}

			if (computeGrowth) {
				IPiece growthPiece = new OperatorPiece(Operators.DIVIDE,
						new IPiece[]{new ExpressionListPiece(new OperatorPiece(Operators.SUBTRACTION, new IPiece[]{currentPiece, previousPiece}, new ExtendedType[] {extendedType, extendedType})),
								new OperatorPiece(Operators.DIVIDE, new IPiece[] { previousPiece, new SimpleConstantValuePiece(100, ExtendedType.INTEGER)}, new ExtendedType[] {extendedType, ExtendedType.INTEGER})
				}, new ExtendedType[] {extendedType, extendedType});
				ISelectPiece growthSel = main.select(growthPiece, mx.getPiece().getAlias()+"_growth");
				for (MeasureMapping m:qm.getMeasureMapping()) {
					if (m.getMapping().getId().equals(compareAx.getMapping().getId())) {
						if (computeGrowth && m.getMapping().getOriginType() == OriginType.GROWTH) {
							measureMappings.add(new MeasureMapping(growthSel, m.getMapping()));
						}
					}
				}
			}
		}

		SimpleQuery newQuery = new SimpleQuery(inner.getUniverse(), inner.getSubject(), main);
		for (AxisMapping am : axisMappings) {
			newQuery.getMapper().add(am);
		}
		for (MeasureMapping mm : measureMappings) {
			newQuery.getMapper().add(mm);
		}
		/*
		List<OrderBy> newOrderBy = new ArrayList<OrderBy>();
		for (OrderBy orderBy: compareAnalysis.getOrders()) {
			if (orderBy.getExpression() instanceof AxisExpression) {
				SimpleMapping m = newQuery.getMapper().find(orderBy.getExpression());
				if (m != null) {
					System.out.println("couuc");
				}
			}
			newOrderBy.add(new OrderBy(orderBy.getPos(), orderBy.getExpression(), orderBy.getOrdering(), orderBy.getNullsOrdering()));
		}
		 */
		newQuery.orderBy(compareAnalysis.getOrders(), newQuery.getMapper(), main);
		return newQuery;
	}

	/**
	 * Allow to overwrite the query from the default analysis
	 * @param analysis
	 * @param query
	 * @param group
	 * @param optimize
	 * @param forceBeyondLimit
	 * @return
	 * @throws ScopeException
	 * @throws SQLScopeException
	 * @throws ComputingException
	 * @throws InterruptedException
	 * @throws RenderingException
	 */
	protected DataMatrix computeAnalysisSimpleForGroup(DashboardAnalysis analysis, SimpleQuery query, MeasureGroup group, boolean optimize,
			boolean forceBeyondLimit)
					throws ScopeException, SQLScopeException, ComputingException, InterruptedException, RenderingException {
		// generate the query
		PreviewWriter qw = new PreviewWriter();

		// compute the signature: do it after generating the query to take into
		// account side-effects
		boolean smartCache = SUPPORT_SMART_CACHE && !analysis.hasRollup();
		AnalysisSmartCacheRequest smartCacheRequest = smartCache
				? new AnalysisSmartCacheRequest(universe, analysis, group, query) : null;
				boolean temporarySignature = false;
				try {
					// run the query using 1/ first the lazy, 2/ the smart cache (if
					// allowed) 3/ direct execution if not lazy
					try {
						// always try lazy first
						runQuery(query, true/* lazy */, analysis, qw);
					} catch (NotInCacheException e) {
						if (smartCacheRequest != null) {
							try {
								return computeAnalysisSimpleForGroupFromSmartCache(analysis, query, smartCacheRequest, qw,
										optimize);
							} catch (NotInCacheException ee) {
								// ignore any error in smartCache
							}
						}
						// still not yet, shall we run it?
						if (!analysis.isLazy()) {
							// if smart-cache enabled, lets keep a forward reference
							// so other can use the smart-cache even if it is not yet
							// computed
							if (smartCacheRequest != null) {
								temporarySignature = AnalysisSmartCache.INSTANCE.put(smartCacheRequest);
							}
							runQuery(query, false, analysis, qw);
						} else {
							throw e;// throw the NotInCache exception
						}

					}
					// if we get here it's that we ran the query, not from the
					// SmartCache
					DataMatrix dm = qw.getDataMatrix();
					if (dm != null) {
						for (DataMatrixTransform transform : query.getPostProcessing()) {
							dm = transform.apply(dm);
						}
					}
					// if it is a full dataset and no rollup, store the layout in the
					// smartCache
					if (smartCacheRequest != null) {
						if (dm.isFullset() && !analysis.hasRollup()) {
							smartCacheRequest.setRowCount(dm);// record the resultset
							// size - so we know the
							// resultset should be
							// available
							if (dm.isFromCache() && !dm.isFromSmartCache()) {
								// from cache, but is it still in the smartCache ?
								if (!AnalysisSmartCache.INSTANCE.contains(smartCacheRequest)) {
									AnalysisSmartCache.INSTANCE.put(smartCacheRequest);
									logger.info("updating analysis in Smart Cache (key="+dm.getRedisKey()+")");
								}
							} else {
								// add to the smart cache
								AnalysisSmartCache.INSTANCE.put(smartCacheRequest);
								logger.info("put analysis in Smart Cache (key="+dm.getRedisKey()+")\"");
							}
						}
					}
					return dm;
				} finally {
					if (smartCacheRequest != null && temporarySignature) {
						// make sure to remove the temporary signature from cache
						// AnalysisSmartCache.INSTANCE.remove(smartCacheRequest);
						AnalysisSmartCacheSignature sign = AnalysisSmartCache.INSTANCE.get(smartCacheRequest);
						if (sign != null && sign.getRowCount() < 0) {
							// this is the temporary entry
							AnalysisSmartCache.INSTANCE.remove(smartCacheRequest);
						}
					}
				}
	}

	/**
	 * execute the analysis but does not read the result: this method can be
	 * used to stream the result back to client, for instance to export the
	 * dataset
	 *
	 * @param analysis
	 * @return
	 * @throws ComputingException
	 * @throws InterruptedException
	 */
	public void executeAnalysis(DashboardAnalysis analysis, QueryWriter writer, boolean lazy)
			throws ComputingException, InterruptedException {
		try {
			long start = System.currentTimeMillis();
			logger.info("start of sql generation");

			List<MeasureGroup> groups = analysis.getGroups();
			if (groups.isEmpty()) {
				SimpleQuery query = this.genSimpleQuery(analysis);

				long stop = System.currentTimeMillis();
				// logger.info("End of sql generation in " +(stop-start)+ "ms"
				// );
				logger.info("task=" + this.getClass().getName() + " method=executeAnalysis.SQLGeneration" + " duration="
						+ (stop - start) + " error=false status=done");
				try {
					String sql = query.render();
					SQLStats queryLog = new SQLStats(query.toString(), "executeAnalysis.SQLGeneration", sql,
							(stop - start), analysis.getUniverse().getProject().getId().getProjectId());
					queryLog.setError(false);
					//					PerfDB.INSTANCE.save(queryLog);

				} catch (RenderingException e) {
					e.printStackTrace();
				}

				QueryRunner runner = new QueryRunner(universe.getContext(), query, lazy, writer, analysis.getJobId());
				runner.run();

			} else {
				// possible only if there is only one group
				if (groups.size() != 1) {
					throw new ComputingException(
							"the analysis cannot be exported in a single query - try removing some metrics");
				}
				// select with one or several KPI groups
				//
				MeasureGroup group = groups.get(0);
				//
				SimpleQuery query = genAnalysisQueryWithSoftFiltering(analysis, group, false, false);
				//
				QueryRunner runner = new QueryRunner(universe.getContext(), query, lazy, writer, analysis.getJobId());
				runner.run();
			}
		} catch (ScopeException e) {
			throw new ComputingException(e);
		} catch (SQLScopeException e) {
			throw new ComputingException(e);
		}
	}

	/**
	 * Generate a simple query to compute an analysis on a single measure group
	 * (i.e. on a single domain) If optimize is true, the method will try to use
	 * soft filters.
	 *
	 * @param if
	 *            cachable is true, the genAnalysisQuery will try to normalize
	 *            the SQL - that means it may ignore some statements like ORDER
	 *            BY or select column order...
	 * @throws InterruptedException
	 */
	protected SimpleQuery genAnalysisQueryWithSoftFiltering(DashboardAnalysis analysis, MeasureGroup group,
			boolean cachable, boolean optimize)
					throws ScopeException, SQLScopeException, ComputingException, InterruptedException {
		//
		DashboardSelection soft_filters = new DashboardSelection();
		List<Axis> hidden_slice = new ArrayList<Axis>();
		//
		Collection<Domain> domains = analysis.getAllDomains();
		//
		int slice_numbers = 0;
		float row_estimate = 1;
		Measure master = group.getMaster();
		SimpleQuery query = new SimpleQuery(master.getParent());
		//
		// check if we can automatically order the query by dimensions
		// only true if the result is cachable (no export) and if there is no
		// limit defined (so the order doesn't modify the resultset)
		// and there is no specific order request
		boolean defaultOrder = !analysis.hasOrderBy() && !analysis.hasLimit() && cachable;
		//
		// combine the axis first
		HashSet<Axis> slices = new HashSet<Axis>();
		for (GroupByAxis groupBy : analysis.getGrouping()) {
			Domain target = groupBy.getAxis().getParent().getRoot();
			if (!domains.contains(target)) {
				List<String> names = new ArrayList<String>(domains.size());
				for (Domain domain : domains) {
					names.add(domain.getName());
				}
				throw new ScopeException("the Axis '" + groupBy.getAxis().prettyPrint()
						+ "' is incompatible with the query scope " + names);
			}
			//
			Space hook = computeSinglePath(analysis, master, groupBy.getAxis().getParent().getTop(), mandatory_link);
			//
			Axis axis = hook.A(groupBy.getAxis());

			if (axis.getDimension() !=null){
				List<Attribute> attributes = AttributeServiceBaseImpl.getInstance().readAll(universe.getContext(), axis.getDimension().getId());
				if (attributes != null) {
					for (Attribute attr: attributes) {
						if (attr.getId().getAttributeId().equals("precomputedRollupLevels")) {
							query.setPrecomputedRollupAxis(groupBy);
						}
					}
				}
			}
			ISelectPiece piece = query.select(axis);
			if (defaultOrder)
				query.orderBy(piece, ORDERING.DESCENT);
			slice_numbers++;
			{
				float size = axis.getEstimatedSize();// getMembers().size();
				if (size > 0)
					row_estimate = row_estimate * size;
			}
			slices.add(axis);
		}
		// krkn-59
		if (analysis.hasRollup()) {
			query.rollUp(analysis.getRollup(), analysis.isRollupGrandTotal());
		}
		//
		// add the selection
		for (DomainSelection selection : analysis.getSelection().get()) {
			// handles conditions
			if (selection.hasConditions()) {
				for (ExpressionInput condition : selection.getConditions()) {
					query.where(condition.getExpression());
				}
			}
			// handles members
			for (Axis axis : selection.getFilters()) {
				Collection<DimensionMember> filters = selection.getMembers(axis);
				Dimension dimension = axis.getDimension();
				if (!optimize) {
					query.where(axis, filters);
				} else {
					if ((dimension.getType().equals(Type.CATEGORICAL) || dimension.getType().equals(Type.INDEX) // ticket:3001
							) && slices.contains(axis)) {
						// analysis already contains the axis filter as a slice
						if (soft_filters != null) {
							soft_filters.add(axis, filters);
						}
					} else {
						// ok, we can decide to slice then filter instead of
						// direct filtering...
						boolean generalize = false;
						IDomain image = axis.getDefinition().getImageDomain();
						if (!image.isInstanceOf(SetDomain.SET) && !image.isInstanceOf(IDomain.CONDITIONAL) // ticket:3014
								// -
								// not
								// for
								// predicate
								) {
							if (slice_numbers < 10 && filters.size() == 1
									&& dimension.getType().equals(Type.CATEGORICAL)) {
								// limited to the situation where the filter
								// applies to only ONE value
								// this is to avoid side effect with
								// non-associative operators (AVG, MIN, MAX...)
								float size = axis.getEstimatedSize();
								if (size < 10000 && row_estimate * size < 200000) {
									generalize = true;
									slice_numbers++;
									row_estimate = row_estimate * size;
								} else {
									// we can use a partition approach ?
									// => this is not that clear... we should
									// not filter on he ID (it will require to
									// inline a IN statement)
									// => and using the Index is not that
									// simple...
								}
							}
						}
						// slice or filter...
						if (generalize) {
							ISelectPiece axisP = query.select(axis);
							if (defaultOrder)
								query.orderBy(axisP, ORDERING.DESCENT);
							if (hidden_slice != null) {
								hidden_slice.add(axis);
							}
							if (soft_filters != null) {
								soft_filters.add(axis, filters);
							}
						} else {
							query.where(axis, filters);
						}
					}
				}
			}
		}
		// softfiltering
		if (!soft_filters.isEmpty() || !hidden_slice.isEmpty()) {
			query.addPostProcessing(new DataMatrixTransformHideColumns<Axis>(hidden_slice));
			query.addPostProcessing(new DataMatrixTransformSoftFilter(soft_filters));
		}
		//
		// add the metrics
		for (Measure buddy : group.getKPIs()) {
			query.select(buddy);
		}
		//
		if (analysis.hasLimit()) {
			query.limit(analysis.getLimit());
		}
		if (!defaultOrder) {
			if (analysis.hasOrderBy()) {
				query.orderBy(analysis.getOrders());
			}
			if (analysis.hasOffset()) {
				query.offset(analysis.getOffset());
			}
		}
		//
		return query;
	}

	/**
	 * generate a simple query without metrics
	 *
	 * @param analysis
	 * @return
	 * @throws ScopeException
	 * @throws SQLScopeException
	 * @throws ComputingException
	 * @throws InterruptedException
	 */
	protected SimpleQuery genSimpleQuery(DashboardAnalysis analysis)
			throws ScopeException, SQLScopeException, ComputingException, InterruptedException {
		if (analysis.getMainDomain() == null) {
			throw new ComputingException("if no kpi is defined, must have one single domain");
		}
		Space root = analysis.getMainDomain();
		// create the Operator
		SimpleQuery query = new SimpleQuery(root);
		for (GroupByAxis item : analysis.getGrouping()) {
			Space hook = computeSinglePath(analysis, root.getDomain(), item.getAxis().getParent().getTop(),
					mandatory_link);
			Axis axis = hook.A(item.getAxis());
			query.select(axis);
		}
		query.getSelect().getGrouping().setForceGroupBy(true);
		//
		for (DomainSelection selection : analysis.getSelection().get()) {
			if (selection.hasConditions()) {
				for (ExpressionInput condition : selection.getConditions()) {
					query.where(condition.getExpression());
				}
			}
			for (Axis filter : selection.getFilters()) {
				query.where(filter, selection.getMembers(filter));
			}
		}

		// krkn-59: rollup
		// => do not add the rollup since there is no KPI to compute...
		//
		if (analysis.hasLimit()) {
			query.limit(analysis.getLimit());
		}
		if (analysis.hasOffset()) {
			query.offset(analysis.getOffset());
		}
		if (analysis.hasOrderBy()) {
			query.orderBy(analysis.getOrders());
		}

		return query;
	}

	/**
	 * generate a Simple Query without using soft-filters
	 *
	 * @param analysis
	 * @param group
	 * @param optimize
	 * @return
	 * @throws ScopeException
	 * @throws SQLScopeException
	 * @throws ComputingException
	 * @throws InterruptedException
	 * @throws RenderingException
	 */
	protected SimpleQuery genAnalysisQuery(DashboardAnalysis analysis, MeasureGroup group, boolean optimize,
			boolean forceBeyondLimit)
					throws ScopeException, SQLScopeException, ComputingException, InterruptedException, RenderingException {
		return this.genAnalysisQueryCachable(analysis, group, optimize, forceBeyondLimit);
	}

	protected SimpleQuery genAnalysisQuery(DashboardAnalysis analysis, MeasureGroup group, boolean optimize)
			throws ScopeException, SQLScopeException, ComputingException, InterruptedException, RenderingException {
		return this.genAnalysisQueryCachable(analysis, group, optimize, false);
	}

	protected SimpleQuery genAnalysisQueryCachable(DashboardAnalysis analysis, MeasureGroup group, boolean optimize,
			boolean forceBeyondLimit)
					throws ScopeException, SQLScopeException, ComputingException, InterruptedException, RenderingException {
		if (forceBeyondLimit || (analysis.hasBeyondLimit() && analysis.hasLimit() && !analysis.hasRollup())) {
			// need to take care of the beyond limit axis => compute the limit
			// only on a subset of axes
			SimpleQuery check = genAnalysisQueryWithBeyondLimitSupport(analysis, group, true, optimize);
			// check is null if cannot apply beyondLimit
			if (check != null)
				return check;
		}
		// else...
		// use the simple method
		return genAnalysisQueryWithSoftFiltering(analysis, group, true, // just
				// set
				// the
				// cachable
				// flag
				// to
				// true
				// -- is
				// this
				// really
				// usefull?
				optimize);
	}

	/**
	 * handling the BeyondLimit parameter Note: rollup not yet supported
	 *
	 * @param analysis
	 * @param group
	 * @param cachable
	 * @param optimize
	 * @param soft_filters
	 * @param hidden_slice
	 * @return the SimpleQuery or null if not applicable
	 * @throws ScopeException
	 * @throws SQLScopeException
	 * @throws ComputingException
	 * @throws InterruptedException
	 * @throws RenderingException
	 */
	protected SimpleQuery genAnalysisQueryWithBeyondLimitSupport(DashboardAnalysis analysis, MeasureGroup group,
			boolean cachable, boolean optimize)
					throws ScopeException, SQLScopeException, ComputingException, InterruptedException, RenderingException {
		//
		// T1890: it is ok to have null beyondLimit => apply to all pivot
		List<GroupByAxis> beyondLimitGroup = analysis.getBeyondLimit()!=null?analysis.getBeyondLimit():Collections.<GroupByAxis>emptyList();
		// prepare the sub-query that will count the limit
		DashboardAnalysis subAnalysisWithLimit = new DashboardAnalysis(analysis.getUniverse());
		// copy dimensions
		ArrayList<Axis> joins = new ArrayList<>();
		for (GroupByAxis groupBy : analysis.getGrouping()) {
			if (!beyondLimitGroup.contains(groupBy)) {
				subAnalysisWithLimit.add(groupBy);
				joins.add(groupBy.getAxis());
			} else {
				// exclude from the analysis
			}
		}
		if (subAnalysisWithLimit.getGrouping().isEmpty()) {//
			// just unset the limit
			analysis.noLimit();
			analysis.noOffset();

			return genAnalysisQueryWithSoftFiltering(analysis, group, cachable, optimize);
		}
		// copy metrics
		for (Measure measure : analysis.getKpis()) {
			subAnalysisWithLimit.add(measure);
		}
		// copy orders
		ArrayList<ExpressionAST> exclude = new ArrayList<>();
		DateExpressionAssociativeTransformationExtractor extractor = new DateExpressionAssociativeTransformationExtractor();
		for (GroupByAxis slice : beyondLimitGroup) {
			exclude.add(extractor.eval(slice.getAxis().getDefinitionSafe()));
		}
		for (OrderBy order : analysis.getOrders()) {
			ExpressionAST naked = extractor.eval(order.getExpression());
			if (!exclude.contains(naked)) {
				subAnalysisWithLimit.orderBy(order);
			}
		}
		// copy stuff
		if (analysis.hasLimit())
			subAnalysisWithLimit.limit(analysis.getLimit());
		if (analysis.hasOffset())
			subAnalysisWithLimit.offset(analysis.getOffset());
		if (analysis.isRollupGrandTotal())
			subAnalysisWithLimit.setRollupGrandTotal(true);
		if (analysis.hasRollup())
			subAnalysisWithLimit.setRollup(analysis.getRollup());
		// copy selection
		if (analysis.getBeyondLimitSelection() != null) {
			subAnalysisWithLimit.setSelection(new DashboardSelection(analysis.getBeyondLimitSelection()));
		} else {
			subAnalysisWithLimit.setSelection(new DashboardSelection(analysis.getSelection()));
		}
		// use the best strategy
		if (joins.size() == 1 && subAnalysisWithLimit.hasLimit() && subAnalysisWithLimit.getLimit() < 50) {
			// run sub-analysis and add filters by hand
			// potential cache hit on the subquery
			DataMatrix selection = computeAnalysisSimple(subAnalysisWithLimit, false);
			Axis join = joins.get(0);
			Collection<DimensionMember> values = selection.getAxisValues(join);
			if (!values.isEmpty()) {
				analysis.getLimit();
				// change the analysis definition
				// => we need to define the join condition explicitly for
				// SmartCache to correctly pick it
				analysis.noLimit();
				analysis.noOffset();

				analysis.getSelection().add(join, values);
				SimpleQuery mainquery = genAnalysisQueryWithSoftFiltering(analysis, group, cachable, optimize);
				// analysis.limit(limit);// restore the limit in case we need it
				// again (compare for example)
				// mainquery.where(join, values);
				return mainquery;
			} else {
				// failed, using original limit
				return genAnalysisQueryWithSoftFiltering(analysis, group, cachable, optimize);
			}
		} else {
			// generate a subquery and use EXISTS operator
			// -- do not optimize, we don't want side effect here
			SimpleQuery subquery = genAnalysisQueryWithSoftFiltering(subAnalysisWithLimit, group, cachable, false);
			//
			// get the original query without limit
			analysis.noLimit();
			analysis.noOffset();

			SimpleQuery mainquery = genAnalysisQueryWithSoftFiltering(analysis, group, cachable, optimize);
			//
			mainquery.join(joins, subquery);
			return mainquery;
		}
	}

	private Space computeSinglePath(Dashboard dashboard, Domain root, Space target, boolean mandatory)
			throws ScopeException, ComputingException {
		Space single_space = null;
		List<Space> paths = computePaths(root, target.getRoot());
		if (paths.isEmpty()) {
			if (mandatory) {
				throw new ScopeException("unable to link domain '" + root.getName() + "' with that Filter");
			} else {
				logger.warn(
						"ignoring axis '" + target.getPath() + "' from the selection, cannot resolve to a valid path");
			}
		} else {
			single_space = paths.get(0);// hum, ok for now...
		}
		if (single_space != null) {
			return single_space.S(target);
		} else {
			return null;
		}
	}

	private Space computeSinglePath(Dashboard dashboard, Measure measure, Space target, boolean mandatory)
			throws ScopeException, ComputingException {
		Space single_space = null;
		if (target.equals(dashboard.getMainDomain())) {
			// try to figure out if there is something possible
			Domain root = measure.getParent().getRoot();
			List<Space> paths = computePaths(root, target.getRoot());
			if (paths.isEmpty() && mandatory) {
				throw new ScopeException("unable to link KPI '" + measure.getName() + "' to the timeline");
			}
			single_space = paths.get(0);// hum, ok for now...
		} else {
			Domain root = measure.getParent().getRoot();
			List<Space> paths = computePaths(root, target.getRoot());
			if (paths.isEmpty()) {
				if (mandatory) {
					throw new ScopeException("unable to link KPI '" + measure.getName() + "' with that Filter");
				} else {
					logger.warn("ignoring axis '" + target.getPath()
					+ "' from the selection, cannot resolve to a valid path");
				}
			} else {
				single_space = paths.get(0);// hum, ok for now...
			}
		}
		if (single_space != null) {
			return single_space.S(target);
		} else {
			return null;
		}
	}

	private List<Space> computePaths(Domain root, Domain target) throws ScopeException, ComputingException {
		LinkedList<Space> paths = new LinkedList<Space>();
		Space black_hole = universe.S(root);
		if (black_hole.getDomain().equals(target)) {
			paths.add(black_hole);
		}
		// first check if there is a direct path
		for (Space space : black_hole.S()) {
			if (space.getDomain().equals(target)) {
				paths.add(space);
			}
		}
		if (paths.isEmpty()) {
			// go deeper
			/*
			 * for (Space space : black_hole.S()) { List<Space> subpaths =
			 * computePaths(space.getDomain(), target); for (Space subspace :
			 * subpaths) { paths.add(space.S(subspace)); } }
			 */
			return universe.getCartography().getAllPaths(universe, root, target);
		}
		return paths;
	}

}
