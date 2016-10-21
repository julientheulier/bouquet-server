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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.api.core.PerfDB;
import com.squid.kraken.v4.api.core.SQLStats;
import com.squid.kraken.v4.caching.NotInCacheException;
import com.squid.kraken.v4.caching.redis.RedisCacheException;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.concurrent.ExecutionManager;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.set.SetDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.IOrderByPiece.ORDERING;
import com.squid.core.sql.render.ISelectPiece;
import com.squid.core.sql.render.RenderingException;
import com.squid.kraken.v4.core.analysis.datamatrix.AxisValues;
import com.squid.kraken.v4.core.analysis.datamatrix.CompareMerger;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.engine.query.QueryRunner;
import com.squid.kraken.v4.core.analysis.engine.query.SimpleQuery;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.AxisMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.MeasureMapping;
import com.squid.kraken.v4.core.analysis.model.Dashboard;
import com.squid.kraken.v4.core.analysis.model.DashboardAnalysis;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.model.DomainSelection;
import com.squid.kraken.v4.core.analysis.model.ExpressionInput;
import com.squid.kraken.v4.core.analysis.model.GroupByAxis;
import com.squid.kraken.v4.core.analysis.model.IntervalleObject;
import com.squid.kraken.v4.core.analysis.model.MeasureGroup;
import com.squid.kraken.v4.core.analysis.model.OrderBy;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Property.OriginType;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
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
		} else if (analysis.getSelection().hasCompareToSelection()) {
			return computeAnalysisCompareTo(analysis);
		} else {
			// disable the optimizing when using the limit feature
			boolean optimize = SUPPORT_SOFT_FILTERS && !analysis.hasLimit() && !analysis.hasOffset()
					&& !analysis.hasRollup();
			return computeAnalysisSimple(analysis, optimize);
		}
	}

	// handle compare T947
	public DataMatrix computeAnalysisCompareTo(final DashboardAnalysis currentAnalysis)
			throws ScopeException, ComputingException, SQLScopeException, InterruptedException, RenderingException {
		// preparing the selection
		DashboardSelection presentSelection = currentAnalysis.getSelection();
		DomainSelection compare = presentSelection.getCompareToSelection();
		Axis joinAxis = null;
		IntervalleObject presentInterval = null;
		IntervalleObject pastInterval = null;
		for (Axis filter : compare.getFilters()) {
			// check if the filter is a join
			GroupByAxis groupBy = findGroupingJoin(filter, currentAnalysis);
			if (groupBy != null) {
				if (joinAxis != null) {
					throw new ScopeException("only one join axis supported");
				}
				joinAxis = groupBy.getAxis();
				// compute the min & max for present
				Collection<DimensionMember> members = presentSelection.getMembers(filter);
				presentInterval = computeMinMax(members);
			}
		}
		//
		// handling orderBy in the proper way...
		final List<OrderBy> fixed = new ArrayList<>();
		List<OrderBy> remaining = new ArrayList<>();
		int i = 0;
		// list the dimensions
		ArrayList<ExpressionAST> dimensions = new ArrayList<>();// order matter
		for (GroupByAxis group : currentAnalysis.getGrouping()) {
			dimensions.add(group.getAxis().getReference());
		}
		// use this list to compute the dimensioni index
		ArrayList<ExpressionAST> dimensionIndexes = new ArrayList<>(dimensions);
		// will hold in which order to merge
		int[] mergeOrder = new int[dimensions.size()];
		// rebuild the full order specs
		List<OrderBy> originalOrders = currentAnalysis.getOrders();
		for (OrderBy order : originalOrders) {
			if (i == 0 && joinAxis != null) {
				if (order.getExpression().equals(joinAxis.getReference())) {
					// ok, it's first
					fixed.add(new OrderBy(i, order.getExpression(), order.getOrdering()));
					mergeOrder[i++] = dimensionIndexes.indexOf(order.getExpression());
					continue;// done for this order
				} else {
					// add it first, default to DESC because it is the join (date)
					fixed.add(new OrderBy(i, joinAxis.getReference(), ORDERING.DESCENT));
					mergeOrder[i++] = dimensionIndexes.indexOf(joinAxis.getReference());
					// now we need to take care of the order
				}
			}
			// not (the first & join)...
			// check if it is a dimension
			if (dimensions.contains(order.getExpression())) {
				// ok, just add it
				fixed.add(new OrderBy(i, order.getExpression(), order.getOrdering()));
				mergeOrder[i++] = dimensionIndexes.indexOf(order.getExpression());
				// and remove the dimension from the list
				dimensions.remove(order.getExpression());
			} else {
				// assuming it is a metric or something else, keep it but at the
				// end
				remaining.add(order);// don't know the position yet
			}
		}
		// add missing dimensions
		if (!dimensions.isEmpty()) {
			for (ExpressionAST dim : dimensions) {
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
		if (originalOrders.isEmpty() && currentAnalysis.getGrouping().size()>=1) {
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
		// copy dimensions
		/*
		 * ArrayList<GroupByAxis> compareBeyondLimit =
		 * currentAnalysis.hasBeyondLimit() ? new ArrayList<GroupByAxis>() :
		 * null;
		 */
		ArrayList<GroupByAxis> compareBeyondLimit = currentAnalysis.hasBeyondLimit() ? new ArrayList<GroupByAxis>()
				: null;
		for (GroupByAxis groupBy : currentAnalysis.getGrouping()) {
			if (groupBy.getAxis().equals(joinAxis)) {
				Axis compareToAxis = new Axis(groupBy.getAxis());
				compareToAxis.setOriginType(OriginType.COMPARETO);
				compareToAxis.setName(groupBy.getAxis().getName() + " [compare]");
				GroupByAxis compareToGroupBy = compareToAnalysis.add(compareToAxis, groupBy.isRollup());
				compareToGroupBy.setRollupPosition(groupBy.getRollupPosition());
				// update the beyondLimit
				if (compareBeyondLimit != null && currentAnalysis.getBeyondLimit().contains(groupBy)) {
					compareBeyondLimit.add(compareToGroupBy);
				}
			} else {
				compareToAnalysis.add(groupBy);
				// update the beyondLimit
				if (compareBeyondLimit != null && currentAnalysis.getBeyondLimit().contains(groupBy)) {
					compareBeyondLimit.add(groupBy);
				}
			}
		}

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
		for (Axis filter : compare.getFilters()) {
			pastSelection.clear(filter);
			pastSelection.add(filter, compare.getMembers(filter));
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
			IntervalleObject isInterval = computeMinMax(compare.getMembers(filter));
			if (!compareToWhat.equals(""))
				compareToWhat += " and ";
			if (isInterval != null) {
				compareToWhat += isInterval.toString();
			} else {
				compareToWhat += "[" + (compare.getMembers(filter)).toString() + "]";
			}
		}
		compareToAnalysis.setSelection(pastSelection);
		// T1890
		// if (currentAnalysis.hasBeyondLimit()) {// T1042: handling beyondLimit
		compareToAnalysis.setBeyondLimit(compareBeyondLimit);
		// use the present selection to compute
		
		if (compareBeyondLimit  == null && joinAxis !=null){
			compareBeyondLimit = new ArrayList<>();
			compareBeyondLimit.add(new GroupByAxis(joinAxis));
			compareToAnalysis.setBeyondLimit(compareBeyondLimit);
		}
		compareToAnalysis.setBeyondLimitSelection(presentSelection);
		
		// }

		// copy metrics (do it after in order to be able to use the
		// pastInterval)
		for (Measure kpi : currentAnalysis.getKpis()) {
			Measure compareToKpi = new Measure(kpi);
			compareToKpi.setOriginType(OriginType.COMPARETO);
			compareToKpi.setName(kpi.getName() + " [compare]");
			compareToKpi.setDescription(kpi.getName() + " comparison on " + compareToWhat);
			compareToAnalysis.add(compareToKpi);
		}

		// compute present & past in //
		Future<DataMatrix> future = ExecutionManager.INSTANCE.submit(universe.getContext().getCustomerId(),
				new Callable<DataMatrix>() {
					@Override
					public DataMatrix call() throws Exception {
						DataMatrix present = computeAnalysisSimple(currentAnalysis, false);
						present.orderBy(fixed);
						return present;
					}
				});
		try {
			// compute past
			DataMatrix past = computeAnalysisSimple(compareToAnalysis, false, true);
			past.orderBy(fixed);
			// wait for present
			DataMatrix present = future.get();

			//
			final Period offset = computeOffset(present, joinAxis, presentInterval, pastInterval);
			//
			Object computeGrowthOption = currentAnalysis.getOption(DashboardAnalysis.COMPUTE_GROWTH_OPTION_KEY);
			boolean computeGrowth = computeGrowthOption != null && computeGrowthOption.equals(true);
			// T1890: the present & past matrices are sorted in post-processing according to the [fixed] order
			// the CompareMerger only works if the matrices are fully sorted
			CompareMerger merger = new CompareMerger(present, past, mergeOrder, joinAxis, offset, computeGrowth);
			DataMatrix debug = merger.merge(false);
			// apply the original order by directive (T1039)
			if (!originalOrders.isEmpty()) {
				debug.orderBy(originalOrders);
			}
			// T1897 - enforce limit & offset
			if (currentAnalysis.hasLimit() && debug.getRows().size() > currentAnalysis.getLimit()) {
				DataMatrixTransformTruncate truncate = new DataMatrixTransformTruncate(currentAnalysis.getLimit(),
						currentAnalysis.getOffset());
				debug = truncate.apply(debug);
			}
			return debug;
		} catch (ExecutionException e) {
			if (e.getCause() != null) {
				if (e.getCause() instanceof RedisCacheException) {
					throw (RedisCacheException) e.getCause();
				}
				if (e.getCause() instanceof NotInCacheException) {
					throw (NotInCacheException) e.getCause();
				}
				if (e.getCause() instanceof ComputingException) {
					throw (ComputingException) e.getCause();
				}
			}
			throw new ComputingException(e.getCause());
		}
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

				DateTime lowerPastDT = new DateTime((Date) lowerPast);
				DateTime lowerPresentDT = new DateTime((Date) lowerPresent);
				DateTime upperPresentDT = new DateTime((Date) upperPresent);
				DateTime upperPastDT = new DateTime((Date) upperPast);

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
				} else if (image.isInstanceOf(IDomain.QUATERLY) || image.isInstanceOf(IDomain.MONTHLY)) {
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

	private Period computeOffset(DataMatrix present, Axis joinAxis, IntervalleObject presentInterval,
			IntervalleObject pastInterval) throws ScopeException {
		if (joinAxis == null || presentInterval == null || pastInterval == null) {
			return null;
		} else {
			AxisValues check = present.find(joinAxis);
			if (check == null) {
				return null;// no need to bother
			} else {
				return computeOffset(presentInterval, pastInterval, check);
			}
		}
	}

	private Period computeOffset(IntervalleObject presentInterval, IntervalleObject pastInterval, AxisValues joinAxis)
			throws ScopeException {
		// it is better to compare on the lower bound because alignment on the
		// end of month is not accurate
		Object present = presentInterval.getLowerBound();
		Object past = pastInterval.getLowerBound();
		//

		IDomain image = joinAxis.getAxis().getDefinition().getImageDomain();
		PeriodType type = computePeriodType(image);
		//
		if (present instanceof Date && past instanceof Date) {

			Period presentPeriod = new Period(new LocalDate(((Date) presentInterval.getLowerBound()).getTime()),
					new LocalDate(((Date) presentInterval.getUpperBound()).getTime()).plusDays(1), type);

			Period pastPeriod = new Period(new LocalDate(((Date) pastInterval.getLowerBound()).getTime()),
					new LocalDate(((Date) pastInterval.getUpperBound()).getTime()).plusDays(1), type);

			Date pastDate = (Date) past;
			DateTime dt = new DateTime(pastDate);
			if (image.isInstanceOf(IDomain.YEARLY)) {
				if (presentPeriod.getYears() > pastPeriod.getYears()) {
					// e.g. presentPeriod of 365 days ->
					// presentPeriod.getYears=1 && past year of 366 days ->
					// pastPeriod.getYears=0
					DateTime newDT = new DateTime(dt.getYear(), 1, 1, 0, 0);
					pastDate = newDT.toDate();
				} else {
					if (dt.getDayOfYear() != 1) {
						// e.g present period of 366 days -> past date at dec 31
						DateTime newDT = new DateTime(dt.getYear() + 1, 1, 1, 0, 0);
						pastDate = newDT.toDate();
					}
				}
			} else if (image.isInstanceOf(IDomain.QUATERLY) || image.isInstanceOf(IDomain.MONTHLY)) {

				if (presentPeriod.getMonths() > pastPeriod.getMonths()) {
					// e.g present period of 28 days(February) ->
					// pastPeriod.getMonths() = 0 (January has 31 days)
					DateTime newDT = new DateTime(dt.getYear(), dt.getMonthOfYear(), 1, 0, 0);
					pastDate = newDT.toDate();
				} else {
					if (dt.getDayOfMonth() != 1) {
						// e.g. present period of 31 day(March) pastDate = Feb 6
						if (dt.getMonthOfYear() == 12) {
							DateTime newDT = new DateTime(dt.getYear() + 1, 1, 1, 0, 0);
							pastDate = newDT.toDate();
						} else {
							DateTime newDT = new DateTime(dt.getYear(), dt.getMonthOfYear() + 1, 1, 0, 0);
							pastDate = newDT.toDate();
						}
					}
				}
			} else {
				// daily, keep Date as it is
			}
			return new Period(new LocalDate((pastDate).getTime()), new LocalDate(((Date) present).getTime()), type);

		} else {
			return null;
		}
	}

	private PeriodType computePeriodType(IDomain image) {
		if (image.isInstanceOf(IDomain.YEARLY)) {
			return PeriodType.years();
		} else if (image.isInstanceOf(IDomain.QUATERLY)) {
			return PeriodType.months();
		} else if (image.isInstanceOf(IDomain.MONTHLY)) {
			return PeriodType.months();
		} else if (image.isInstanceOf(IDomain.WEEKLY)) {
			return PeriodType.weeks();
		} else {
			return PeriodType.days();
		}
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
		for (MeasureGroup group : analysis.getGroups()) {
			//
			DataMatrix dm = computeAnalysisSimpleForGroup(analysis, group, optimize, forceBeyondLimit);
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
				// T1913
				for (AxisMapping ax : queryBis.getMapper().getAxisMapping()) {
					AxisMapping mapping = query.getMapper().find(ax.getAxis());
					if (mapping != null && !mapping.getAxis().getName().equals(ax.getAxis().getName())) {
						ax.getAxis().setName(mapping.getAxis().getName());
					}
				}
				for (MeasureMapping mx : queryBis.getMapper().getMeasureMapping()) {
					MeasureMapping mapping = query.getMapper().find(mx.getMapping());
					if (mapping != null && !mapping.getMapping().getName().equals(mx.getMapping().getName())) {
						mx.getMapping().setName(mapping.getMapping().getName());
					}
				}
				runQuery(queryBis, lazy, analysis, qw);
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
				if (dm != null) {
					for (DataMatrixTransform transform : queryBis.getPostProcessing()) {
						dm = transform.apply(dm);
					}
				}
				long end = System.currentTimeMillis();
				logger.info("HIT! get analysis from Smart Cache in " + (end - start) + "ms");
				// set the smart cache flag
				dm.setFromSmartCache(true);
				return dm;
			} catch (Exception ee) {
				// catch all, we don't want to fail here !
			}
		} else {
			long end = System.currentTimeMillis();
			logger.info("Smart Cache consumed: " + (end - start) + "ms");
		}
		// else
		throw new NotInCacheException("cannot use this cached datamatrix");// for
																			// any
																			// reason
	}

	protected DataMatrix computeAnalysisSimpleForGroup(DashboardAnalysis analysis, MeasureGroup group, boolean optimize,
			boolean forceBeyondLimit)
			throws ScopeException, SQLScopeException, ComputingException, InterruptedException, RenderingException {
		// generate the query
		PreviewWriter qw = new PreviewWriter();
		SimpleQuery query = this.genAnalysisQueryCachable(analysis, group, optimize, forceBeyondLimit);
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
							logger.info("put analysis in Smart Cache");
						}
					} else {
						// add to the smart cache
						AnalysisSmartCache.INSTANCE.put(smartCacheRequest);
						logger.info("put analysis in Smart Cache");
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
					PerfDB.INSTANCE.save(queryLog);

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
				Long limit = analysis.getLimit();
				// change the analysis definition
				// => we need to define the join condition explicitly for
				// SmartCache to correctly pick it
				analysis.noLimit();
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
