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
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

import com.squid.kraken.v4.api.core.PerfDB;
import com.squid.kraken.v4.api.core.SQLStats;

import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.domain.IDomain;
import com.squid.core.domain.set.SetDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.jdbc.engine.IExecutionItem;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.IOrderByPiece.ORDERING;
import com.squid.core.sql.render.ISelectPiece;
import com.squid.core.sql.render.RenderingException;
import com.squid.kraken.v4.core.analysis.datamatrix.AxisValues;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.engine.query.SimpleQuery;
import com.squid.kraken.v4.core.analysis.model.Dashboard;
import com.squid.kraken.v4.core.analysis.model.DashboardAnalysis;
import com.squid.kraken.v4.core.analysis.model.DomainSelection;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
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
import com.squid.kraken.v4.export.ExecuteAnalysisResult;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.model.Domain;

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
	
	private static final boolean SUPPORT_SOFT_FILTERS = false; // turn to true to support soft-filter optimization

	public AnalysisCompute(Universe universe) {
		this.universe = universe;
	}

	public List<SimpleQuery> reinject(DashboardAnalysis analysis)
			throws ComputingException, ScopeException, SQLScopeException,
			InterruptedException, RenderingException {
		List<MeasureGroup> groups = analysis.getGroups();
		List<SimpleQuery> queries = new ArrayList<SimpleQuery>();
		if (groups.isEmpty()) {
			SimpleQuery query = this.genSimpleQuery(analysis);
			queries.add(query);
		} else {
			// StringBuilder result = new StringBuilder();
			boolean optimize = false;
			for (MeasureGroup group : analysis.getGroups()) {
				SimpleQuery query = this.genAnalysisQuery(analysis, group,
						optimize);
				queries.add(query);
			}
		}
		return queries;
	}

	public String viewSQL(DashboardAnalysis analysis)
			throws ComputingException, ScopeException, SQLScopeException,
			InterruptedException, RenderingException {
		List<MeasureGroup> groups = analysis.getGroups();
		if (groups.isEmpty()) {
			SimpleQuery query = this.genSimpleQuery(analysis);
			return query.render();
		} else {
			StringBuilder result = new StringBuilder();
			boolean optimize = false;
			for (MeasureGroup group : analysis.getGroups()) {
				SimpleQuery query = this.genAnalysisQuery(analysis, group,
						optimize);
				result.append(query.render());
				result.append("\n\n");
			}
			return result.toString();
		}
	}

	public DataMatrix computeAnalysis(DashboardAnalysis analysis)
			throws ScopeException, SQLScopeException, ComputingException,
			InterruptedException {
		//
		List<MeasureGroup> groups = analysis.getGroups();
		if (groups.isEmpty()) {
			SimpleQuery query = this.genSimpleQuery(analysis);
			DataMatrix dm = query.run(analysis.isLazy());
			return dm;
		} else if (analysis.getSelection().hasCompareToSelection()) {
			return computeCompareToAnalysis(analysis);
		} else {
			// disable the optimizing when using the limit feature
			@SuppressWarnings("unused")
			boolean optimize = SUPPORT_SOFT_FILTERS && !analysis.hasLimit() && !analysis.hasOffset()
					&& !analysis.hasRollup();
			return computeAnalysisSimple(analysis, optimize);
		}
	}

	// handle compare T947
	public DataMatrix computeCompareToAnalysis(DashboardAnalysis currentAnalysis) throws ScopeException, ComputingException, SQLScopeException, InterruptedException {
		// preparing the selection
		DashboardSelection presentSelection = currentAnalysis.getSelection();
		DomainSelection compare = presentSelection.getCompareToSelection();
		Axis joinAxis = null;
		IntervalleObject presentInterval = null;
		IntervalleObject pastInterval = null;
		for (Axis filter : compare.getFilters()) {
			// check if the filter is a join
			GroupByAxis groupBy = findGroupingJoin(filter, currentAnalysis);
			if (groupBy!=null) {
				if (joinAxis!=null) {
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
		List<OrderBy> fixed = new ArrayList<>();
		List<OrderBy> remaining = new ArrayList<>();
		int i=0;
		// list the dimensions
		ArrayList<ExpressionAST> dimensions = new ArrayList<>();// order matter
		for (GroupByAxis group : currentAnalysis.getGrouping()) {
			dimensions.add(group.getAxis().getReference());
		}
		int[] mergeOrder = new int[dimensions.size()];// will hold in which order to merge
		// rebuild the full order specs
		for (OrderBy order : currentAnalysis.getOrders()) {
			if (i==0 && joinAxis!=null) {
				if (order.getExpression().equals(joinAxis.getReference())) {
					// ok, it's first
					fixed.add(new OrderBy(i, order.getExpression(), order.getOrdering()));
					mergeOrder[i++] = dimensions.indexOf(order.getExpression());
					continue;// done for this order
				} else {
					// add it first
					fixed.add(new OrderBy(i, joinAxis.getReference(), ORDERING.DESCENT));// default to DESC
					mergeOrder[i++] = dimensions.indexOf(joinAxis.getReference());
					// now we need to take care of the order
				}
			}
			// not (the first & join)...
			// check if it is a dimension
			if (dimensions.contains(order.getExpression())) {
				// ok, just add it
				fixed.add(new OrderBy(i, order.getExpression(), order.getOrdering()));
				mergeOrder[i++] = dimensions.indexOf(order.getExpression());
				// and remove the dimension from the list
				dimensions.remove(order.getExpression());
			} else {
				// assuming it is a metric or something else, keep it but at the end
				remaining.add(order);// don't know the position yet
			}
		}
		// add missing dimensions
		if (!dimensions.isEmpty()) {
			for (ExpressionAST dim : dimensions) {
				if (joinAxis==null || !joinAxis.getReference().equals(dim)) {
					fixed.add(new OrderBy(i, dim, ORDERING.DESCENT));// default to DESC
					mergeOrder[i++] = dimensions.indexOf(dim);
				}
			}
		}
		// add non-dimensions
		if (!remaining.isEmpty()) {
			for (OrderBy order : remaining) {
				fixed.add(new OrderBy(i++, order.getExpression(), order.getOrdering()));
			}
		}
		currentAnalysis.setOrders(fixed);
		//
		// compute present
		DataMatrix present = computeAnalysisSimple(currentAnalysis, false);
		//
		// compute the past version
		DashboardAnalysis compareToAnalysis = new DashboardAnalysis(universe);
		// copy dimensions
		for (GroupByAxis groupBy : currentAnalysis.getGrouping()) {
			if (groupBy.getAxis().equals(joinAxis)) {
				Axis compareToAxis = new Axis(groupBy.getAxis());
				compareToAxis.setOriginType(OriginType.COMPARETO);
				compareToAxis.setName(groupBy.getAxis().getName()+" (compare)");
				GroupByAxis compareToGroupBy = compareToAnalysis.add(compareToAxis, groupBy.isRollup());
				compareToGroupBy.setRollupPosition(groupBy.getRollupPosition());
			} else {
				compareToAnalysis.add(groupBy);
			}
		}
		// copy metrics
		for (Measure kpi : currentAnalysis.getKpis()) {
			Measure compareToKpi = new Measure(kpi);
			compareToKpi.setOriginType(OriginType.COMPARETO);
			compareToKpi.setName(kpi.getName()+" (compare)");
			compareToAnalysis.add(compareToKpi);
		}
		// copy stuff
		if (currentAnalysis.hasLimit()) compareToAnalysis.limit(currentAnalysis.getLimit());
		if (currentAnalysis.hasOffset()) compareToAnalysis.offset(currentAnalysis.getOffset());
		if (currentAnalysis.isRollupGrandTotal()) compareToAnalysis.setRollupGrandTotal(true);
		if (currentAnalysis.hasRollup()) compareToAnalysis.setRollup(currentAnalysis.getRollup());
		compareToAnalysis.setOrders(currentAnalysis.getOrders());
		// copy the selection and replace with compare filters
		DashboardSelection pastSelection = new DashboardSelection(presentSelection);
		for (Axis filter : compare.getFilters()) {
			pastSelection.clear(filter);
			pastSelection.add(filter, compare.getMembers(filter));
			if (joinAxis!=null && filter.equals(joinAxis)) {
				pastInterval = computeMinMax(compare.getMembers(filter));
			}
		}
		compareToAnalysis.setSelection(pastSelection);
		DataMatrix past = computeAnalysisSimple(compareToAnalysis, false);
		//
		final int offset = computeOffset(present, joinAxis, presentInterval, pastInterval);
		//
		CompareMerger merger = new CompareMerger(present, past, mergeOrder, joinAxis, offset);
		DataMatrix debug = merger.merge(false);
		return debug;
	}
	
	private GroupByAxis findGroupingJoin(Axis join, DashboardAnalysis from) {
		DateExpressionAssociativeTransformationExtractor checker = new DateExpressionAssociativeTransformationExtractor();
		ExpressionAST naked1 = checker.eval(join.getDimension()!=null?join.getReference():join.getDefinitionSafe());
		IDomain d1 = join.getDefinitionSafe().getImageDomain();
		for (GroupByAxis groupBy : from.getGrouping()) {
			IDomain d2 = groupBy.getAxis().getDefinitionSafe().getImageDomain();
			if (d1.isInstanceOf(IDomain.TEMPORAL) && d2.isInstanceOf(IDomain.TEMPORAL)) {
				// if 2 dates, try harder...
				// => the groupBy can be a associative transformation of the filter
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

	private int computeOffset(DataMatrix present, Axis joinAxis, IntervalleObject presentInterval, IntervalleObject pastInterval) {
		if (joinAxis==null || presentInterval==null || pastInterval==null) {
			return 0;
		} else {
			AxisValues check = present.find(joinAxis);
			if (check==null) {
				return 0;// no need to bother
			} else {
				return computeOffset(presentInterval, pastInterval, check.getOrdering());
			}
		}
	}

	private int computeOffset(IntervalleObject presentInterval, IntervalleObject pastInterval, ORDERING ordering) {
		// it is better to compare on the lower bound because alignment on the end of month is not accurate
		Object present = presentInterval.getLowerBound();
		Object past = pastInterval.getLowerBound();
		if (present instanceof Date && past instanceof Date) {
			return daysBetween((Date)past, (Date)present);
		} else {
			return 0;
		}
	}
	
	private int daysBetween(Date lower, Date upper) {
		return Days.daysBetween(new LocalDate(((Date)lower).getTime()), new LocalDate(((Date)upper).getTime())).getDays();
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
	 * This method expect to compute a "simple" analysis, that is not requiring a merge/compare operation
	 * It supports the NoLimit parameter.
	 * @param analysis
	 * @param optimize
	 * @return
	 * @throws ScopeException
	 * @throws ComputingException
	 * @throws SQLScopeException
	 * @throws InterruptedException
	 */
	private DataMatrix computeAnalysisSimple(DashboardAnalysis analysis,
			boolean optimize) throws ScopeException, ComputingException,
			SQLScopeException, InterruptedException {
		// select with one or several KPI groups
		DataMatrix result = null;
		for (MeasureGroup group : analysis.getGroups()) {
			//
			// generate the query
			// -- note that some optimization can take place here: soft-filters, noLimit...
			DashboardSelection soft_filters = new DashboardSelection();// will list the soft-filters
			ArrayList<Axis> hidden_slice = new ArrayList<Axis>();// and the corresponding axis to hide
			SimpleQuery query = this.genAnalysisQueryCachable(analysis,
					group, optimize, soft_filters, hidden_slice);
			// compute the data-matrix
			DataMatrix dm = query.run(analysis.isLazy());
			if (dm != null) {
				// hide axis in case there are coming from generalized query
				for (Axis axis : hidden_slice) {
					dm.getAxisColumn(axis).setVisible(false);
				}
				// apply the soft filters if any left
				if (!soft_filters.isEmpty()) {
					dm = dm.filter(soft_filters, false);//ticket:2923 Null values must not be retained.
				}
				// merge if needed
				if (result==null) {
					result = dm;
				} else {
					result = result.merge(dm);
				}
			} else {
				result = null;
			}
        }
        return result;
    }

    protected DashboardAnalysis subQueryLimit(DashboardAnalysis analysis, MeasureGroup group) throws ScopeException {
        if (analysis.hasLimit() && analysis.hasOrderBy()) {
            // ok, let's check if it is a n-variate time-series
            if (analysis.getGrouping().size()==2) {
                // first checked there are 2 axes
                GroupByAxis groupBy1 = analysis.getGrouping().get(0);
                GroupByAxis groupBy2 = analysis.getGrouping().get(1);
                Type type1 = groupBy1.getAxis().getDimensionType();
                Type type2 = groupBy2.getAxis().getDimensionType();
                if (type1==Type.CONTINUOUS 
                		&& groupBy1.getAxis().getDefinition().getImageDomain().isInstanceOf(IDomain.TEMPORAL)
                		&& type2!=Type.CONTINUOUS) {// works for CAT & INDEX
                    DashboardAnalysis copy = new DashboardAnalysis(analysis.getUniverse());
                    copy.add(group.getKPIs());
                    copy.add(groupBy2.getAxis(),groupBy2.isRollup());
                    copy.setSelection(analysis.getSelection());
                    for (OrderBy orderBy : analysis.getOrders()) {
                    	if (orderBy.getExpression().equals(groupBy1.getAxis().getReference())) {
                    		// do not use
                    		return null;
                    	}
                        copy.orderBy(orderBy);
                    }
                    copy.limit(analysis.getLimit());
                    return copy;
                } else if (type2==Type.CONTINUOUS 
                		&& groupBy2.getAxis().getDefinition().getImageDomain().isInstanceOf(IDomain.TEMPORAL)
                		&& type1!=Type.CONTINUOUS) {
                    DashboardAnalysis copy = new DashboardAnalysis(analysis.getUniverse());
                    copy.add(group.getKPIs());
                    copy.add(groupBy1.getAxis(),groupBy1.isRollup());
                    copy.setSelection(analysis.getSelection());
                    for (OrderBy orderBy : analysis.getOrders()) {
                    	if (orderBy.getExpression().equals(groupBy2.getAxis().getReference())) {
                    		// do not use
                    		return null;
                    	}
                        copy.orderBy(orderBy);
                    }
                    copy.limit(analysis.getLimit());
                    return copy;
                }
            }
        }
        // else
        return null;
	}
	
    /**
     * execute the analysis but does not read the result: 
     * this method can be used to stream the result back to client, for instance to export the dataset
     * @param analysis
     * @return
     * @throws ComputingException
     * @throws InterruptedException
     */
	public ExecuteAnalysisResult executeAnalysis(DashboardAnalysis analysis) throws ComputingException, InterruptedException {
	    try {
            long start = System.currentTimeMillis();
    		logger.info("start of sql generation");

	        List<MeasureGroup> groups = analysis.getGroups();
            if (groups.isEmpty()) {
                 SimpleQuery query = this.genSimpleQuery(analysis);
                
                long stop = System.currentTimeMillis();
        		//logger.info("End of sql generation  in " +(stop-start)+ "ms" );
    			logger.info("task="+this.getClass().getName()+" method=executeAnalysis.SQLGeneration"+" duration="+ (stop-start)+" error=false status=done");
                try {
                    String sql = query.render();
                    SQLStats queryLog = new SQLStats(query.toString(), "executeAnalysis.SQLGeneration",sql, (stop-start), analysis.getUniverse().getProject().getId().getProjectId());
                    queryLog.setError(false);
                    PerfDB.INSTANCE.save(queryLog);

                } catch (RenderingException e) {
                    e.printStackTrace();
                }

        		IExecutionItem execute = query.executeQuery();
        		ExecuteAnalysisResult res = new ExecuteAnalysisResult(execute, query.getMapper());  
        		return res;
        	
            } else {
                // possible only if there is only one group
                if (groups.size()!=1) {
                    throw new ComputingException("the analysis cannot be exported in a single query - try removing some metrics");
                }
                // select with one or several KPI groups
                MeasureGroup group = groups.get(0);
                //
                SimpleQuery query = genAnalysisQueryWithSoftFiltering(analysis, group, false, false, null, null);
        		//
        		IExecutionItem execute = query.executeQuery();
        		ExecuteAnalysisResult res = new ExecuteAnalysisResult(execute, query.getMapper());        		
        		return res;
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
	 * @param if cachable is true, the genAnalysisQuery will try to normalize
	 *        the SQL - that means it may ignore some statements like ORDER BY
	 *        or select column order...
	 * @throws InterruptedException
	 */
	protected SimpleQuery genAnalysisQueryWithSoftFiltering(DashboardAnalysis analysis,
			MeasureGroup group, boolean cachable,
			boolean optimize, DashboardSelection soft_filters,
			List<Axis> hidden_slice) throws ScopeException, SQLScopeException,
			ComputingException, InterruptedException {
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
		boolean defaultOrder = !analysis.hasOrderBy() && !analysis.hasLimit()
				&& cachable;
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
				throw new ScopeException("the Axis '"
						+ groupBy.getAxis().prettyPrint()
						+ "' is incompatible with the query scope " + names);
			}
			//
			Space hook = computeSinglePath(analysis, master, groupBy.getAxis()
					.getParent().getTop(), mandatory_link);
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
				Collection<DimensionMember> filters = selection
						.getMembers(axis);
				Dimension dimension = axis.getDimension();
				if (!optimize) {
					query.where(axis, filters);
				} else {
					if ((dimension.getType().equals(Type.CATEGORICAL) || dimension
							.getType().equals(Type.INDEX) // ticket:3001
							)
							&& slices.contains(axis)) {
						// analysis already contains the axis filter as a slice
						if (soft_filters != null) {
							soft_filters.add(axis, filters);
						}
					} else {
						// ok, we can decide to slice then filter instead of
						// direct filtering...
						boolean generalize = false;
						IDomain image = axis.getDefinition().getImageDomain();
						if (!image.isInstanceOf(SetDomain.SET)
								&& !image.isInstanceOf(IDomain.CONDITIONAL) // ticket:3014
																			// -
																			// not
																			// for
																			// predicate
						) {
							if (slice_numbers < 10
									&& filters.size() == 1
									&& dimension.getType().equals(
											Type.CATEGORICAL)) {
								// limited to the situation where the filter
								// applies to only ONE value
								// this is to avoid side effect with
								// non-associative operators (AVG, MIN, MAX...)
								float size = axis.getEstimatedSize();
								if (size < 10000
										&& row_estimate * size < 200000) {
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
	 * @param analysis
	 * @return
	 * @throws ScopeException
	 * @throws SQLScopeException
	 * @throws ComputingException
	 * @throws InterruptedException
	 */
	protected SimpleQuery genSimpleQuery(DashboardAnalysis analysis)
			throws ScopeException, SQLScopeException, ComputingException,
			InterruptedException {
		if (analysis.getMainDomain() == null) {
			throw new ComputingException(
					"if no kpi is defined, must have one single domain");
		}
		Space root = analysis.getMainDomain();
		// create the Operator
		SimpleQuery query = new SimpleQuery(root);
		for (GroupByAxis item : analysis.getGrouping()) {
			Space hook = computeSinglePath(analysis, root.getDomain(), item
					.getAxis().getParent().getTop(), mandatory_link);
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
		} else {
			query.limit(1000);// if you want all, use the export
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
	 * @param analysis
	 * @param group
	 * @param optimize
	 * @return
	 * @throws ScopeException
	 * @throws SQLScopeException
	 * @throws ComputingException
	 * @throws InterruptedException
	 */
	protected SimpleQuery genAnalysisQuery(DashboardAnalysis analysis,
			MeasureGroup group, boolean optimize) throws ScopeException,
			SQLScopeException, ComputingException, InterruptedException {
		return this.genAnalysisQueryCachable(analysis, group, optimize,
				null, null);
	}

	protected SimpleQuery genAnalysisQueryCachable(
			DashboardAnalysis analysis, MeasureGroup group, boolean optimize,
			DashboardSelection soft_filters, List<Axis> hidden_slice)
			throws ScopeException, SQLScopeException, ComputingException,
			InterruptedException {
		if (analysis.hasBeyondLimit() && analysis.hasLimit() && !analysis.hasRollup()) {
			// need to take care of the beyond limit axis => compute the limit only on a subset of axes
			return genAnalysisQueryWithBeyondLimitSupport(analysis, group, true, optimize, soft_filters, hidden_slice);
		} else {
			// use the simple method
			return genAnalysisQueryWithSoftFiltering(analysis, group, 
					true, // just set the cachable flag to true -- is this really usefull?
					optimize, soft_filters, hidden_slice);
		}
	}

	/**
	 * this is the magic version that tries to not apply the limit for n-variate time-series.
	 * But the way it works is too magical (i.e. you can't predict the output) so it is disable for now.
	 * We will look for a explicit way to achieve that magic, for instance by providing a way to define the LIMIT scope.
	 * 
	 * @param analysis
	 * @param group
	 * @param optimize
	 * @param soft_filters
	 * @param hidden_slice
	 * @return
	 * @throws ScopeException
	 * @throws SQLScopeException
	 * @throws ComputingException
	 * @throws InterruptedException
	 */
	protected SimpleQuery createOperatorKPIPopulateFiltersWithMagic(
			DashboardAnalysis analysis, MeasureGroup group, boolean optimize,
			DashboardSelection soft_filters, List<Axis> hidden_slice)
			throws ScopeException, SQLScopeException, ComputingException,
			InterruptedException {
		//
		DashboardAnalysis subQueryLimit = subQueryLimit(analysis, group);
		if (subQueryLimit != null) {
			analysis.noLimit();
		}
		SimpleQuery query = genAnalysisQueryWithSoftFiltering(analysis, group, true,
				optimize, soft_filters, hidden_slice);
		//
		// handle the subquery if any
		if (subQueryLimit != null) {
			// if we have a subquery, apply the limit to it and read the first
			// values in order to filter the actual query
			// note: if the database does support limit, we could use a subquery
			// in place... or a temporary table?
			DataMatrix temp = computeAnalysis(subQueryLimit);
			AxisValues values = temp.getAxes().get(0);// we should have only one
														// value
			if (values != null) {
				ConcurrentSkipListSet<DimensionMember> filters = new ConcurrentSkipListSet<>();
				filters.addAll(values.getMembers());
				query.where(values.getAxis(), filters);// need to pass the query
														// axis
			} else {
				// failed, restoring limit
				analysis.limit(subQueryLimit.getLimit());
			}
		}
		return query;
	}
	
	/**
	 * handling the BeyondLimit parameter
	 * Note: rollup not yet supported
	 * @param analysis
	 * @param group
	 * @param cachable
	 * @param optimize
	 * @param soft_filters
	 * @param hidden_slice
	 * @return
	 * @throws ScopeException
	 * @throws SQLScopeException
	 * @throws ComputingException
	 * @throws InterruptedException
	 */
	protected SimpleQuery genAnalysisQueryWithBeyondLimitSupport(DashboardAnalysis analysis,
			MeasureGroup group, boolean cachable,
			boolean optimize, DashboardSelection soft_filters,
			List<Axis> hidden_slice) throws ScopeException, SQLScopeException,
			ComputingException, InterruptedException {
		//
		// prepare the sub-query that will count the limit
		DashboardAnalysis subAnalysisWithLimit = new  DashboardAnalysis(analysis.getUniverse());
		// copy dimensions
		ArrayList<Axis> joins = new ArrayList<>();
		for (GroupByAxis groupBy : analysis.getGrouping()) {
			if (!analysis.getBeyondLimit().contains(groupBy)) {
				subAnalysisWithLimit.add(groupBy);
				joins.add(groupBy.getAxis());
			} else {
				// exclude from the analysis
			}
		}
		if (subAnalysisWithLimit.getGrouping().isEmpty()) {
			throw new ScopeException("invalid beyondLimit request: there are no remaining axis to apply the limit");
		}
		// copy metrics
		for (Measure measure : analysis.getKpis()) {
			subAnalysisWithLimit.add(measure);
		}
		// copy orders
		ArrayList<ExpressionAST> exclude = new ArrayList<>();
		DateExpressionAssociativeTransformationExtractor extractor = new DateExpressionAssociativeTransformationExtractor();
		for (GroupByAxis slice : analysis.getBeyondLimit()) {
			exclude.add(extractor.eval(slice.getAxis().getDefinitionSafe()));
		}
		for (OrderBy order : analysis.getOrders()) {
			ExpressionAST naked = extractor.eval(order.getExpression());
			if (!exclude.contains(naked)) {
				subAnalysisWithLimit.orderBy(order);
			}
		}
		// copy stuff
		if (analysis.hasLimit()) subAnalysisWithLimit.limit(analysis.getLimit());
		if (analysis.hasOffset()) subAnalysisWithLimit.offset(analysis.getOffset());
		if (analysis.isRollupGrandTotal()) subAnalysisWithLimit.setRollupGrandTotal(true);
		if (analysis.hasRollup()) subAnalysisWithLimit.setRollup(analysis.getRollup());
		// copy selection
		subAnalysisWithLimit.setSelection(new DashboardSelection(analysis.getSelection()));
		// use the best strategy
		if (joins.size()==1 && subAnalysisWithLimit.getLimit()<50) {
			// run sub-analysis and add filters by hand (cache hit on the subquery)
			DataMatrix selection = computeAnalysisSimple(subAnalysisWithLimit, false);
			Axis join = joins.get(0);
			AxisValues values = selection.getAxisColumn(join);
			if (values != null && !values.getMembers().isEmpty()) {
				ConcurrentSkipListSet<DimensionMember> filters = new ConcurrentSkipListSet<>();
				filters.addAll(values.getMembers());
				analysis.noLimit();
				SimpleQuery mainquery = genAnalysisQueryWithSoftFiltering(analysis, group, cachable, optimize, soft_filters, hidden_slice);
				mainquery.where(values.getAxis(), filters);
				return mainquery;
			} else {
				// failed, using original limit
				return genAnalysisQueryWithSoftFiltering(analysis, group, cachable, optimize, soft_filters, hidden_slice);
			}
		} else {
			// generate a subquery and use EXISTS operator
			// -- do not optimize, we don't want side effect here
			SimpleQuery subquery = genAnalysisQueryWithSoftFiltering(subAnalysisWithLimit, group, cachable, false, soft_filters, hidden_slice);
			//
			// get the original query without limit
			analysis.noLimit();
			SimpleQuery mainquery = genAnalysisQueryWithSoftFiltering(analysis, group, cachable, optimize, soft_filters, hidden_slice);
			//
			mainquery.join(joins, subquery);
			return mainquery;
		}
	}

	private Space computeSinglePath(Dashboard dashboard, Domain root,
			Space target, boolean mandatory) throws ScopeException,
			ComputingException {
		Space single_space = null;
		List<Space> paths = computePaths(root, target.getRoot());
		if (paths.isEmpty()) {
			if (mandatory) {
				throw new ScopeException("unable to link domain '"
						+ root.getName() + "' with that Filter");
			} else {
				logger.warn("ignoring axis '"
						+ target.getPath()
						+ "' from the selection, cannot resolve to a valid path");
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

	private Space computeSinglePath(Dashboard dashboard, Measure measure,
			Space target, boolean mandatory) throws ScopeException,
			ComputingException {
		Space single_space = null;
		if (target.equals(dashboard.getMainDomain())) {
			// try to figure out if there is something possible
			Domain root = measure.getParent().getRoot();
			List<Space> paths = computePaths(root, target.getRoot());
			if (paths.isEmpty() && mandatory) {
				throw new ScopeException("unable to link KPI '"
						+ measure.getName() + "' to the timeline");
			}
			single_space = paths.get(0);// hum, ok for now...
		} else {
			Domain root = measure.getParent().getRoot();
			List<Space> paths = computePaths(root, target.getRoot());
			if (paths.isEmpty()) {
				if (mandatory) {
					throw new ScopeException("unable to link KPI '"
							+ measure.getName() + "' with that Filter");
				} else {
					logger.warn("ignoring axis '"
							+ target.getPath()
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

	private List<Space> computePaths(Domain root, Domain target)
			throws ScopeException, ComputingException {
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
			return universe.getCartography()
					.getAllPaths(universe, root, target);
		}
		return paths;
	}

}
