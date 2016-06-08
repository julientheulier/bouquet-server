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
package com.squid.kraken.v4.core.analysis.engine.hierarchy;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.database.model.Column;
import com.squid.core.database.statistics.ColumnStatistics;
import com.squid.core.database.statistics.IDatabaseStatistics;
import com.squid.core.database.statistics.ObjectStatistics;
import com.squid.core.database.statistics.PartitionInfo;
import com.squid.core.database.statistics.PartitionTable;
import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.reference.ColumnReference;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.IOrderByPiece.ORDERING;
import com.squid.core.sql.render.RenderingException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex.Status;
import com.squid.kraken.v4.core.analysis.engine.query.HierarchyQuery;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.AttributeMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.DimensionMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.MeasureMapping;
import com.squid.kraken.v4.core.analysis.model.IntervalleObject;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.database.impl.DatabaseServiceImpl;
import com.squid.kraken.v4.core.database.impl.DatasourceDefinition;
import com.squid.kraken.v4.model.Attribute;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.model.Domain;

public class DomainHierarchyQueryGenerator {

	private static final Logger logger = LoggerFactory.getLogger(DomainHierarchyQueryGenerator.class);
	
	private DomainHierarchy hierarchy;
	
	protected HashMap<DimensionIndex,HierarchyQuery> queries;
	
	protected ArrayList<DimensionIndex> eagerIndexing;

	public DomainHierarchyQueryGenerator(DomainHierarchy hierarchy) {
		this.hierarchy = hierarchy;
	}

	/**
	 * prepare a set of queries to compute the domain hierarchy <li>group the
	 * dimension by root hierarchy <li>optimize the select complexity if
	 * possible
	 * 
	 * @throws DatabaseServiceException
	 * @throws SQLScopeException
	 * @throws ScopeException
	 * 
	 */
	public void prepareQueries() throws ScopeException,
			SQLScopeException {
		 prepareQueries(hierarchy.getRoot(), hierarchy.getStructure());
	}

	/**
	 * prepare a set of queries to compute the domain hierarchy <li>group the
	 * dimension by root hierarchy <li>optimize the select complexity if
	 * possible
	 * 
	 * @param space
	 * @param hierarchies
	 * @return a list of queries to run
	 * @throws ScopeException
	 * @throws SQLScopeException
	 * @throws DatabaseServiceException
	 */
	protected  void prepareQueries(Space space,
			List<List<DimensionIndex>> hierarchies) throws ScopeException,
			SQLScopeException {
		this.queries = new HashMap<DimensionIndex,HierarchyQuery>();
		this.eagerIndexing = new ArrayList<DimensionIndex>();
		
		HierarchyQuery main_query = null;
		HierarchyQuery continuous_query = null;
		Domain domain = space.getDomain();
		for (List<DimensionIndex> hierarchy : hierarchies) {
	 
			
			// first handle continuous dimension
			boolean handling_continuous = false;
			DimensionIndex root = hierarchy.get(0);
			if (root.getDimension().getType() == Type.CONTINUOUS) {
				IntervalleObject innerRange = computeContinuousStatistic(root);
				if (innerRange == null) {
					if (continuous_query == null) {
						continuous_query = new HierarchyQuery(
								space.getUniverse(), domain);
						DimensionMapping dm = continuous_query.selectContinuous(
								domain, root);
						dm.setOption(DimensionMapping.COMPUTE_INDEX);
						handling_continuous = true;
						
						String renderedQuery;
						try {
							renderedQuery = continuous_query.render();
						} catch (RenderingException e) {
							logger.error("could no create SQL Query for "
									+ hierarchy.toString());
							root.setPermanentError("could no create SQL Query for "
									+ hierarchy.toString());
							continue;
						}

						if (needRefresh(hierarchy, renderedQuery)) {
							logger.debug(" adding  "+ renderedQuery);
							this.queries.put(root, continuous_query);
							this.eagerIndexing.add(root);							
						}
					}

				} else {
					// check interval type
					ExpressionAST min = innerRange.getLowerBoundExpression();
					ExpressionAST max = innerRange.getUpperBoundExpression();
					IDomain check = root.getAxis().getDefinitionSafe()
							.getImageDomain();
					if (min.getImageDomain().isInstanceOf(check)
							&& max.getImageDomain().isInstanceOf(check)) {
						// we can use the range to optimize the min/max query,
						// but it will only work for that index
						HierarchyQuery range_query = new HierarchyQuery(
								space.getUniverse(), domain);
						DimensionMapping dm = range_query.selectContinuous(
								domain, root);
						ExpressionAST condition = createRangeExpression(root,
								innerRange);
						range_query.where(condition);
						dm.setOption(DimensionMapping.COMPUTE_INDEX);
						handling_continuous = true;
						String renderedQuery;
						try {
							renderedQuery = range_query.render();
						} catch (RenderingException e) {
							logger.error("could no create SQL Query for "
									+ hierarchy.toString());
							root.setPermanentError("could no create SQL Query for "
									+ hierarchy.toString());
							continue;
						}

						if (needRefresh(hierarchy, renderedQuery)) {
							this.queries.put(dm.getDimensionIndex(), range_query);// add it in the first
														// place
							logger.debug(" adding  "+ renderedQuery);
						}

					}
				}
			}
			HierarchyQuery select = new HierarchyQuery(space.getUniverse(),
					domain);
			select.setOrdering(ORDERING.ASCENT);
			boolean required = prepareQueryForDimension(domain, select,
					hierarchy, handling_continuous);
			// logger.info("is required ?" + required);
			if (required) {
				float estimate = select.getEstimatedComplexity();
				// logger.info("does " + hierarchy.toString() +" need refresh");
				String renderedQuery;
				try {
					renderedQuery = select.render();
				} catch (RenderingException e) {
					logger.info("could no create SQL Query for "
							+ hierarchy.toString());
					root.setPermanentError("could no create SQL Query for "
							+ hierarchy.toString());
					continue;
				}

				if (needRefresh(hierarchy, renderedQuery)) {
					if (main_query != null
							&& main_query.getQuerySize()
									+ select.getQuerySize() <= 4
							&& main_query.getEstimatedComplexity() * estimate < 100000) {
						// merge with the main query instead
						prepareQueryForDimension(domain, main_query, hierarchy,
								handling_continuous);
						String dis = "" ; 

						for (DimensionIndex di : hierarchy){
							this.queries.put(di, main_query);
							dis+= di.getDimensionName() + " ";
						}
						logger.debug(dis +"\nadding  "+ renderedQuery);


					} else {
						main_query = select;

						String dis = "" ; 
						for (DimensionIndex di : hierarchy){
							this.queries.put(di, main_query);
							dis+= di.getDimensionName() + " ";
						}
						logger.debug(dis +"\nadding  "+ renderedQuery);

					}
				}
				//SFA: what's the point?
				/*
				if (root.getDimension().getType() == Type.CONTINUOUS) {
					DimensionMapping dm = main_query.getDimensionMapping(root);
					if (dm != null)
						dm.setOption(DimensionMapping.COMPUTE_CORRELATIONS);
				}
				*/
			}

			// }
			if (hierarchy.size() > 1) {
				if (root.getStatus() == Status.ERROR) {
					logger.info(
							"Could not initialized Correlation Mapping for \n"
									+ hierarchy.toString(),
							" :\n Root index wih Status ERROR");
				} else {
					root.initCorrelationMapping(hierarchy);
				}
			}
		}
	}

	/**
	 * build an expression to filter outside of the innerRange
	 * 
	 * @param root
	 * @param innerRange
	 * @return
	 * @throws ScopeException
	 */
	protected ExpressionAST createRangeExpression(DimensionIndex root,
			IntervalleObject innerRange) throws ScopeException {
		ExpressionAST def = root.getAxis().getDefinition();
		ExpressionAST lower = ExpressionMaker.LESSOREQUAL(def,
				innerRange.getLowerBoundExpression());
		ExpressionAST upper = ExpressionMaker.LESSOREQUAL(
				innerRange.getUpperBoundExpression(), def);
		return ExpressionMaker.OR(lower, upper);
	}

	// KRKN-71 try to use statistics
	/**
	 * try to extract the range of possible values for the index, based on the
	 * database statistic
	 * 
	 * @param root
	 * @return an IntervalObject or null if no statistics available
	 * @throws ScopeException
	 * @throws DatabaseServiceException
	 */
	protected IntervalleObject computeContinuousStatistic(DimensionIndex root)
			throws ScopeException {
		try {
			ExpressionAST def = root.getAxis().getDefinition();
			if (def instanceof ColumnReference) {
				Column column = ((ColumnReference) def).getColumn();
				Universe universe = root.getAxis().getParent().getUniverse();
				DatasourceDefinition ds = DatabaseServiceImpl.INSTANCE
						.getDatasourceDefinition(universe.getProject());
				IDatabaseStatistics stats = ds.getDBManager().getStatistics();
				if (stats != null) {
					if (stats.isPartitionTable(column.getTable())) {
						// if the table is partitioned, apply to each partition
						PartitionInfo partition = stats.getPartitionInfo(column
								.getTable());
						if (partition.isPartitionKey(column)) {
							List<PartitionTable> partitions = partition
									.getPartitionTables();
							Object lower = null;
							Object upper = null;
							for (PartitionTable partitionTable : partitions) {
								// using stats not working on GP for now
								/*
								 * Column partitionCol =
								 * partitionTable.getTable().findColumnByName
								 * (column.getName()); if (partitionCol!=null) {
								 * ColumnStatistics colstats =
								 * stats.getStatistics(partitionCol); if
								 * (colstats!=null) { Object min =
								 * colstats.getMin(); Object max =
								 * colstats.getMax(); if (range==null) { range =
								 * IntervalleObject.createInterval(min, max); } else
								 * { range = IntervalleObject.merge(range,
								 * IntervalleObject.createInterval(min, max)); } } }
								 */
								// check if the partition is empty
								try {
									ObjectStatistics tableStats = stats
											.getStatistics(partitionTable
													.getTable());
									if (tableStats.getSize() > 0) {
										Object min = partitionTable.getRangeStart();
										Object max = partitionTable.getRangeEnd();
										IntervalleObject range = IntervalleObject
												.createInterval(min, max);
										if (range != null) {
											// compute the inner range of the
											// partition
											if (lower == null) {
												lower = range.getUpperBound();
											} else {
												if (range.compareUpperBoundTo(lower) < 0) {
													lower = range.getUpperBound();
												}
											}
											if (upper == null) {
												upper = range.getLowerBound();
											} else {
												if (range.compareLowerBoundTo(upper) > 0) {
													upper = range.getLowerBound();
												}
											}
										}
									}
								} catch (SQLException e) {
									logger.warn(e.getMessage());
								}
							}
							return IntervalleObject.createInterval(lower, upper);
						}
					} else {
						ColumnStatistics colstats = stats.getStatistics(column);
						if (colstats != null) {
							Object min = colstats.getMin();
							Object max = colstats.getMax();
							return IntervalleObject.createInterval(min, max);
						}
					}
				}
			}
		} catch (ExecutionException e) {
			logger.error(e.getMessage());
		}
		// else
		return null;
	}

	/**
	 * add the list of dimensionIndex to the given select
	 * 
	 * @param domain
	 * @param select
	 * @param indexes
	 * @return
	 */
	protected boolean prepareQueryForDimension(Domain domain,
			HierarchyQuery select, List<DimensionIndex> indexes,
			boolean handling_continuous) {
		boolean result = false;
		DimensionIndex root = indexes.get(0);
		for (DimensionIndex index : indexes) {
			if (!(index instanceof DimensionIndexProxy)) {
				boolean needed = prepareQueryForDimension(domain, select, index);
				if (!needed && index.getStatus() == Status.STALE) {
					index.setDone();
				}
				if (handling_continuous && index == root
						&& index.getDimension().getType() == Type.CONTINUOUS) {
					// we can ignore it
				} else {
					result = needed || result;
				}
			}
		}
		return result;
	}

	/**
	 * add the dimensionIndex associated with the axis to the given select
	 * 
	 * @param domain
	 * @param select
	 * @param axis
	 * @param index
	 * @return
	 * @throws ScopeException
	 * @throws SQLScopeException
	 */
	protected boolean prepareQueryForDimension(Domain domain,
			HierarchyQuery select, DimensionIndex index) {
		//
		try {
			Dimension d = index.getDimension();
			ExpressionAST definition = DimensionIndexCreationUtils
					.getDefinition(index.getAxis());
			IDomain image = definition.getImageDomain();
			if (image.isInstanceOf(IDomain.CONDITIONAL)) {// ticket:3014
				// don't update result yet, no need to run query
				// predicate dimension have only one possible value == TRUE
				index.getMemberByID(true);
				return false;
			} else if (d.getType().equals(Type.CATEGORICAL)) {
				//
				DimensionMapping dmap = select.select(domain, index);
				select.getSelect()
						.getStatement()
						.addComment(
								"Indexing dimension "
										+ index.getDimensionName());
				//
				for (Attribute attr : index.getAttributes()) {
					AttributeMapping attrmap = select.select(domain, index,
							attr);
					dmap.putMapping(attr.getId().getAttributeId(), attrmap);
				}
				//
				return true;
			} else if (d.getType().equals(Type.CONTINUOUS)) {
				// for continuous dimension we want to compute the min/max
				Axis axis = index.getAxis();
				Measure min = axis.getParent().M(
						ExpressionMaker.MIN(axis.getDefinition()));
				Measure max = axis.getParent().M(
						ExpressionMaker.MAX(axis.getDefinition()));
				MeasureMapping kxmin = select.select(min);
				MeasureMapping kxmax = select.select(max);
				select.add(kxmin, kxmax, domain, index);
				select.getSelect()
						.getStatement()
						.addComment(
								"Indexing dimension "
										+ index.getDimensionName());
				//
				return true;
			} else {
				// what about handling indexes ?
				return false;
			}
		} catch (ScopeException | SQLScopeException e) {
			// failed to compute the dimension
			index.setPermanentError(e.getMessage());
			logger.error("failed to compute dimension "
					+ index.getAxis().prettyPrint() + " with error: " + e);
			return false;
		}
	}

	protected boolean needRefresh(List<DimensionIndex> hierarchy, String query) {
		boolean res = false;
		for (DimensionIndex index : hierarchy) {
			index.initStore(query);
			if (index.getStatus() == Status.STALE) {
				logger.info("Dimension"  + index.getDimensionName() + "index needs refresh")  ;
				res = true;
			}
		}
		return res;
	}
}
