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
package com.squid.kraken.v4.api.core.analytics;

import java.io.IOException;
import java.net.URI;
import java.sql.Types;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.concurrent.ExecutionManager;
import com.squid.core.domain.DomainNumericConstant;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.analytics.AnalyticDomain;
import com.squid.core.domain.operators.ExtendedType;
import com.squid.core.domain.operators.IntrinsicOperators;
import com.squid.core.domain.operators.OperatorDefinition;
import com.squid.core.domain.sort.DomainSort;
import com.squid.core.domain.sort.DomainSort.NullsPosition;
import com.squid.core.domain.sort.DomainSort.SortDirection;
import com.squid.core.domain.sort.SortOperatorDefinition;
import com.squid.core.domain.vector.VectorOperatorDefinition;
import com.squid.core.expression.ConstantValue;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.NumericConstant;
import com.squid.core.expression.Operator;
import com.squid.core.expression.PrettyPrintOptions;
import com.squid.core.expression.PrettyPrintOptions.ReferenceStyle;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.RenderingException;
import com.squid.core.sql.render.SQLSkin;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.EngineUtils;
import com.squid.kraken.v4.api.core.InvalidIdAPIException;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl.OutputFormat;
import com.squid.kraken.v4.api.core.JobStats;
import com.squid.kraken.v4.api.core.NotYetImplementedException;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.api.core.projectanalysisjob.AnalysisJobComputer;
import com.squid.kraken.v4.caching.NotInCacheException;
import com.squid.kraken.v4.core.analysis.datamatrix.AxisValues;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.datamatrix.IDataMatrixConverter;
import com.squid.kraken.v4.core.analysis.datamatrix.MeasureValues;
import com.squid.kraken.v4.core.analysis.datamatrix.RecordConverter;
import com.squid.kraken.v4.core.analysis.datamatrix.TableConverter;
import com.squid.kraken.v4.core.analysis.datamatrix.TransposeConverter;
import com.squid.kraken.v4.core.analysis.engine.bookmark.BookmarkManager;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex.Status;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchy;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchyManager;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.FacetBuilder;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.SegmentManager;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingService;
import com.squid.kraken.v4.core.analysis.model.DashboardAnalysis;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.model.DomainSelection;
import com.squid.kraken.v4.core.analysis.model.ExpressionInput;
import com.squid.kraken.v4.core.analysis.model.Intervalle;
import com.squid.kraken.v4.core.analysis.scope.AxisExpression;
import com.squid.kraken.v4.core.analysis.scope.GlobalExpressionScope;
import com.squid.kraken.v4.core.analysis.scope.MeasureExpression;
import com.squid.kraken.v4.core.analysis.scope.SpaceExpression;
import com.squid.kraken.v4.core.analysis.scope.SpaceScope;
import com.squid.kraken.v4.core.analysis.scope.UniverseScope;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.reference.DomainReference;
import com.squid.kraken.v4.core.model.domain.DomainDomain;
import com.squid.kraken.v4.model.AnalyticsQuery;
import com.squid.kraken.v4.model.AnalyticsQueryImpl;
import com.squid.kraken.v4.model.AnalyticsReply;
import com.squid.kraken.v4.model.AnalyticsResult;
import com.squid.kraken.v4.model.AnalyticsSelection;
import com.squid.kraken.v4.model.AnalyticsSelectionImpl;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.BookmarkConfig;
import com.squid.kraken.v4.model.DataHeader;
import com.squid.kraken.v4.model.DataHeader.Column;
import com.squid.kraken.v4.model.DataLayout;
import com.squid.kraken.v4.model.DataTable;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Expression;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.FacetExpression;
import com.squid.kraken.v4.model.FacetMember;
import com.squid.kraken.v4.model.FacetMemberInterval;
import com.squid.kraken.v4.model.FacetMemberString;
import com.squid.kraken.v4.model.FacetSelection;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.NavigationQuery.Style;
import com.squid.kraken.v4.model.Problem;
import com.squid.kraken.v4.model.Problem.Severity;
import com.squid.kraken.v4.model.ProjectAnalysisJob;
import com.squid.kraken.v4.model.ProjectAnalysisJob.Direction;
import com.squid.kraken.v4.model.ProjectAnalysisJob.Index;
import com.squid.kraken.v4.model.ProjectAnalysisJob.Nulls;
import com.squid.kraken.v4.model.ProjectAnalysisJob.OrderBy;
import com.squid.kraken.v4.model.ProjectAnalysisJob.Position;
import com.squid.kraken.v4.model.ProjectAnalysisJob.RollUp;
import com.squid.kraken.v4.model.ProjectAnalysisJobPK;
import com.squid.kraken.v4.model.ResultInfo;
import com.squid.kraken.v4.model.StatePK;
import com.squid.kraken.v4.model.ViewQuery;
import com.squid.kraken.v4.model.ViewReply;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.vegalite.VegaliteConfigurator;
import com.squid.kraken.v4.vegalite.VegaliteSpecs;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Data;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.DataType;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Format;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.FormatType;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Mark;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Operation;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Order;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Sort;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Stacked;

/**
 * Factoring the core services (query and view) to avoid polluting with rest/http code and data structure
 * @author sergefantino
 *
 */
public class AnalyticsServiceCore {

	static final Logger logger = LoggerFactory
			.getLogger(AnalyticsServiceCore.class);

	private final DateFormat ISO8601_full = FacetBuilder.createUTCDateFormat();

	public AnalyticsReply runAnalysis(
			final AppContext userContext,
			String BBID,
			String stateId,
			final AnalyticsQuery query,
			DataLayout data,
			boolean computeGrowth,
			boolean applyFormatting,
			Integer timeout
			) throws InterruptedException, TimeoutException, ScopeException, ComputingException, SQLScopeException, RenderingException
	{
		Space space = null;// if we can initialize it, fine to report in the catch block
		//
		space = getSpace(userContext, BBID);
		//
		Bookmark bookmark = space.getBookmark();
		BookmarkConfig config = BookmarkManager.INSTANCE.readConfig(bookmark);
		// check the state
		if (stateId!=null && !stateId.equals("")) {
			// read the state
			StatePK pk = new StatePK(userContext.getCustomerId(), stateId);
			BookmarkConfig stateConfig = BookmarkManager.INSTANCE.readConfig(userContext, pk);
			if (stateConfig!=null) {
				config = stateConfig;
			}
		}
		//
		// merge the bookmark config with the query
		mergeBookmarkConfig(space, query, config);
		//
		// set limit of not defined
		if (query.getLimit()==null) {
			query.setLimit((long) 100);
		}
		// create the facet selection
		FacetSelection selection = createFacetSelection(space, query);
		// create the job
		final ProjectAnalysisJob job = createAnalysisJob(space, query, selection, OutputFormat.JSON);
		// options
		Map<String, Object> optionKeys = new HashMap<>();
		// -- growth ?
		if (!selection.getCompareTo().isEmpty()) {
			optionKeys.put(DashboardAnalysis.COMPUTE_GROWTH_OPTION_KEY, computeGrowth);
		}
		// -- applyFormatting
		if (applyFormatting) {
			optionKeys.put(DataMatrix.APPLY_FORMAT_OPTION, true);
		}
		job.setOptionKeys(optionKeys);
		// update the facet selection with actual values
		FacetSelection actual = computeFacetSelection(space, selection);
		//
		final boolean lazyFlag = (query.getLazy() != null) && (query.getLazy().equals("true") || query.getLazy().equals("noError"));
		//
		// create the AnalysisResult
		AnalyticsReply reply = new AnalyticsReply(space);
		//
		reply.setSelection(convertToSelection(userContext, query, space, job, actual));
		//
		reply.setQuery(query);
		//
		if (data==null) data=DataLayout.TABLE;
		if (data==DataLayout.SQL) {
			// bypassing the ComputingService
			AnalysisJobComputer computer = new AnalysisJobComputer();
			String sql = computer.viewSQL(userContext, job);
			reply.setResult(sql);
		} else {
			if (query.getStyle()==Style.HTML) {
				// change data format to legacy
				data=DataLayout.LEGACY;
				if (query.getLimit()>100 && query.getMaxResults()==null) {
					// try to apply maxResults
					query.setMaxResults(100);
				}
			}
			try {
				Callable<DataMatrix> task = new Callable<DataMatrix>() {
					@Override
					public DataMatrix call() throws Exception {
						return compute(userContext, job, query.getMaxResults(), query.getStartIndex(), lazyFlag);
					}
				};
				Future<DataMatrix> futur = ExecutionManager.INSTANCE.submit(userContext.getCustomerId(), task);
				DataMatrix matrix = null;
				if (timeout==null) {
					// using the customer execution engine to control load
					matrix = futur.get();
				} else {
					matrix = futur.get(timeout>1000?timeout:1000, java.util.concurrent.TimeUnit.MILLISECONDS);
				}
				if (data==DataLayout.LEGACY) {
					DataTable legacy = matrix.toDataTable(userContext, query.getMaxResults(), query.getStartIndex(), false, null);
					reply.setResult(legacy);
				} else {
					IDataMatrixConverter<Object[]> converter = getConverter(data);
					DataHeader header = computeDataHeader(matrix, computePrettyPrinterOptions(query.getStyle(), space));
					AnalyticsResult result = new AnalyticsResult();
					Object[] output = converter.convert(query, matrix);
					result.setHeader(header);
					result.setDataLayout(data);
					result.setData(output);
					result.setInfo(getAnalyticsResultInfo(output.length, query.getStartIndex(), matrix));
					reply.setResult(result);
				}
			} catch (NotInCacheException e) {
				if (query.getLazy().equals("noError")) {
					reply.setResult(new AnalyticsResult());
				} else {
					throw e;
				}
			} catch (ExecutionException e) {
				Throwable cause = e.getCause();
				if (cause instanceof NotInCacheException) {
					if (query.getLazy().equals("noError") || query.getStyle()==Style.HTML) {
						query.add(new Problem(Severity.ERROR, "SQL", "Lazy flag prevented to run the query: "+cause.toString(), cause));
						reply.setResult(new AnalyticsResult());
					} else {
						// now using a 404 instead of the 204
						throw new AnalyticsAPIException(cause, 404, query);
					}
				} else {
					if (query.getStyle()==Style.HTML) {
						// wrap the exception in a Problem
						query.add(new Problem(Severity.ERROR, "SQL", "Failed to run the query: "+cause.toString(), cause));
					} else {
						// just let it go
						throwCauseException(e);
					}
				}
			}
		}
		//
		return reply;
	}


	public Space getSpace(AppContext userContext, String BBID) {
		try {
			GlobalExpressionScope scope = new GlobalExpressionScope(userContext);
			ExpressionAST expr = scope.parseExpression(BBID);
			if (expr instanceof SpaceExpression) {
				SpaceExpression ref = (SpaceExpression)expr;
				Space space = ref.getSpace();
				return space;
			}
		} catch (ScopeException e) {
			throw new ObjectNotFoundAPIException("invalid REFERENCE: "+e.getMessage(), true);
		}
		// else
		throw new ObjectNotFoundAPIException("invalid REFERENCE", true);
	}


	/**
	 * merge the bookmark config with the current query. It modifies the query. Query parameters take precedence over the bookmark config.
	 *
	 * @param space
	 * @param query
	 * @param config
	 * @throws ScopeException
	 * @throws ComputingException
	 * @throws InterruptedException
	 */
	protected void mergeBookmarkConfig(Space space, AnalyticsQuery query, BookmarkConfig config) throws ScopeException, ComputingException, InterruptedException {
		ReferenceStyle prettyStyle = getReferenceStyle(query.getStyle());
		PrettyPrintOptions globalOptions = new PrettyPrintOptions(prettyStyle, null);
		UniverseScope globalScope = new UniverseScope(space.getUniverse());
		PrettyPrintOptions localOptions = new PrettyPrintOptions(prettyStyle, space.getTop().getImageDomain());
		SpaceScope localScope = new SpaceScope(space);
		if (query.getDomain() == null) {
			query.setDomain(space.prettyPrint(globalOptions));
		}
		if (query.getLimit() == null) {
			if (config!=null) {
				query.setLimit(config.getLimit());
			}
		}
		//
		// handling the period
		//
		if (query.getPeriod()==null && config!=null && config.getPeriod()!=null && !config.getPeriod().isEmpty()) {
			// look for this domain period
			String domainID = space.getDomain().getOid();
			String period = config.getPeriod().get(domainID);
			if (period!=null) {
				ExpressionAST expr = globalScope.parseExpression(period);
				IDomain image = expr.getImageDomain();
				if (image.isInstanceOf(IDomain.TEMPORAL)) {
					// ok, it's a date
					query.setPeriod(expr.prettyPrint(localOptions));
				}
			}
		}
		if (query.getPeriod()==null) {
			DomainHierarchy hierarchy = DomainHierarchyManager.INSTANCE.getHierarchy(space.getUniverse().getProject().getId(), space.getDomain(), false);
			for (DimensionIndex index : hierarchy.getDimensionIndexes()) {
				if (index.isVisible() && index.getDimension().getType().equals(Type.CONTINUOUS) && index.getAxis().getDefinitionSafe().getImageDomain().isInstanceOf(IDomain.TEMPORAL)) {
					// use it as period
					Axis axis = index.getAxis();
					ExpressionAST expr = new AxisExpression(axis);
					query.setPeriod(expr.prettyPrint(localOptions));
					if (query.getTimeframe()==null) {
						query.setTimeframe(new ArrayList<>());
						if (index.getStatus()==Status.DONE) {
							query.getTimeframe().add("__CURRENT_MONTH");
						} else {
							query.getTimeframe().add("__ALL");
						}
					}
					// quit the loop!
					break;
				}
			}
			if (query.getPeriod()==null) {
				// nothing selected - double check and auto detect?
				if (query.getTimeframe()!=null && query.getTimeframe().size()>0) {
					query.add(new Problem(Severity.WARNING, "period", "No period defined: you cannot set the timeframe"));
				}
				if (query.getCompareTo()!=null && query.getCompareTo().size()>0) {
					query.add(new Problem(Severity.WARNING, "period", "No period defined: you cannot set the compareTo"));
				}
			}
		}
		//
		// merging groupBy
		//
		boolean groupbyWildcard = isWildcard(query.getGroupBy());
		if (query.getGroupBy() == null || groupbyWildcard) {
			List<String> groupBy = new ArrayList<String>();
			if (config==null) {
				// it is not a bookmark, then we will provide default select *
				// only if there is nothing selected at all (groupBy & metrics)
				// or user ask for it explicitly is wildcard
				/*
				if (groupbyWildcard || query.getMetrics() == null) {
					boolean periodIsSet = false;
					if (query.getPeriod()!=null) {
						groupBy.add(query.getPeriod());
						periodIsSet = true;
					}
					// use a default pivot selection...
					// -- just list the content of the table
					for (Dimension dimension : space.getDimensions()) {
						Axis axis = space.A(dimension);
						try {
							DimensionIndex index = axis.getIndex();
							IDomain image = axis.getDefinitionSafe().getImageDomain();
							if (index!=null && index.isVisible() && index.getStatus()!=Status.ERROR && !image.isInstanceOf(IDomain.OBJECT)) {
								boolean isTemporal = image.isInstanceOf(IDomain.TEMPORAL);
								if (!isTemporal || !periodIsSet) {
									groupBy.add(axis.prettyPrint(localOptions));
									if (isTemporal) periodIsSet = true;
								}
							}
						} catch (ComputingException | InterruptedException e) {
							// ignore this one
						}
					}
				}
				 */
			} else if (config.getChosenDimensions() != null) {
				for (String chosenDimension : config.getChosenDimensions()) {
					try {
						String f = null;
						if (chosenDimension.startsWith("@")) {
							// need to fix the scope
							ExpressionAST expr = globalScope.parseExpression(chosenDimension);
							f = expr.prettyPrint(localOptions);//rewriteExpressionToLocalScope(expr, space);
							if (expr.getName() !=null && !expr.getName().isEmpty()){
								f+= " as '"+expr.getName()+"'";
							}
						} else {
							// legacy support raw ID
							// parse to validate and apply prettyPrint options
							try{
								ExpressionAST expr = globalScope.parseExpression(chosenDimension );
								f = expr.prettyPrint(localOptions);
								if (expr.getName() !=null && !expr.getName().isEmpty()){
									f+= " as '"+expr.getName()+"'";
								}

							}catch(ScopeException e){
								ExpressionAST expr = localScope.parseExpression("@'" + chosenDimension + "'");
								f = expr.prettyPrint(localOptions);
								if (expr.getName() !=null && !expr.getName().isEmpty()){
									f+= " as '"+expr.getName()+"'";
								}
							}
						}
						groupBy.add(f);
					} catch (ScopeException e) {
						query.add(new Problem(Severity.WARNING, chosenDimension, "failed to parse bookmark dimension: " + e.getMessage(), e));
					}
				}
			}
			if (groupbyWildcard) {
				query.getGroupBy().remove(0);// remove the first one
				groupBy.addAll(query.getGroupBy());// add reminding
			}
			query.setGroupBy(groupBy);
		}
		// merging Metrics
		boolean metricWildcard = isWildcard(query.getMetrics());
		if (query.getMetrics() == null || metricWildcard) {
			List<String> metrics = new ArrayList<>();
			if (config==null) {
				// T3036 - only keep the count()
				boolean someIntrinsicMetric = false;
				/*
				for (Measure measure : space.M()) {
					Metric metric = measure.getMetric();
					if (metric!=null && !metric.isDynamic()) {
						IDomain image = measure.getDefinitionSafe().getImageDomain();
						if (image.isInstanceOf(IDomain.AGGREGATE)) {
							Measure m = space.M(metric);
							metrics.add((new MeasureExpression(m)).prettyPrint(localOptions));
							//metrics.add(rewriteExpressionToLocalScope(new MeasureExpression(m), space));
							someIntrinsicMetric = true;
						}
					}
				}
				 */
				if (!someIntrinsicMetric) {
					metrics.add("count() as 'Count'// default metric");
				}
			} else if (config.getChosenMetrics() != null) {
				for (String chosenMetric : config.getChosenMetrics()) {
					// parse to validate and reprint
					try {
						// this is for legacy compatibility...
						ExpressionAST expr = localScope.parseExpression("@'" + chosenMetric + "'");
						String   m =expr.prettyPrint(localOptions) ;
						if (expr.getName()!=null && !expr.getName().isEmpty()){
							m+= " as '"+ expr.getName() +"'";
						}
						metrics.add(m);
					} catch (ScopeException e) {
						try {
							ExpressionAST expr = globalScope.parseExpression(chosenMetric);
							String   m =expr.prettyPrint(localOptions) ;
							if (expr.getName()!=null && !expr.getName().isEmpty()){
								m+= " as '"+ expr.getName() +"'";
							}
							metrics.add(m);
						} catch (ScopeException ee) {
							query.add(new Problem(Severity.WARNING, chosenMetric, "failed to parse bookmark metric: " + ee.getMessage(), ee));
						}
					}
				}
			} else if (config.getAvailableMetrics()!=null && (query.getGroupBy()==null || query.getGroupBy().isEmpty())) {
				// no axis selected, no choosen metrics, but available metrics
				// this is an old bookmark (analytics), that used to display the KPIs
				// so just compute the KPIs
				for (String availableMetric : config.getAvailableMetrics()) {
					// parse to validate and reprint
					try {
						ExpressionAST expr = localScope.parseExpression("@'" + availableMetric + "'");
						metrics.add(expr.prettyPrint(localOptions));
					} catch (ScopeException e) {
						query.add(new Problem(Severity.WARNING, availableMetric, "failed to parse bookmark metric: " + e.getMessage(), e));
					}
				}
			}
			if (metricWildcard) {
				query.getMetrics().remove(0);// remove the first one
				metrics.addAll(query.getMetrics());// add reminding
			}
			query.setMetrics(metrics);
		}
		if (query.getOrderBy() == null) {
			if (config!=null && config.getOrderBy()!=null) {
				query.setOrderBy(new ArrayList<String>());
				for (OrderBy orderBy : config.getOrderBy()) {
					// legacy issue? in some case the bookmark contains invalid orderBy expressions
					if (orderBy.getExpression()!=null) {
						ExpressionAST expr = globalScope.parseExpression(orderBy.getExpression().getValue());
						IDomain image = expr.getImageDomain();
						if (!image.isInstanceOf(DomainSort.DOMAIN)) {
							if (orderBy.getDirection()==Direction.ASC) {
								expr = ExpressionMaker.ASC(expr);
							} else {
								expr = ExpressionMaker.DESC(expr);
							}
						}
						query.getOrderBy().add(expr.prettyPrint(localOptions));
					}
				}
			}
		}
		if (query.getRollups() == null) {
			if (config!=null && config.getRollups()!=null && !config.getRollups().isEmpty()) {
				query.setRollups(new ArrayList<>());
				for (RollUp rollup : config.getRollups()) {
					query.getRollups().add(rollup.toString());
				}
			}
		}
		//
		// handling selection
		//
		FacetSelection selection = config!=null?config.getSelection():new FacetSelection();
		boolean filterWildcard = isWildcardFilters(query.getFilters());
		List<String> filters = query.getFilters()!=null?new ArrayList<>(query.getFilters()):new ArrayList<String>();
		if (filterWildcard) {
			filters.remove(0); // remove the *
		}
		String period = null;
		if (query.getPeriod()!=null && query.getTimeframe()==null) {
			ExpressionAST expr = localScope.parseExpression(query.getPeriod());
			period = expr.prettyPrint(new PrettyPrintOptions(ReferenceStyle.IDENTIFIER, null));
		}
		List<String> rootFilters = query.getRootFilters()!=null?new ArrayList<>(query.getRootFilters()):new ArrayList<String>();;
		if (selection != null) {
			if (!selection.getFacets().isEmpty()) {// always iterate over selection at least to capture the period
				boolean keepConfig = filterWildcard || filters.isEmpty();
				// look for the selection
				for (Facet facet : selection.getFacets()) {
					if (!facet.getSelectedItems().isEmpty()) {
						if (!SegmentManager.isSegmentFacet(facet) && globalScope.parseExpression(facet.getId()).prettyPrint(new PrettyPrintOptions(ReferenceStyle.IDENTIFIER, null)).equals(period)){
							// it's the period
							List<FacetMember> items = facet.getSelectedItems();
							if (items.size()==1) {
								FacetMember timeframe = items.get(0);
								if (timeframe instanceof FacetMemberInterval) {
									String upperBound = ((FacetMemberInterval) timeframe).getUpperBound();
									if (upperBound.startsWith("__")) {
										// it's a shortcut
										query.setTimeframe(Collections.singletonList(upperBound));
									} else {
										// it's a date
										String lowerBound = ((FacetMemberInterval) timeframe).getLowerBound();
										query.setTimeframe(new ArrayList<String>(2));
										query.getTimeframe().add(lowerBound);
										query.getTimeframe().add(upperBound);
									}
								}
							}
						} else if (SegmentManager.isSegmentFacet(facet) && keepConfig) {
							// it's the segment facet
							for (FacetMember item : facet.getSelectedItems()) {
								if (item instanceof FacetMemberString) {
									FacetMemberString member = (FacetMemberString)item;
									if (SegmentManager.isOpenFilter(member)) {
										// open filter is jut a formula
										String formula = member.getValue();
										if (formula.startsWith("=")) {
											formula = formula.substring(1);
										}
										filters.add(formula);
									} else {
										// it's a segment name
										// check the ID
										try {
											if (member.getId().startsWith("@")) {
												ExpressionAST seg = globalScope.parseExpression(member.getId());
												filters.add(seg.prettyPrint(localOptions));
											} else {
												// use the name
												ExpressionAST seg = globalScope.parseExpression("'"+member.getValue()+"'");
												filters.add(seg.prettyPrint(localOptions));
											}
										} catch (ScopeException e) {
											query.add(new Problem(Severity.ERROR, member.getId(), "Unable to parse segment with value='"+member+"'", e));
										}
									}
								}
							}
						} else if (keepConfig) {
							ExpressionAST expr = globalScope.parseExpression(facet.getId());
							String  filter = expr.prettyPrint(localOptions);
							if (facet.getSelectedItems().size()==1) {
								if (facet.getSelectedItems().get(0) instanceof FacetMemberString) {
									filter += "=";
									FacetMember member = facet.getSelectedItems().get(0);
									filter += "\""+member.toString()+"\"";
									filters.add(filter);
								}
							} else {
								filter += " IN {";
								boolean first = true;
								for (FacetMember member : facet.getSelectedItems()) {
									if (member instanceof FacetMemberString) {
										if (!first) {
											filter += " , ";
										} else {
											first = false;
										}
										filter += "\""+member.toString()+"\"";
									}
								}
								filter += "}";
								if (!first) {
									filters.add(filter);
								}
							}
						}
					}
				}

				if (selection.getRootFacets() != null && !selection.getRootFacets().isEmpty()) {
					for (Facet facet:selection.getRootFacets()) {
						ExpressionAST expr = globalScope.parseExpression(facet.getId());
						String  filter = expr.prettyPrint(localOptions);
						if (facet.getSelectedItems().size()==1) {
							if (facet.getSelectedItems().get(0) instanceof FacetMemberString) {
								filter += "=";
								FacetMember member = facet.getSelectedItems().get(0);
								filter += "\""+member.toString()+"\"";
								rootFilters.add(filter);
							}
						} else {
							filter += " IN {";
							boolean first = true;
							for (FacetMember member : facet.getSelectedItems()) {
								if (member instanceof FacetMemberString) {
									if (!first) {
										filter += " , ";
									} else {
										first = false;
									}
									filter += "\""+member.toString()+"\"";
								}
							}
							filter += "}";
							if (!first) {
								rootFilters.add(filter);
							}
						}
					}

				}
			}
			query.setRootFilters(rootFilters);
			query.setFilters(filters);
			//
			// check compareTo

			if (!selection.getCompareTo().isEmpty()) {
				for (Facet facet : selection.getCompareTo()) {
					if (!facet.getSelectedItems().isEmpty()) {

						ExpressionAST exprFacet = globalScope.parseExpression(facet.getId());
						if (exprFacet.prettyPrint(new PrettyPrintOptions(ReferenceStyle.IDENTIFIER, null)).equals(period)){
							// it's the period
							List<FacetMember> items = facet.getSelectedItems();
							if (items.size()==1) {
								FacetMember timeframe = items.get(0);
								if (timeframe instanceof FacetMemberInterval) {
									String upperBound = ((FacetMemberInterval) timeframe).getUpperBound();
									if (upperBound.startsWith("__")) {
										// it's a shortcut
										query.setCompareTo(Collections.singletonList(upperBound));
									} else {
										// it's a date
										String lowerBound = ((FacetMemberInterval) timeframe).getLowerBound();
										query.setCompareTo(new ArrayList<String>(2));
										query.getCompareTo().add(lowerBound);
										query.getCompareTo().add(upperBound);
									}
								}
							}
						}
					}
				}
			}
			//
			// check timeframe again
			if (query.getPeriod()!=null && (query.getTimeframe()==null || query.getTimeframe().size()==0)) {
				// add a default timeframe
				query.setTimeframe(Collections.singletonList("__CURRENT_MONTH"));
			}
		}
	}

	private PrettyPrintOptions.ReferenceStyle getReferenceStyle(Style style) {
		switch (style) {
			case HUMAN:
			case HTML:
				return ReferenceStyle.NAME;
			case LEGACY:
				return ReferenceStyle.LEGACY;
			case ROBOT:
			default:
				return ReferenceStyle.IDENTIFIER;
		}
	}

	private boolean isWildcard(List<String> facets) {
		if (facets !=null && !facets.isEmpty()) {
			String first = facets.get(0);
			return first.equals("*");
		}
		// else
		return false;
	}

	private boolean isWildcardFilters(List<String> items) {
		if (items !=null && !items.isEmpty()) {
			String first = items.get(0);
			return first.equals("*");
		} else {
			return false;// only return true if it is a real wildcard
		}
	}

	/**
	 *  update the facet selection with actual values
	 * @param space
	 * @param selection
	 * @return
	 */
	protected FacetSelection computeFacetSelection(Space space, FacetSelection selection) {
		try {
			FacetSelection rootSelection = new FacetSelection();
			rootSelection.getRootFacets().addAll(selection.getRootFacets());
			FacetSelection nonRootSelection = new FacetSelection();
			nonRootSelection.setCompareTo(selection.getCompareTo());
			nonRootSelection.setFacets(selection.getFacets());
			DashboardSelection ds = EngineUtils.getInstance()
					.applyFacetSelection(space.getUniverse().getContext(), space.getUniverse(), Collections.singletonList(space.getDomain()), nonRootSelection);
			DashboardSelection rootds = EngineUtils.getInstance()
					.applyFacetSelection(space.getUniverse().getContext(), space.getUniverse(), Collections.singletonList(space.getDomain()), rootSelection);
			//
			List<Facet> result = new ArrayList<>();
			List<Facet> rootResult = new ArrayList<>();
			result.addAll(ComputingService.INSTANCE.glitterFacets(space.getUniverse(),
					space.getDomain(), ds));
			rootResult.addAll(ComputingService.INSTANCE.glitterFacets(space.getUniverse(),
					space.getDomain(), rootds));
			FacetSelection facetSelectionResult = new FacetSelection();

			facetSelectionResult.setFacets(result);
			facetSelectionResult.setRootFacets(rootResult);

			if (ds.hasCompareToSelection()) {
				// create a fresh seelction with the compareTo
				DashboardSelection compareDS = new DashboardSelection();
				Domain domain = ds.getCompareToSelection().getDomain();
				compareDS.add(ds.getCompareToSelection());
				ArrayList<Facet> facets = new ArrayList<>();
				for (Axis filter : ds.getCompareToSelection().getFilters()) {
					facets.add(ComputingService.INSTANCE.glitterFacet(space.getUniverse(), domain, compareDS, filter, null, 0, 100, null));
				}
				facetSelectionResult.setCompareTo(facets);
			}
			return facetSelectionResult;
		} catch (TimeoutException | ScopeException | ComputingException | InterruptedException e1) {
			return selection;
		}
	}

	/**
	 * @param space
	 * @param query
	 * @param config
	 * @return
	 * @throws ScopeException
	 */
	protected FacetSelection createFacetSelection(Space space, AnalyticsQuery query) throws ScopeException {
		FacetSelection selection = new FacetSelection();
		SpaceScope scope = new SpaceScope(space);
		Domain domain = space.getDomain();
		// handle period & timeframe
		if (query.getPeriod()!=null && !query.getPeriod().equals("") && query.getTimeframe()!=null && query.getTimeframe().size()>0) {
			ExpressionAST expr = scope.parseExpression(query.getPeriod());
			Facet facet = createFacetInterval(space, expr, query.getTimeframe());
			selection.getFacets().add(facet);
		}
		// handle compareframe
		if (query.getPeriod()!=null && !query.getPeriod().equals("") && query.getCompareTo()!=null && query.getCompareTo().size()>0) {
			ExpressionAST expr = scope.parseExpression(query.getPeriod());
			Facet compareFacet = createFacetInterval(space, expr, query.getCompareTo());
			selection.setCompareTo(Collections.singletonList(compareFacet));
		}
		List<String> allFilters = new ArrayList<String>();
		if (query.getRootFilters() != null) {
			allFilters.addAll( query.getRootFilters());
		}
		if (query.getFilters() != null) {
			allFilters.addAll( query.getFilters());
		}
		// handle filters
		if (allFilters.size() >0) {
			Facet segment = SegmentManager.newSegmentFacet(domain);
			Facet rootSegment = SegmentManager.newSegmentFacet(domain);
			for (String filter : allFilters) {
				filter = filter.trim();
				if (!filter.equals("")) {
					try {
						ExpressionAST filterExpr = scope.parseExpression(filter);
						if (!filterExpr.getImageDomain().isInstanceOf(IDomain.CONDITIONAL)) {
							throw new ScopeException("Invalid filter, must be a condition");
						}
						Facet facet = createFacet(filterExpr);
						if (facet!=null) {
							if (query.getRootFilters().contains(filter)) {
								selection.getRootFacets().add(facet);
							} else {
								selection.getFacets().add(facet);
							}
						} else {
							FacetMemberString openFilter = SegmentManager.newOpenFilter(filterExpr, filter);
							if (query.getRootFilters().contains(filter)) {
								rootSegment.getSelectedItems().add(openFilter);
							} else {
								segment.getSelectedItems().add(openFilter);
							}
						}
					} catch (ScopeException e) {
						query.add(new Problem(Severity.ERROR, filter, "invalid filter definition: \n"+e.getMessage(), e));
					}
				}
			}
			if (!rootSegment.getSelectedItems().isEmpty()) {
				selection.getRootFacets().add(rootSegment);
			}
			if (!segment.getSelectedItems().isEmpty()) {
				selection.getFacets().add(segment);
			}
		}
		//
		return selection;
	}

	private Facet createFacet(ExpressionAST expr) {
		if (expr instanceof Operator) {
			Operator op = (Operator)expr;
			if (op.getOperatorDefinition().getId()==IntrinsicOperators.EQUAL & op.getArguments().size()==2) {
				ExpressionAST dim = op.getArguments().get(0);
				ExpressionAST value = op.getArguments().get(1);
				if (value instanceof ConstantValue) {
					Facet facet = new Facet();
					facet.setId(dim.prettyPrint());
					if (dim instanceof AxisExpression) {
						Axis axis = ((AxisExpression)dim).getAxis();
						if (axis.getDimension()!=null) {
							facet.setDimension(axis.getDimension());
							Object constant = ((ConstantValue)value).getValue();
							if (constant!=null) {
								facet.getSelectedItems().add(new FacetMemberString(constant.toString(), constant.toString()));
								return facet;
							}
						}
					}
				}
			} else if (op.getOperatorDefinition().getId()==IntrinsicOperators.IN & op.getArguments().size()==2) {
				ExpressionAST dim = op.getArguments().get(0);
				ExpressionAST second = op.getArguments().get(1);
				if (second instanceof Operator) {
					Operator vector = (Operator)second;
					if (vector.getOperatorDefinition().getExtendedID().equals(VectorOperatorDefinition.ID)) {
						Facet facet = new Facet();
						facet.setId(dim.prettyPrint());
						if (dim instanceof AxisExpression) {
							Axis axis = ((AxisExpression)dim).getAxis();
							facet.setDimension(axis.getDimension());
							if (axis.getDimension()!=null) {
								for (ExpressionAST value : vector.getArguments()) {
									Object constant = ((ConstantValue)value).getValue();
									if (constant!=null) {
										FacetMember member = new FacetMemberString(constant.toString(), constant.toString());
										facet.getSelectedItems().add(member);
									}
								}
								if (!facet.getSelectedItems().isEmpty()) {
									return facet;
								}
							}
						}
					}
				}
			}
		}
		// else
		return null;
	}

	private Facet createFacetInterval(Space space, ExpressionAST expr, List<String> values) throws ScopeException {
		Facet facet = new Facet();
		facet.setId(rewriteExpressionToGlobalScope(expr, space));
		if (expr instanceof AxisExpression) {
			Axis axis = ((AxisExpression)expr).getAxis();
			facet.setDimension(axis.getDimension());
		} else {
			Dimension fake = new Dimension();
			fake.setType(Type.CONTINUOUS);
		}
		String lowerbound = values.get(0);
		String upperbound = values.size()==2?values.get(1):lowerbound;
		FacetMemberInterval member = new FacetMemberInterval(lowerbound, upperbound);
		facet.getSelectedItems().add(member);
		return facet;
	}

	protected ProjectAnalysisJob createAnalysisJob(Space root, AnalyticsQuery query, FacetSelection selection, OutputFormat format) throws ScopeException {
		// read the domain reference
		if (query.getDomain() == null) {
			throw new ScopeException("incomplete specification, you must specify the data domain expression");
		}
		Universe universe = root.getUniverse();
		Domain domain = root.getDomain();
		UniverseScope globalScope = new UniverseScope(universe);

		//AccessRightsUtils.getInstance().checkRole(universe.getContext(), domain, AccessRight.Role.READ);
		// handle the columns
		List<Metric> metrics = new ArrayList<Metric>();
		List<FacetExpression> facets = new ArrayList<FacetExpression>();
		//DomainExpressionScope domainScope = new DomainExpressionScope(universe, domain);
		int facetCount = 0;
		int legacyFacetCount = 0;// count how much real facets we have to
		// translate indexes
		int legacyMetricCount = 0;
		HashMap<Integer, Integer> lookup = new HashMap<>();// convert simple
		// indexes into
		// analysisJob
		// indexes
		HashSet<Integer> metricSet = new HashSet<>();// mark metrics
		if ((query.getGroupBy() == null || query.getGroupBy().isEmpty())
				&& (query.getMetrics() == null || query.getMetrics().isEmpty())) {
			throw new ScopeException("this is an empty query (not column provided), can't run the analysis: try setting the groupBy or metrics parameters");
		}
		// now we are going to use the domain Space scope
		SpaceScope scope = new SpaceScope(root);
		// add the period parameter if available
		if (query.getPeriod()!=null && !query.getPeriod().equals("")) {
			try {
				ExpressionAST period = scope.parseExpression(query.getPeriod());
				scope.addParam("__PERIOD", period);
			} catch (ScopeException e) {
				// ignore
			}
		}
		// quick fix to support the old facet mechanism
		ArrayList<String> analysisFacets = new ArrayList<>();
		if (query.getGroupBy()!=null) analysisFacets.addAll(query.getGroupBy());
		if (query.getMetrics()!=null) analysisFacets.addAll(query.getMetrics());
		for (String facet : analysisFacets) {
			if (facet!=null && facet.length()>0) {// ignore empty values
				ExpressionAST colExpression = scope.parseExpression(facet);
				IDomain image = colExpression.getImageDomain();
				if (image.isInstanceOf(IDomain.AGGREGATE) || image.isInstanceOf(AnalyticDomain.DOMAIN)) {
					IDomain source = colExpression.getSourceDomain();
					String name = colExpression.getName();// T1807
					if (!source.isInstanceOf(DomainDomain.DOMAIN)) {
						// need to add the domain
						// check if it needs grouping?
						if (colExpression instanceof Operator) {
							Operator op = (Operator)colExpression;
							if (op.getArguments().size()>1 && op.getOperatorDefinition().getPosition()!=OperatorDefinition.PREFIX_POSITION) {
								colExpression = ExpressionMaker.GROUP(colExpression);
							}
						}
						// add the domain// relink with the domain
						colExpression = ExpressionMaker.COMPOSE(new DomainReference(universe, domain), colExpression);
					}
					// now it can be transformed into a measure
					Measure m = universe.asMeasure(colExpression);
					if (m == null) {
						throw new ScopeException("cannot use expression='" + facet + "'");
					}
					Metric metric = new Metric();
					metric.setExpression(new Expression(m.prettyPrint()));
					if (name == null) {
						name = m.getName();
					}
					metric.setName(name);
					metrics.add(metric);
					//
					lookup.put(facetCount, legacyMetricCount++);
					metricSet.add(facetCount);
					facetCount++;
				} else {
					// it's a dimension
					IDomain source = colExpression.getSourceDomain();
					String name = colExpression.getName();// T1807
					if (!source.isInstanceOf(DomainDomain.DOMAIN)) {
						// need to add the domain
						// check if it needs grouping?
						if (colExpression instanceof Operator) {
							Operator op = (Operator)colExpression;
							if (op.getArguments().size()>1 && op.getOperatorDefinition().getPosition()!=OperatorDefinition.PREFIX_POSITION) {
								colExpression = ExpressionMaker.GROUP(colExpression);
							}
						}
						// add the domain// relink with the domain
						colExpression = ExpressionMaker.COMPOSE(new DomainReference(universe, domain), colExpression);
					}
					Axis axis = root.getUniverse().asAxis(colExpression);
					if (axis == null) {
						throw new ScopeException("cannot use expression='" + colExpression.prettyPrint() + "'");
					}
					if (name!=null) {
						axis.setName(name);
					}
					facets.add(new FacetExpression(axis.prettyPrint(), axis.getName()));
					//
					lookup.put(facetCount, legacyFacetCount++);
					facetCount++;
				}
			}
		}

		// handle orderBy
		List<OrderBy> orderBy = new ArrayList<>();
		int pos = 1;
		if (query.getOrderBy() != null) {
			for (String order : query.getOrderBy()) {
				if (order != null && order.length()>0) {
					// let's try to parse it
					try {
						ExpressionAST expr = scope.parseExpression(order);
						IDomain image = expr.getImageDomain();
						Direction direction = getDirection(image);
						Nulls nulls = getNullsPosition(image) ;
						if (expr  instanceof NumericConstant) {
							// it is a reference to the facets
							DomainNumericConstant num = (DomainNumericConstant) image
									.getAdapter(DomainNumericConstant.class);
							int index = num.getValue().intValue();
							if (!lookup.containsKey(index)) {
								throw new ScopeException("the orderBy index specified (" + index + ") is out of bounds");
							}
							int legacy = lookup.get(index);
							if (metricSet.contains(index)) {
								legacy += legacyFacetCount;
							}
							orderBy.add(new OrderBy(legacy, direction));
						} else {
							// it's an expression which is now scoped into the bookmark
							// but job is expecting it to be scoped in the universe... (OMG)
							// also we must remove the sort operator to avoid nasty SQL error when generating the SQL
							expr = unwrapOrderByExpression(expr);
							String universalExpression = rewriteExpressionToGlobalScope(expr, root);
							orderBy.add(new OrderBy(new Expression(universalExpression), direction, nulls));
						}
					} catch (ScopeException e) {
						try {

							ExpressionAST expr = globalScope.parseExpression(order);
							expr = unwrapOrderByExpression(expr);
							IDomain image = expr.getImageDomain();
							Direction direction = getDirection(image);
							Nulls nulls = getNullsPosition(image) ;
							String universalExpression = rewriteExpressionToGlobalScope(expr, root);
							orderBy.add(new OrderBy(new Expression(universalExpression), direction, nulls));

						}catch(ScopeException e1){
							throw new ScopeException(
									"unable to parse orderBy expression at position " + pos + ": " + e1.getMessage(), e1);
						}
					}
				}
				pos++;
			}
		}
		// handle rollup - fix indexes
		pos = 1;
		List<RollUp> rollups = new ArrayList<>();
		if (query.getRollups() != null) {
			for (String value : query.getRollups()) {
				RollUp rollup = parseRollup(value, pos);
				if (rollup!=null && rollup.getCol() > -1) {// ignore grand-total
					// can't rollup on metric
					if (metricSet.contains(rollup.getCol())) {
						throw new ScopeException(
								"invalid rollup expression at position " + pos + ": the index specified ("
										+ rollup.getCol() + ") is not valid: cannot rollup on metric");
					}
					if (!lookup.containsKey(rollup.getCol())) {
						throw new ScopeException("invalid rollup expression at position " + pos
								+ ": the index specified (" + rollup.getCol() + ") is out of bounds");
					}
					int legacy = lookup.get(rollup.getCol());
					rollup.setCol(legacy);
				}
				if (rollup!=null) {
					rollups.add(rollup);
				}
			}
		}

		// create the actual job
		// - using the AnalysisQuery.getQueryID() as the job OID: this one is unique for a given query
		ProjectAnalysisJobPK pk = new ProjectAnalysisJobPK(universe.getProject().getId(), query.getQueryID());
		ProjectAnalysisJob analysisJob = new ProjectAnalysisJob(pk);
		analysisJob.setDomains(Collections.singletonList(domain.getId()));
		analysisJob.setMetricList(metrics);
		analysisJob.setFacets(facets);
		analysisJob.setOrderBy(orderBy);
		analysisJob.setSelection(selection);
		analysisJob.setRollups(rollups);
		analysisJob.setAutoRun(true);

		// automatic limit?
		if (query.getLimit() == null && format == OutputFormat.JSON) {
			int complexity = analysisJob.getFacets().size();
			if (complexity < 4) {
				analysisJob.setLimit((long) Math.pow(10, complexity + 1));
			} else {
				analysisJob.setLimit(100000L);
			}
		} else {
			analysisJob.setLimit(query.getLimit());
		}

		// offset
		if (query.getOffset()!=null) {
			analysisJob.setOffset(query.getOffset());
		}

		// beyond limit
		if (query.getBeyondLimit()!=null && !query.getBeyondLimit().isEmpty()) {
			ArrayList<Index> indexes = new ArrayList<>(query.getBeyondLimit().size());
			for (String value : query.getBeyondLimit()) {
				// check if it is a number
				Integer x = getIntegerValue(value);
				if (x==null || x<0 && x>=query.getGroupBy().size()) {
					x = query.getGroupBy().indexOf(value);
					if (x<0) {
						// try harder
						try {
							ExpressionAST valExpr = scope.parseExpression(value);
							for (int i=0;i<query.getGroupBy().size();i++) {
								ExpressionAST expr = scope.parseExpression(query.getGroupBy().get(i));
								if (valExpr.equals(expr)) {
									x = i;
									break;
								}
							}
						} catch (ScopeException e) {
							// ignore
						}
					}
				}
				if (x==null || x<0) {
					query.add(new Problem(Severity.WARNING, "beyondLimit", "invalid beyondLimit parameter: "+value+": ignored:  must be an valid integer position or a groupBy expression"));
				} else {
					indexes.add(new Index(x));
				}
			}
			analysisJob.setBeyondLimit(indexes);
		}
		return analysisJob;
	}

	protected List<RollUp> parseRollups(List<String> values) throws ScopeException {
		List<RollUp> rollups = new ArrayList<>();
		int pos = 1;
		for (String value : values) {
			RollUp rollup = parseRollup(value, pos++);
			if (rollup!=null) {
				rollups.add(rollup);
			}
		}
		return rollups;
	}

	private RollUp parseRollup(String value, int pos) throws ScopeException {
		if (value!=null && !value.equals("")) {
			RollUp rollup = new RollUp();
			Position position = Position.FIRST;// default
			Pattern lastPattern = Pattern.compile("(\\w+)\\((-?\\d+)\\)", Pattern.CASE_INSENSITIVE);
			value = value.trim().toLowerCase();
			Matcher matcher = lastPattern.matcher(value);
			if (matcher.matches()) {
				String op = matcher.group(1).toUpperCase();
				if (op.equals("LAST")) {
					position = Position.LAST;
				} else if (op.equals("FIRST")) {
					position = Position.FIRST;
				} else {
					throw new ScopeException("invalid rollup expression at position " + pos
							+ ": must be a valid indexe N or the expression FIRST(N) or LAST(N) to set the rollup position");
				}
				value = matcher.group(2);
			}
			try {
				int index = Integer.parseInt(value);
				if (index<-1) {
					throw new ScopeException("invalid rollup expression at position " + pos
							+ ": the index specified (" + rollup.getCol() + ") is out of bounds, must be -1 for grand total or a valid groupBy column index");
				}
				rollup.setCol(index);
				rollup.setPosition(position);
			} catch (NumberFormatException e) {
				throw new ScopeException("invalid rollup expression at position " + pos
						+ ": must be a valid indexe N or the expression FIRST(N) or LAST(N) to set the rollup position");
			}
			return rollup;
		}
		// else return null, it's not an error
		return null;
	}

	private Integer getIntegerValue(String value) {
		try {
			return Integer.parseInt(value);
		} catch (NumberFormatException e) {
			return null;
		}
	}

	/**
	 * if the orderBy expression is DESC(x) or ASC(x), just unwrap and return x
	 * else do nothing
	 * @param orderBy
	 * @return
	 */
	private ExpressionAST unwrapOrderByExpression(ExpressionAST expr) {
		if (expr.getImageDomain().isInstanceOf(DomainSort.DOMAIN) && expr instanceof Operator) {
			// remove the first operator
			Operator op = (Operator)expr;
			if (op.getArguments().size()==1
					&& op.getOperatorDefinition() instanceof SortOperatorDefinition)
			{
				return op.getArguments().get(0);
			}
		}
		// else do nothing
		return expr;
	}

	/**
	 * rewrite a local expression valid in the root scope as a global expression
	 * @param expr
	 * @param root
	 * @return
	 * @throws ScopeException
	 */
	private String rewriteExpressionToGlobalScope(ExpressionAST expr, Space root) throws ScopeException {
		IDomain source = expr.getSourceDomain();
		if (!source.isInstanceOf(DomainDomain.DOMAIN)) {
			String global = root.prettyPrint();
			String value = expr.prettyPrint();
			return global+".("+value+")";
		} else {
			if (expr.getName()!=null && ! expr.getName().isEmpty()){
				return expr.prettyPrint() + " as '"  +  expr.getName() + "'";
			} else {
				return expr.prettyPrint();
			}
		}
	}

	private Direction getDirection(IDomain domain) {
		if (domain.isInstanceOf(DomainSort.DOMAIN)) {
			DomainSort sort = (DomainSort) domain.getAdapter(DomainSort.class);
			if (sort != null) {
				SortDirection direction = sort.getDirection();
				if (direction != null) {
					switch (direction) {
						case ASC:
							return Direction.ASC;
						case DESC:
							return Direction.DESC;
					}
				}
			}
		}
		// else
		// no desc | asc operator provided: use default
		if (domain.isInstanceOf(IDomain.NUMERIC) || domain.isInstanceOf(IDomain.TEMPORAL)) {
			return Direction.DESC;
		} else { //if (image.isInstanceOf(IDomain.STRING)) {
			return Direction.ASC;
		}
	}

	private Nulls getNullsPosition(IDomain domain) {
		if (domain.isInstanceOf(DomainSort.DOMAIN)) {
			DomainSort sort = (DomainSort) domain.getAdapter(DomainSort.class);
			if (sort != null) {
				NullsPosition nulls = sort.getNullsPosition();
				if (nulls != null) {
					switch (nulls) {
						case NULLS_FIRST:
							return Nulls.NULLS_FIRST;
						case NULLS_LAST:
							return Nulls.NULLS_LAST;
						default:
							return Nulls.UNDEFINED;
					}
				}
			}
		}
		// else
		// no desc | asc operator provided: use default
		return Nulls.UNDEFINED;
	}

	/**
	 * Convert a FacetSelection into a AnalyticsSelection, suitable to use with the analytics API
	 * @param ctx
	 * @param job
	 * @param actual
	 * @return
	 */
	protected AnalyticsSelection convertToSelection(AppContext ctx, AnalyticsQuery query, Space space, ProjectAnalysisJob job, FacetSelection actual) {
		AnalyticsSelection selection = new AnalyticsSelectionImpl();
		//
		selection.setPeriod(query.getPeriod());
		// using the right style!
		ReferenceStyle prettyStyle = getReferenceStyle(query.getStyle());
		PrettyPrintOptions localOptions = new PrettyPrintOptions(prettyStyle, space.getTop().getImageDomain());
		//
		try {
			ArrayList<String> filters = new ArrayList<>();
			DashboardAnalysis ds = AnalysisJobComputer.buildDashboardAnalysis(ctx, job);
			for (DomainSelection sel : ds.getSelection().get()) {
				for (ExpressionInput cond : sel.getConditions()) {
					filters.add(cond.getInput());
				}
				for (Axis axis : sel.getFilters()) {
					if (query.getPeriod()!=null && !query.getPeriod().equals("") && checkAxisIsPeriod(query.getPeriod(), axis, space)) {
						Collection<DimensionMember> members = sel.getMembers(axis);
						if (members.size()==1) {
							DimensionMember member = members.iterator().next();
							Object value = member.getID();
							if (value instanceof Intervalle) {
								Intervalle interval = (Intervalle)value;
								ArrayList<String> timeframe = new ArrayList<>();
								timeframe.add(ISO8601_full.format(interval.getLowerBound()));
								timeframe.add(ISO8601_full.format(interval.getUpperBound()));
								selection.setTimeframe(timeframe);
							}
						}
					} else {
						Collection<DimensionMember> members = sel.getMembers(axis);
						ExpressionAST expr = convertToExpression(axis, members);
						if (expr!=null) {
							filters.add(expr.prettyPrint(localOptions));
						}
					}
				}
			}
			selection.setFilters(filters);
			//
			if (actual.getCompareTo()!=null && !actual.getCompareTo().isEmpty()) {
				DomainSelection sel = ds.getSelection().getCompareToSelection();
				// only handling PERIOD
				for (Axis axis : sel.getFilters()) {
					if (query.getPeriod()!=null && !query.getPeriod().equals("") && checkAxisIsPeriod(query.getPeriod(), axis, space)) {
						Collection<DimensionMember> members = sel.getMembers(axis);
						if (members.size()==1) {
							DimensionMember member = members.iterator().next();
							Object value = member.getID();
							if (value instanceof Intervalle) {
								Intervalle interval = (Intervalle)value;
								ArrayList<String> timeframe = new ArrayList<>();
								timeframe.add(ISO8601_full.format(interval.getLowerBound()));
								timeframe.add(ISO8601_full.format(interval.getUpperBound()));
								selection.setCompareTo(timeframe);
							}
						}
					}
				}
			}
		} catch (InterruptedException | ComputingException | ScopeException | SQLScopeException e) {
			// ignore
		}
		return selection;
	}

	protected ExpressionAST convertToExpression(Axis axis, Collection<DimensionMember> filters)
			throws ScopeException, SQLScopeException {
		//
		ExpressionAST expr = axis.getReference();
		// ticket:3014 - handles predicates
		if (expr.getImageDomain().isInstanceOf(IDomain.CONDITIONAL)) {
			// ok, apply the predicate only if filters == [true]
			if (filters.size() == 1) {
				// get the first
				Iterator<DimensionMember> iter = filters.iterator();
				DimensionMember member = iter.next();
				if (member.getID() instanceof Boolean && ((Boolean) member.getID()).booleanValue()) {
					// if true, add the predicate
					return expr;
				}
			}
			// else
			return null;
		}
		boolean filter_by_null = false;// T1198
		List<Object> filter_by_members = new ArrayList<Object>();
		ExpressionAST filter_by_intervalle = null;
		for (DimensionMember filter : filters) {
			Object value = filter.getID();
			// check if the member is an interval
			if (value instanceof Intervalle) {
				ExpressionAST where = convertToInterval(expr, (Intervalle) value);
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
		//
		return filterALL;
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

	boolean checkFacetIsPeriod(String period, Facet facet, Space space) {
		try {
			// we must parse the period
			SpaceScope scope = new SpaceScope(space);
			ExpressionAST expr = scope.parseExpression(period);
			Axis axis = space.getUniverse().axis(facet.getId());
			ExpressionAST check = scope.createReferringExpression(axis);
			return check.equals(expr);
		} catch (ScopeException e) {
			return false;
		}
	}

	boolean checkAxisIsPeriod(String period, Axis axis, Space space) {
		try {
			// we must parse the period
			SpaceScope scope = new SpaceScope(space);
			ExpressionAST expr = scope.parseExpression(period);
			ExpressionAST check = scope.createReferringExpression(axis);
			return check.equals(expr);
		} catch (ScopeException e) {
			return false;
		}
	}

	/**
	 * moved some legacy code out of AnalysisJobComputer
	 * => still need to bypass the ProjectAnalysisJob
	 * @param ctx
	 * @param job
	 * @param maxResults
	 * @param startIndex
	 * @param lazy
	 * @return
	 * @throws ComputingException
	 * @throws InterruptedException
	 */
	private DataMatrix compute(AppContext ctx, ProjectAnalysisJob job, Integer maxResults, Integer startIndex,
			boolean lazy) throws ComputingException, InterruptedException {
		// build the analysis
		long start = System.currentTimeMillis();
		logger.info("Starting preview compute for job " + job.getId());
		DashboardAnalysis analysis;
		try {
			analysis = AnalysisJobComputer.buildDashboardAnalysis(ctx, job, lazy);
		} catch (Exception e) {
			throw new ComputingException(e);
		}
		// run the analysis
		DataMatrix datamatrix = ComputingService.INSTANCE.glitterAnalysis(analysis, null);
		if (lazy && (datamatrix == null)) {
			throw new NotInCacheException("Lazy preview, analysis " + analysis.getJobId() + "  not in cache");
		} else {
			job.setRedisKey(datamatrix.getRedisKey());
			long stop = System.currentTimeMillis();
			logger.info("task=" + this.getClass().getName() + " method=compute" + " jobid="
					+ job.getId().getAnalysisJobId() + " duration=" + (stop - start));
			JobStats queryLog = new JobStats(job.getId().getAnalysisJobId(), "AnalysisJobComputer.compute",
					(stop - start), job.getId().getProjectId());
			queryLog.setError(false);
			//			PerfDB.INSTANCE.save(queryLog);
			return datamatrix;
		}
	}

	private IDataMatrixConverter<Object[]> getConverter(DataLayout format) {
		if (format==DataLayout.TABLE) {
			return new TableConverter();
		} else if (format==DataLayout.RECORDS) {
			return new RecordConverter();
		} else if (format==DataLayout.TRANSPOSE) {
			return new TransposeConverter();
		} else {
			throw new InvalidIdAPIException("invalid format="+format, true);
		}
	}

	/**
	 * @param style
	 * @param space
	 * @return
	 */
	private PrettyPrintOptions computePrettyPrinterOptions(Style style, Space space) {
		if (style==Style.HTML || style==Style.HUMAN) {
			return new PrettyPrintOptions(ReferenceStyle.NAME, space.getImageDomain());
		} else if (style==Style.ROBOT) {
			return new PrettyPrintOptions(ReferenceStyle.IDENTIFIER, space.getImageDomain());
		} else if (style==Style.LEGACY) {
			// top level reference
			return new PrettyPrintOptions(ReferenceStyle.IDENTIFIER, null);
		} else {
			// error ?
			return new PrettyPrintOptions(ReferenceStyle.IDENTIFIER, space.getImageDomain());
		}
	}
	private DataHeader computeDataHeader(DataMatrix dm, PrettyPrintOptions options) {
		// export header
		DataHeader header = new DataHeader();
		List<AxisValues> axes = dm.getAxes();
		List<MeasureValues> kpis = dm.getKPIs();
		int pos = 0;
		for (int i = 0; i < axes.size(); i++) {
			AxisValues m = axes.get(i);
			if (m.isVisible()) {
				Dimension dim = m.getAxis().getDimension();
				ExtendedType colExtType;
				if (dim != null) {
					colExtType = getExtendedType(m.getAxis().getDefinitionSafe(), dm.getDatabase().getSkin());
					Column col = new Column();
					col.setPos(pos++);
					col.setName(m.getAxis().getName());
					col.setDefinition(m.getAxis().prettyPrint(options));
					col.setDescription(m.getAxis().getDescription());
					col.setFormat(computeFormat(m.getAxis(), colExtType));
					col.setRole(com.squid.kraken.v4.model.DataHeader.Role.GROUPBY);
					/*
					Col col = new Col(dim.getId(), m.getAxis().getName(), colExtType, Col.Role.DOMAIN, pos++);
					col.setDefinition(m.getAxis().prettyPrint());
					col.setOriginType(m.getAxis().getOriginType());
					col.setDescription(m.getAxis().getDescription());
					col.setFormat(computeFormat(m.getAxis(), colExtType));
					 */
					header.getColumns().add(col);
				} else {
					String name = m.getAxis().getName();
					colExtType = getExtendedType(m.getAxis().getDefinitionSafe(), dm.getDatabase().getSkin());
					Column col = new Column();
					col.setPos(pos++);
					col.setName(name);
					col.setDefinition(m.getAxis().prettyPrint(options));
					col.setRole(com.squid.kraken.v4.model.DataHeader.Role.GROUPBY);
					/*
					DimensionPK pk = new DimensionPK(m.getAxis().getParent().getDomain().getId(), ID);
					Col col = new Col(pk, name, colExtType, Col.Role.DOMAIN, pos++);
					if (def != null)
						col.setDefinition(m.getAxis().prettyPrint());
					col.setOriginType(m.getAxis().getOriginType());
					 */
					header.getColumns().add(col);
				}
			}
		}
		for (int i = 0; i < kpis.size(); i++) {
			MeasureValues v = kpis.get(i);
			if (v.isVisible()) {
				Measure m = v.getMeasure();
				m.getMetric();
				ExtendedType type = getExtendedType(m.getDefinitionSafe(), dm.getDatabase().getSkin());
				Column col = new Column();
				/*
				Col col = new Col(metric != null ? metric.getId() : null, m.getName(), type, Col.Role.DATA, pos++);
				 */
				col.setPos(pos++);
				col.setName(m.getName());
				col.setDefinition(m.prettyPrint(options));
				col.setDescription(m.getDescription());
				col.setFormat(computeFormat(m, type));
				col.setRole(com.squid.kraken.v4.model.DataHeader.Role.METRIC);
				//
				header.getColumns().add(col);
			}
		}
		return header;
	}

	private com.squid.kraken.v4.model.DataHeader.DataType getDataType(Axis axis) {
		ExpressionAST expr = axis.getDefinitionSafe();
		IDomain image = expr.getImageDomain();
		if (image.isInstanceOf(IDomain.DATE)) {
			return com.squid.kraken.v4.model.DataHeader.DataType.DATE;
		} else if (image.isInstanceOf(IDomain.TIMESTAMP)) {
			return com.squid.kraken.v4.model.DataHeader.DataType.DATE;
		} else if (image.isInstanceOf(IDomain.NUMERIC)) {
			return com.squid.kraken.v4.model.DataHeader.DataType.NUMBER;
		} else if (image.isInstanceOf(IDomain.STRING)) {
			return com.squid.kraken.v4.model.DataHeader.DataType.STRING;
		} else {
			return com.squid.kraken.v4.model.DataHeader.DataType.STRING;
		}
	}

	private ExtendedType getExtendedType(ExpressionAST expr, SQLSkin skin) {
		return expr.computeType(skin);
	}

	private String computeFormat(Axis axis, ExtendedType type) {
		IDomain image = axis.getDefinitionSafe().getImageDomain();
		if (image.isInstanceOf(IDomain.NUMERIC))
			return null;
		if (axis.getFormat() != null) {
			return axis.getFormat();
		} else {
			return computeFormat(type);
		}
	}

	private String computeFormat(Measure measure, ExtendedType type) {
		if (measure.getFormat() != null) {
			return measure.getFormat();
		} else {
			return computeFormat(type);
		}
	}

	/**
	 * @param type
	 * @return
	 */
	private String computeFormat(ExtendedType type) {
		IDomain image = type.getDomain();
		if (image.isInstanceOf(IDomain.TIMESTAMP)) {
			return "%tY-%<tm-%<tdT%<tH:%<tM:%<tS.%<tLZ";
		}
		if (image.isInstanceOf(IDomain.NUMERIC)) {
			switch (type.getDataType()) {
				case Types.INTEGER:
				case Types.BIGINT:
				case Types.SMALLINT:
				case Types.TINYINT:
					return "%,d";
				case Types.DOUBLE:
				case Types.DECIMAL:
				case Types.FLOAT:
				case Types.NUMERIC:
					if (type.getScale() > 0) {
						return "%,.2f";
					} else {
						return "%,d";
					}
				default:
					break;
			}
		}
		// else
		return null;
	}

	/**
	 * @param matrix
	 * @return
	 */
	private ResultInfo getAnalyticsResultInfo(Integer pageSize, Integer startIndex, DataMatrix matrix) {
		ResultInfo info = new ResultInfo();
		info.setFromCache(matrix.isFromCache());
		info.setFromSmartCache(matrix.isFromSmartCache());// actually we don't know the origin, see T1851
		info.setExecutionDate(matrix.getExecutionDate().toString());
		info.setStartIndex(startIndex);
		info.setPageSize(pageSize);
		info.setTotalSize(matrix.getRows().size());
		info.setComplete(matrix.isFullset());
		return info;
	}

	/**
	 * Try to find the most relevant exception in the stack
	 * @param the execution exception
	 */
	protected void throwCauseException(ExecutionException e) {
		Throwable cause = getCauseException(e);
		throwAPIException(cause);
	}

	private Throwable getCauseException(ExecutionException e) {
		Throwable previous = e;
		Throwable last = e;
		while (last.getCause()!=null) {
			previous = last;
			last = last.getCause();
		}
		if (previous.getMessage()!=null) {
			return previous;
		} else {
			return last;
		}
	}

	/**
	 * @param previous
	 * @return
	 */
	private void throwAPIException(Throwable exception) throws APIException {
		if (exception instanceof APIException) {
			throw (APIException)exception;
		} else {
			throw new APIException(exception, true);
		}
	}

	public ViewReply viewAnalysis(
			final AppContext userContext,
			String BBID,
			ViewQuery view,
			String data,
			boolean preFetch, boolean computeInfo) throws ScopeException, ComputingException, InterruptedException {
		Space space = getSpace(userContext, BBID);
		//
		if (data==null) data="URL";
		//
		Bookmark bookmark = space.getBookmark();
		BookmarkConfig config = BookmarkManager.INSTANCE.readConfig(bookmark);
		//
		// handle the limit
		Long explicitLimit = view.getLimit();
		AnalyticsQueryImpl query = new AnalyticsQueryImpl(view);
		// merge the bookmark config with the query
		mergeBookmarkConfig(space, query, config);
		//
		// change the query ref to use the domain one
		// - we don't want to have side-effects
		String domainBBID = "@'"+space.getUniverse().getProject().getOid()+"'.@'"+space.getDomain().getOid()+"'";
		query.setBBID(domainBBID);
		//
		Properties options = view.getOptionsAsPropertiesSafe();
		//
		VegaliteConfigurator inputConfig = new VegaliteConfigurator(space, query);
		// first check the provided parameters, because they must override the default
		VegaliteSpecs channels = new VegaliteSpecs();
		channels.encoding.x = inputConfig.createChannelDef("x", view.getX(), options);
		channels.encoding.y = inputConfig.createChannelDef("y", view.getY(), options);
		channels.encoding.color = inputConfig.createChannelDef("color", view.getColor(), options);
		channels.encoding.size = inputConfig.createChannelDef("size", view.getSize(), options);
		channels.encoding.column = inputConfig.createChannelDef("column", view.getColumn(), options);
		channels.encoding.row = inputConfig.createChannelDef("row", view.getRow(), options);
		if (inputConfig.getRequired().getMetrics().size()>0) {
			// override the default metrics
			query.setMetrics(inputConfig.getRequired().getMetrics());
		}
		if (inputConfig.getRequired().getGroupBy().size()>0) {
			// override the default metrics
			query.setGroupBy(inputConfig.getRequired().getGroupBy());
		}
		//
		// add the compareTo() if needed
		/*
		if (query.getCompareframe()!=null && query.getMetrics()!=null && query.getMetrics().size()==1) {
			String expression = query.getMetrics().get(0);
			ExpressionAST ast = inputConfig.parse(expression);
			query.getMetrics().add("compareTo("+inputConfig.prettyPrint(ast)+")");
		}
		 */
		//
		int dims = (query.getGroupBy()!=null)?query.getGroupBy().size():0;
		int kpis = (query.getMetrics()!=null)?query.getMetrics().size():0;
		// this will be the output config including the default settings
		VegaliteConfigurator outputConfig = new VegaliteConfigurator(space, query);
		//
		// generate a default dataviz if x and y are not already sets
		if (view.getX()==null || view.getY()==null) {
			// DOMAIN
			if (config==null) {
				// not a bookmark, use default & specs if nothing provided
				// T1935: if explicit groupBy, use it
				boolean metricsDone = false;
				if (view.getGroupBy()!=null && view.getGroupBy().size()>0 && query.getGroupBy()!=null && query.getGroupBy().size()>0) {
					int next = 0;
					// reorder the dims to have the period first
					ArrayList<Integer> reorder = new ArrayList<>();
					for (int i=0;i<dims;i++) {
						String dim = query.getGroupBy().get(i);
						ExpressionAST expr = inputConfig.parse(dim);
						boolean isTemporal = expr.getImageDomain().isInstanceOf(IDomain.TEMPORAL);
						if (isTemporal) {
							reorder.add(0, i);// put it in front
						} else {
							reorder.add(i);
						}
					}
					while (next<dims) {
						if (next==1) {
							// insert the metrics after the first dimension
							metricsDone = handleMetrics(query, inputConfig, view, options, channels, true);
						}
						String dim = query.getGroupBy().get(reorder.get(next++));
						if (!inputConfig.getRequired().getGroupBy().contains(dim)) {
							ExpressionAST expr = inputConfig.parse(dim);
							boolean isTemporal = expr.getImageDomain().isInstanceOf(IDomain.TEMPORAL);
							if ((view.getX()==null || channels.encoding.x.type==DataType.quantitative) && view.getY()==null && !isTemporal) {// use Y only for categories and left a channel for the metrics
								// use it as the y
								view.setY(dim);
								channels.encoding.y = inputConfig.createChannelDef("y", view.getY(), options);
							} else if ((view.getY()==null || channels.encoding.y.type==DataType.quantitative) && view.getX()==null) {
								// use it as the x
								view.setX(dim);
								channels.encoding.x = inputConfig.createChannelDef("x", view.getX(), options);
							} else if (view.getColor()==null) {
								// use it as the color
								view.setColor(dim);
								channels.encoding.color = inputConfig.createChannelDef("color", view.getColor(), options);
							} else if (view.getColumn()==null) {
								// use it as the column
								view.setColumn(dim);
								channels.encoding.column = inputConfig.createChannelDef("column", view.getColumn(), options);
							} else if (view.getRow()==null) {
								// use it as the column
								view.setRow(dim);
								channels.encoding.row = inputConfig.createChannelDef("row", view.getRow(), options);
							} else {
								break;// no more channel available
							}
						}
					}
				} else {
					// add the period if nothing is selected
					if (view.getX()==null) {
						view.setX("daily(__PERIOD)");
						channels.encoding.x = inputConfig.createChannelDef("x", view.getX(), options);
					}
				}
				if (!metricsDone) {
					handleMetrics(query, inputConfig, view, options, channels, false);
				}
				// add the period if no group set
				if (view.getX()==null && !inputConfig.isTimeseries() && query.getPeriod()!=null) {
					view.setX("__PERIOD");
				}
				// TIME-SERIES
			} else if (config.getCurrentAnalysis()!=null && config.getCurrentAnalysis().equalsIgnoreCase(BookmarkConfig.TIMESERIES_ANALYSIS)) {
				// use the period as the x
				if (view.getX()==null && !inputConfig.isTimeseries()) {
					view.setX("__PERIOD");
				}
				// take care of y axis and metrics
				if (view.getY()==null && (!inputConfig.isHasMetric() || !inputConfig.isHasMetricValue())) {
					if (kpis==0 && !inputConfig.isHasMetric()) {
						// we need a default metric
						view.setY("count() // this is the default metric");
					} else if (kpis==1 && !inputConfig.isHasMetric()) {
						// we can only use the first one for now
						view.setY(query.getMetrics().get(0));
					} else if (!inputConfig.isHasMetricValue()) {
						view.setY(TransposeConverter.METRIC_VALUE_COLUMN);
						if (!inputConfig.isHasMetricSeries()) {
							if (view.getColor()==null) {
								view.setColor(TransposeConverter.METRIC_SERIES_COLUMN);
							} else if (view.getColumn()==null) {
								view.setColumn(TransposeConverter.METRIC_SERIES_COLUMN);
							} else if (view.getRow()==null) {
								view.setRow(TransposeConverter.METRIC_SERIES_COLUMN);
							}
						}
					}
				}
				// add reminding groupBy
				int next = 0;
				while (next<dims) {
					String groupBy = query.getGroupBy().get(next++);
					if (!groupBy.equals("__PERIOD") && !groupBy.equals(query.getPeriod()) && !inputConfig.getRequired().getGroupBy().contains(groupBy)) {
						if (view.getColor()==null) {
							// use it as the color
							view.setColor(groupBy);
						} else if (view.getColumn()==null) {
							// use it as the column
							view.setColumn(groupBy);
						} else if (view.getRow()==null) {
							// use it as the column
							view.setRow(groupBy);
						} else {
							break;// no more channel available
						}
					}
				}
				// BARCHART
			} else if (config.getCurrentAnalysis()!=null && config.getCurrentAnalysis().equalsIgnoreCase(BookmarkConfig.BARCHART_ANALYSIS)) {
				if (view.getY()==null && (!inputConfig.isHasMetric() || !inputConfig.isHasMetricValue())) {
					if (kpis==0 && !inputConfig.isHasMetric()) {
						// we need a default metric
						view.setY("count() // this is the default metric");
					} else if (kpis==1 && !inputConfig.isHasMetric()) {
						// we can only use the first one for now
						view.setY(query.getMetrics().get(0));
					} else if (!inputConfig.isHasMetricValue()) {
						view.setY(TransposeConverter.METRIC_VALUE_COLUMN);
						if (!inputConfig.isHasMetricSeries()) {
							if (view.getColor()==null) {
								view.setColor(TransposeConverter.METRIC_SERIES_COLUMN);
							} else if (view.getColumn()==null) {
								view.setColumn(TransposeConverter.METRIC_SERIES_COLUMN);
							} else if (view.getRow()==null) {
								view.setRow(TransposeConverter.METRIC_SERIES_COLUMN);
							}
						}
					}
				}
				// add reminding groupBy
				int next = 0;
				while (next<dims) {
					if (!inputConfig.getRequired().getGroupBy().contains(query.getGroupBy().get(next))) {
						if (view.getX()==null) {
							// use it as the x
							view.setX(query.getGroupBy().get(next++));
						} else if (view.getColor()==null) {
							// use it as the color
							view.setColor(query.getGroupBy().get(next++));
						} else if (view.getColumn()==null) {
							// use it as the column
							view.setColumn(query.getGroupBy().get(next++));
						} else if (view.getRow()==null) {
							// use it as the column
							view.setRow(query.getGroupBy().get(next++));
						} else {
							break;// no more channel available
						}
					}
				}
			} else {// TABLE_ANALYSIS or unknown
				if (kpis==0) {
					// we need at least one dim
					query.setMetrics(Collections.singletonList("count() // this is the default metric"));
					kpis++;
				}
				if (dims==0) {
					// just display the metrics
					if (view.getX()==null && !inputConfig.isHasMetric() && !inputConfig.isHasMetricValue()) {
						if (kpis==1) {
							view.setX(query.getMetrics().get(0));
						} else {
							// multi-kpis
							view.setX(TransposeConverter.METRIC_VALUE_COLUMN);
							if (!inputConfig.isHasMetricSeries()) {
								view.setY(TransposeConverter.METRIC_SERIES_COLUMN);
							}
						}
					}
				} else if (kpis==1) {
					// display a barchart or timeseries
					if (view.getY()==null && !inputConfig.isHasMetric() && !inputConfig.isHasMetricValue()) {
						view.setY(query.getMetrics().get(0));
					}
					for (String next : query.getGroupBy()) {
						if (view.getX()==null) {
							if (!next.equals(query.getPeriod())) {
								// change the barchart orientation
								view.setX(view.getY());
								view.setY(next);
							} else {
								view.setX(next);
							}
						} else if (view.getColor()==null) {
							// use it as the column
							view.setColor(next);
						} else if (view.getColumn()==null) {
							// use it as the column
							view.setColumn(next);
						} else if (view.getRow()==null) {
							// use it as the column
							view.setRow(next);
						} else {
							break;// no more channel available
						}
					}
				} else {
					// multiple kpis
					if (view.getX()==null) {
						view.setX(query.getGroupBy().get(0));
					}
					if (view.getY()==null && !inputConfig.isHasMetric()) {
						if (!inputConfig.isHasMetricValue()) {
							view.setY(TransposeConverter.METRIC_VALUE_COLUMN);
						}
						if (!inputConfig.isHasMetricSeries()) {
							if (view.getColor()==null) {
								view.setColor(TransposeConverter.METRIC_SERIES_COLUMN);
							} else if (view.getColumn()==null) {
								view.setColumn(TransposeConverter.METRIC_SERIES_COLUMN);
							} else if (view.getRow()==null) {
								view.setRow(TransposeConverter.METRIC_SERIES_COLUMN);
							}
						}
					}
					int next = 1;
					while (next<dims) {
						if (view.getColumn()==null) {
							// use it as the column
							view.setColumn(query.getGroupBy().get(next++));
						} else if (view.getRow()==null) {
							// use it as the column
							view.setRow(query.getGroupBy().get(next++));
						} else {
							break;// no more channel available
						}
					}
				}
			}
		}
		// rollup is not supported
		if (query.getRollups()!=null) {
			query.setRollups(null);
		}
		//
		//
		VegaliteSpecs specs = new VegaliteSpecs();
		specs.encoding.x = outputConfig.createChannelDef("x", view.getX(), options);
		specs.encoding.y = outputConfig.createChannelDef("y", view.getY(), options);
		specs.encoding.color = outputConfig.createChannelDef("color", view.getColor(), options);
		specs.encoding.size = outputConfig.createChannelDef("size", view.getSize(), options);
		specs.encoding.column = outputConfig.createChannelDef("column", view.getColumn(), options);
		specs.encoding.row = outputConfig.createChannelDef("row", view.getRow(), options);
		//
		if (specs.encoding.x!=null && specs.encoding.y!=null) {
			if (specs.encoding.x.type==DataType.nominal && specs.encoding.y.type==DataType.quantitative) {
				// auto sort
				specs.encoding.x.sort = new Sort(specs.encoding.y.field, Operation.max, Order.descending);
			} else if (specs.encoding.y.type==DataType.nominal && specs.encoding.x.type==DataType.quantitative) {
				// auto sort
				specs.encoding.y.sort = new Sort(specs.encoding.x.field, Operation.max, Order.descending);
			}
		}
		//
		// force using required
		query.setGroupBy(outputConfig.getRequired().getGroupBy());
		query.setMetrics(outputConfig.getRequired().getMetrics());
		//
		// enforce the explicit limit
		if (explicitLimit==null) {// compute the default
			int validDimensions = dims;
			if (outputConfig.isTimeseries()) validDimensions--;
			if (outputConfig.isHasMetricSeries()) validDimensions--;// excluding the metrics series
			if (dims>0) {
				explicitLimit = 10L;// keep 10 for each dim
				for (int i = validDimensions-1; i>0; i--) explicitLimit = explicitLimit*10;// get the power
			}
		}
		if (explicitLimit!=null && // if time-series, there's not explicit limit
				(query.getLimit()==null || query.getLimit()>explicitLimit)) {
			query.setLimit(explicitLimit);
		}
		// beyond limit
		if (outputConfig.isTimeseries()) {
			query.setBeyondLimit(Collections.singletonList(Integer.toString(outputConfig.getTimeseriesPosition())));
			query.setMaxResults(null);
		} else {
			//
			// set limit of not defined
			if (query.getLimit()==null) {
				query.setLimit((long) 20);
			}
		}
		final int startIndex = query.getStartIndex()!=null?query.getStartIndex():0;
		final int maxResults = query.getMaxResults()!=null?query.getMaxResults():(query.getLimit()!=null?query.getLimit().intValue():100);
		// make sure we order by something
		if (query.getOrderBy()==null || query.getOrderBy().size()==0) {
			if (query.getMetrics().size()>0) {
				ExpressionAST m = outputConfig.parse(query.getMetrics().get(0));
				query.setOrderBy(Collections.singletonList("desc("+outputConfig.prettyPrint(m)+")"));
			} else {
				query.setOrderBy(Collections.singletonList("desc(count())"));
			}
		} else {
			// check orderBy
			if (query.getMetrics().size()>0) {
				ExpressionAST m = outputConfig.parse(query.getMetrics().get(0));
				boolean check = false;
				for (String orderBy : query.getOrderBy()) {
					ExpressionAST o = outputConfig.parse(orderBy);
					if (o.getImageDomain().isInstanceOf(DomainSort.DOMAIN) && o instanceof Operator) {
						// remove the first operator
						Operator op = (Operator)o;
						if (op.getArguments().size()==1
								&& (op.getOperatorDefinition().getExtendedID().equals(SortOperatorDefinition.ASC_ID)
										|| op.getOperatorDefinition().getExtendedID().equals(SortOperatorDefinition.DESC_ID)))
						{
							o = op.getArguments().get(0);
						}
					}
					if (o.equals(m)) {
						check = true;
					}
				}
				if (!check) {
					query.getOrderBy().add("desc("+outputConfig.prettyPrint(m)+")");
				}
			}
		}
		//
		// create the facet selection
		FacetSelection selection = createFacetSelection(space, query);
		final ProjectAnalysisJob job = createAnalysisJob(space, query, selection, OutputFormat.JSON);
		//
		// handling data
		ResultInfo info = null;
		if (data.equals("EMBEDED") || data.equals("EMBEDDED")) {
			DataMatrix matrix = compute(userContext, job, query.getMaxResults(), query.getStartIndex(), false);
			if (!outputConfig.isHasMetricSeries()) {
				specs.data = transformToVegaData(query, matrix, DataLayout.RECORDS);
			} else {
				specs.data = transformToVegaData(query, matrix, DataLayout.TRANSPOSE);
			}
			int end = startIndex+maxResults;
			if (end>matrix.getRows().size()) end=matrix.getRows().size();
			info = getAnalyticsResultInfo(end-startIndex, startIndex, matrix);
		} else if (data.equals("URL")) {
			if (preFetch) {// always prefetch if HTML
				// run the query
				Callable<DataMatrix> task = new Callable<DataMatrix>() {
					@Override
					public DataMatrix call() throws Exception {
						return compute(userContext, job, maxResults, startIndex, false);
					}
				};
				// execute the task, no need to wait for result
				Future<DataMatrix> future = ExecutionManager.INSTANCE.submit(userContext.getCustomerId(), task);
				if (computeInfo) {
					// in that case we want to wait for the result in order to get data info
					try {
						DataMatrix matrix = future.get();
						int end = startIndex+maxResults;
						if (end>matrix.getRows().size()) end=matrix.getRows().size();
						info = getAnalyticsResultInfo(end-startIndex, startIndex, matrix);
					} catch (ExecutionException e) {
						throwCauseException(e);
					}
				}
			}
			specs.data = new Data();
			{
				HashMap<String, Object> override = new HashMap<>();
				override.put(AnalyticsServiceConstants.COMPARETO_COMPUTE_GROWTH_PARAM, false);
				if (!outputConfig.isHasMetricSeries()) {
					specs.data.url = buildAnalyticsQueryURI(userContext, query, "RECORDS", "DATA", null/*default style*/, override).toString();
				} else {
					specs.data.url = buildAnalyticsQueryURI(userContext, query, "TRANSPOSE", "DATA", null/*default style*/, override).toString();
				}
			}
			specs.data.format = new Format();
			specs.data.format.type = FormatType.json;// lowercase only!
		} else {
			throw new APIException("undefined value for data parameter, must be EMBEDDED or URL");
		}
		// mark
		if (outputConfig.isTimeseries()) {
			specs.mark = Mark.line;
		} else {
			if (specs.encoding.x!=null && specs.encoding.x.type==DataType.quantitative && specs.encoding.y!=null && specs.encoding.y.type==DataType.quantitative) {
				// use ticks
				if (specs.encoding.size!=null) {
					specs.mark = Mark.circle;
				} else if (specs.encoding.x.bin || specs.encoding.y.bin) {
					specs.mark = Mark.bar;
				} else {
					specs.mark = Mark.point;
				}
			} else {
				specs.mark = Mark.bar;
			}
		}
		// options
		if (view.hasOptons()) {
			try {
				Properties properties = view.getOptionsAsProperties();
				// mark
				Object omark = properties.get("mark");
				if (omark!=null) {
					try {
						Mark mark = Mark.valueOf(omark.toString());
						specs.mark = mark;
					} catch (IllegalArgumentException e) {
						query.add(new Problem(Severity.WARNING,"options","invalid options parameter 'mark': possible values are: "+toString(Mark.values())));
					}
				}
				// mark-stacked
				Object omarkStacked = properties.get("mark.stacked");
				if (omarkStacked!=null) {
					try {
						Stacked stacked = Stacked.valueOf(omarkStacked.toString());
						specs.config.mark = new VegaliteSpecs.MarkConfig();
						specs.config.mark.stacked = stacked;
						if (stacked==Stacked.none) {
							specs.config.mark.opacity = 0.6;
						}
					} catch (IllegalArgumentException e) {
						query.add(new Problem(Severity.WARNING,"options","invalid options parameter 'mark-stacked': possible values are: "+toString(Stacked.values())));
					}
				}
			} catch (IOException e) {
				query.add(new Problem(Severity.WARNING,"options","invalid options definition: must be: property1:value1;property2:value2;... where properties can be: mark"));
			}
		}
		// size
		if (specs.encoding.row==null && specs.encoding.column==null) {
			specs.config.cell = new VegaliteSpecs.Cell(640,400);
		} else {
			specs.config.cell = new VegaliteSpecs.Cell(320,200);
		}
		//
		ViewReply reply = new ViewReply(space);
		// update the facet selection with actual values
		FacetSelection actual = computeFacetSelection(space, selection);
		reply.setSelection(convertToSelection(userContext, query, space, job, actual));
		reply.setQuery(query);
		reply.setResult(specs);
		//
		// add all information
		reply.setView(view);
		reply.setResultInfo(info);
		//
		return reply;
	}

	/**
	 * @param userContext
	 * @param query
	 * @param string
	 * @param string2
	 * @param object
	 * @param override
	 * @return
	 */
	protected URI buildAnalyticsQueryURI(AppContext userContext, AnalyticsQuery query, String data, String envelope, Style style, HashMap<String, Object> override) {
		throw new NotYetImplementedException("the only data format supported is EMBEDDED", true);
	}


	private boolean handleMetrics(AnalyticsQueryImpl query, VegaliteConfigurator inputConfig, ViewQuery view, Properties options, VegaliteSpecs channels, boolean hasMoreDimensions) throws ScopeException {
		// add the metrics after the first dimension has been set
		if (query.getMetrics()==null || query.getMetrics().size()==0) {
			if (!inputConfig.isHasMetric()) {
				// use count() for now
				if (view.getX()==null) {
					view.setX("count()");
					channels.encoding.x = inputConfig.createChannelDef("x", view.getX(), options);
				} else {
					view.setY("count()");
					channels.encoding.y = inputConfig.createChannelDef("y", view.getY(), options);
				}
			}
		} else {
			// display some metrics
			// - single metric
			if (query.getMetrics().size()==1) {
				if (!inputConfig.isHasMetric()) {
					if (query.getCompareTo()!=null && !query.getCompareTo().isEmpty() && !inputConfig.isHasMetricValue() && !inputConfig.isHasMetricValue()) {
						// handle it like a multi-variate
						if (view.getX()==null) {
							view.setX("__VALUE");
							channels.encoding.x = inputConfig.createChannelDef("x", view.getX(), options);
						} else if (view.getY()==null) {
							view.setY("__VALUE");
							channels.encoding.y = inputConfig.createChannelDef("y", view.getY(), options);
						}
						if (view.getY()==null) {
							view.setY("__METRICS");
							channels.encoding.y = inputConfig.createChannelDef("y", view.getY(), options);
						} else if (view.getColor()==null) {
							view.setColor("__METRICS");
							channels.encoding.color = inputConfig.createChannelDef("color", view.getColor(), options);
						} else if (view.getColumn()==null) {
							view.setColumn("__METRICS");
							channels.encoding.column = inputConfig.createChannelDef("column", view.getColumn(), options);
						} else if (view.getRow()==null) {
							view.setRow("__METRICS");
							channels.encoding.row = inputConfig.createChannelDef("row", view.getRow(), options);
						}
					} else {
						if (view.getX()==null) {
							view.setX(query.getMetrics().get(0));
							channels.encoding.x = inputConfig.createChannelDef("x", view.getX(), options);
						} else if (view.getY()==null) {
							view.setY(query.getMetrics().get(0));
							channels.encoding.y = inputConfig.createChannelDef("y", view.getY(), options);
						}
					}
				}
				// - multiple metrics
			} else if (view.getY()==null || view.getColor()==null || view.getColumn()==null || view.getRow()==null) {
				// set __VALUE
				if (!inputConfig.isHasMetricValue()) {
					if (view.getX()==null) {
						view.setX("__VALUE");
						channels.encoding.x = inputConfig.createChannelDef("x", view.getX(), options);
					} else {
						view.setY("__VALUE");
						channels.encoding.y = inputConfig.createChannelDef("y", view.getY(), options);
					}
				}
				// set __METRICS
				if (!inputConfig.isHasMetricSeries()) {
					if (view.getY()==null) {
						view.setY("__METRICS");
						channels.encoding.y = inputConfig.createChannelDef("y", view.getY(), options);
					} else if (view.getColor()==null && !hasMoreDimensions) {
						view.setColor("__METRICS");
						channels.encoding.color = inputConfig.createChannelDef("color", view.getColor(), options);
					} else if (view.getColumn()==null) {
						view.setColumn("__METRICS");
						channels.encoding.column = inputConfig.createChannelDef("column", view.getColumn(), options);
					} else if (view.getRow()==null) {
						view.setRow("__METRICS");
						channels.encoding.row = inputConfig.createChannelDef("row", view.getRow(), options);
					}
				}
			}
		}
		return true;
	}

	private Data transformToVegaData(AnalyticsQuery query, DataMatrix matrix, DataLayout format) {
		IDataMatrixConverter<Object[]> converter = getConverter(format);
		Data data = new Data();
		data.values = converter.convert(query, matrix);
		return data;
	}

	private String toString(Object[] array) {
		String result = null;
		for (Object x : array) {
			if (result==null) {
				result = x.toString();
			} else {
				result += ", "+x.toString();
			}
		}
		return result;
	}


	public BookmarkConfig createBookmarkConfig(Space space, AnalyticsQuery query) throws ScopeException {
		SpaceScope scope = new SpaceScope(space);
		BookmarkConfig config = new BookmarkConfig();
		// set the project ID
		config.setProject(space.getUniverse().getProject().getOid());
		// config use the Domain OID
		config.setDomain(space.getDomain().getOid());
		//config.setSelection();
		config.setLimit(query.getLimit());
		//
		if (query.getPeriod()!=null && !query.getPeriod().equals("")) {
			config.setPeriod(null);
		}
		//
		if (query.getGroupBy() != null) {
			List<String> chosenDimensions = new ArrayList<>();
			for (String facet : query.getGroupBy()) {
				// add the domain scope
				ExpressionAST expr = scope.parseExpression(facet);
				chosenDimensions.add(rewriteExpressionToGlobalScope(expr, space));
			}
			String[] toArray = new String[chosenDimensions.size()];
			config.setChosenDimensions(chosenDimensions.toArray(toArray));
		}
		if (query.getMetrics() != null) {
			List<String> choosenMetrics = new ArrayList<>();
			for (String facet : query.getMetrics()) {
				ExpressionAST expr = scope.parseExpression(facet);
				choosenMetrics.add(rewriteChoosenMetric(expr));
			}
			String[] toArray = new String[choosenMetrics.size()];
			config.setChosenMetrics(choosenMetrics.toArray(toArray));
		}
		//
		if (query.getOrderBy() != null) {
			config.setOrderBy(new ArrayList<OrderBy>());
			for (String orderBy : query.getOrderBy()) {
				ExpressionAST expr = scope.parseExpression(orderBy);
				Direction direction = getDirection(expr.getImageDomain());
				Nulls nulls = getNullsPosition(expr.getImageDomain()) ;
				expr = unwrapOrderByExpression(expr);
				OrderBy copy = new OrderBy(new Expression(rewriteExpressionToGlobalScope(expr, space)), direction, nulls);
				config.getOrderBy().add(copy);
			}
		}
		//
		if (query.getRollups() != null) {
			config.setRollups(parseRollups(query.getRollups()));
		}
		// add the selection
		FacetSelection selection = createFacetSelection(space, query);
		config.setSelection(selection);
		// period and compareTo
		if (query.getPeriod()!=null && !query.getPeriod().equals("")) {
			// add the period into the hashMap....
			HashMap<String, String> map = new HashMap<>();
			ExpressionAST expr = scope.parseExpression(query.getPeriod());
			map.put(config.getDomain(), rewriteExpressionToGlobalScope(expr, space));
			config.setPeriod(map);
		}
		//
		config.setCurrentAnalysis(BookmarkConfig.TABLE_ANALYSIS);
		return config;
	}

	private String rewriteChoosenMetric(ExpressionAST expr) {

		String pref ="";
		String alias="";
		boolean hasAlias= expr.getName() != null && !expr.getName().isEmpty();

		if(hasAlias){
			alias=" as '" + expr.getName() +"'";
		}
		if (!hasAlias){
			if (expr instanceof MeasureExpression) {
				MeasureExpression measure = (MeasureExpression)expr;
				if (measure.getMeasure().getMetric()!=null) {
					return pref + measure.getMeasure().getMetric().getOid();
				}
			}
		}
		// else
		String  exprGlobal = expr.prettyPrint(PrettyPrintOptions.ROBOT_GLOBAL);

		return pref + exprGlobal + alias;
	}

}
