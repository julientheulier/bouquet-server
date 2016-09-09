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
package com.squid.kraken.v4.api.core.projectanalysisjob;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.ExpressionRef;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.IOrderByPiece.ORDERING;
import com.squid.core.sql.render.RenderingException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.EngineUtils;
import com.squid.kraken.v4.api.core.InvalidCredentialsAPIException;
import com.squid.kraken.v4.api.core.JobComputer;
import com.squid.kraken.v4.api.core.JobStats;
import com.squid.kraken.v4.api.core.PerfDB;
import com.squid.kraken.v4.api.core.domain.DomainServiceBaseImpl;
import com.squid.kraken.v4.api.core.project.ProjectServiceBaseImpl;
import com.squid.kraken.v4.caching.NotInCacheException;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchy;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingService;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.model.DashboardAnalysis;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.model.GroupByAxis;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Property;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.visitor.ExtractReferences;
import com.squid.kraken.v4.export.ExportSourceWriter;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.DataTable;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.Expression;
import com.squid.kraken.v4.model.ExpressionObject;
import com.squid.kraken.v4.model.FacetExpression;
import com.squid.kraken.v4.model.FacetSelection;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.MetricPK;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectAnalysisJob;
import com.squid.kraken.v4.model.ProjectAnalysisJob.Direction;
import com.squid.kraken.v4.model.ProjectAnalysisJob.Index;
import com.squid.kraken.v4.model.ProjectAnalysisJob.OrderBy;
import com.squid.kraken.v4.model.ProjectAnalysisJob.RollUp;
import com.squid.kraken.v4.model.ProjectAnalysisJobPK;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.writers.ExportQueryWriter;

/**
 * Compute a ProjectAnalysisJob using the Engine.<br>
 * Enforces the following rules :
 * <ul>
 * <li>Job shall define either domains or metrics.
 * <li>If job defines domains, all the domains metrics will be computed.</li>
 * <li>If job defines dimensions, computation will use them as an axis.</li>
 * </ul>
 */
public class AnalysisJobComputer implements JobComputer<ProjectAnalysisJob, ProjectAnalysisJobPK, DataTable> {

	static final Logger logger = LoggerFactory.getLogger(AnalysisJobComputer.class);

	public static final AnalysisJobComputer INSTANCE = new AnalysisJobComputer();

	/**
	 * Preview Compute
	 * <ul>
	 * <li>Checks if an analysis is in Redis Cache <br>
	 * 
	 * <li>If yes, returns the results as a Datatable
	 * <li>else
	 * <ul>
	 * <li>if the lazy flag is set to true, throw a NotInCacheException
	 * (RuntimeException)
	 * <li>else compute the result from the database, and return the results as
	 * a Datatable
	 * </ul>
	 * </ul>
	 */

	@Override
	public DataTable compute(AppContext ctx, ProjectAnalysisJob job, Integer maxResults, Integer startIndex,
			boolean lazy) throws ComputingException, InterruptedException {
		// build the analysis
		long start = System.currentTimeMillis();

		logger.info("Starting preview compute for job " + job.getId());

		DashboardAnalysis analysis;
		try {
			analysis = buildDashboardAnalysis(ctx, job, lazy);
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
			PerfDB.INSTANCE.save(queryLog);
			DataTable res = datamatrix.toDataTable(ctx, maxResults, startIndex, false, job.getOptionKeys());
			logger.debug("Is result set in REDIS complete? " + res.getFullset());
			return res;
		}
	}

	/**
	 * <ul>
	 * Export compute
	 * <li>Checks if an analysis is in Redis Cache <br>
	 * <li>If yes, write it into outputStream using writer
	 * <li>else
	 * <ul>
	 * <li>if the lazy flag is set to true, throw a NotInCacheException
	 * (RuntimeException)
	 * <li>else write it into outputStream using writer
	 * </ul>
	 * </ul>
	 * returns a datatable containing the number of lines written
	 */

	public DataTable compute(AppContext ctx, ProjectAnalysisJob job, OutputStream outputStream,
			ExportSourceWriter writer, boolean lazy) throws ComputingException, InterruptedException {
		// build the analysis
		long start = System.currentTimeMillis();
		logger.info("Starting export compute for job " + job.getOid());
		DashboardAnalysis analysis;
		try {
			analysis = buildDashboardAnalysis(ctx, job, true);
		} catch (ScopeException e1) {
			throw new ComputingException(e1);
		}
		ExportQueryWriter eqw = new ExportQueryWriter(writer, outputStream, job.getId().getAnalysisJobId());
		ComputingService.INSTANCE.executeAnalysis(analysis, eqw, lazy);

		DataTable results = new DataTable();
		results.setTotalSize(eqw.getLinesWritten());

		long stop = System.currentTimeMillis();
		// logger.info("End of compute for job " +
		// job.getId().getAnalysisJobId().toString() + " in " +(stop-start)+
		// "ms" );
		logger.info("task=" + this.getClass().getName() + " method=compute" + " jobid="
				+ job.getOid() + " status=done duration=" + (stop - start));
		JobStats queryLog = new JobStats(job.getId().toString(), "FacetJobComputer", (stop - start),
				job.getId().getProjectId());
		PerfDB.INSTANCE.save(queryLog);

		return results;
	}

	public static List<Domain> readDomains(AppContext ctx, ProjectAnalysisJob job) throws ScopeException {
		List<Domain> domains = new ArrayList<Domain>();
		for (DomainPK domainId : job.getDomains()) {
			domains.add(ProjectManager.INSTANCE.getDomain(ctx, domainId));
		}
		return domains;
	}

	public String viewSQL(AppContext ctx, ProjectAnalysisJob job)
			throws ComputingException, InterruptedException, ScopeException, SQLScopeException, RenderingException {
		DashboardAnalysis analysis;
		try {
			analysis = buildDashboardAnalysis(ctx, job);
		} catch (ScopeException e1) {
			throw new ComputingException(e1);
		}
		// run the analysis
		return ComputingService.INSTANCE.viewSQL(analysis);
	}

	public static DashboardAnalysis buildDashboardAnalysis(AppContext ctx, ProjectAnalysisJob job)
			throws ScopeException, ComputingException, InterruptedException {
		return buildDashboardAnalysis(ctx, job, false);
	}

	public static DashboardAnalysis buildDashboardAnalysis(AppContext ctx, ProjectAnalysisJob job, boolean lazy)
			throws ScopeException, ComputingException, InterruptedException {
		logger.info("AnalysisJobComputer.buildDashboardAnalysis(): start " + job.getId().toString());
		ProjectServiceBaseImpl projService = ProjectServiceBaseImpl.getInstance();

		String customerId = job.getCustomerId();

		ProjectPK projectPK = new ProjectPK(customerId, job.getId().getProjectId());
		FacetSelection selection = job.getSelection();

		// get the project using a root context since JDBC settings may not be
		// visible to the user
		long start = System.currentTimeMillis();
		Project project = projService.read(ctx, projectPK, true);// use the user
		// ctx to
		// read the
		// project -
		// let the
		// query
		// worker
		// use the
		// root ctx
		// if needed
		// logger.info("Project deep-read (ms) : "
		// + (System.currentTimeMillis() - start));
		long duration = (System.currentTimeMillis() - start);
		logger.info(
				" jobid=" + job.getId() + " task=deepread " + " duration=" + duration);

		JobStats queryLog = new JobStats(job.getId().getAnalysisJobId(),
				"AnalysisJobComputer.ProjectDeepRead", duration, job.getId().getProjectId());
		queryLog.setError(false);
		PerfDB.INSTANCE.save(queryLog);

		Universe universe = new Universe(ctx, project);
		// define a dashboard
		DashboardAnalysis dash = new DashboardAnalysis(universe);

		dash.lazy(lazy);
		dash.setJobId(job.getOid());
		// setup the metrics
		List<Metric> metrics = job.getMetricList();

		// krkn-61: list the domains
		List<Domain> domains = new ArrayList<Domain>();

		if (metrics.isEmpty()) {
			// ticket:2905 if metric is empty, do not run group-by analyze but
			// just a simple select
			domains = readDomains(ctx, job);
			if (domains.size() != 1) {
				long stop = System.currentTimeMillis();
				/*
				 * logger.info("AnalysisJobComputer.buildDashboardAnalysis() " +
				 * job.getId().toString() + " ended in " + (stop - start) +
				 * "ms with error" +
				 * "if no kpi is defined, must have one single domain");
				 */
				PerfDB.logPerf(logger, job, "AnalysisJobComputer.buildDashboardAnalysis()", true, (stop - start),
						"if no kpi is defined, must have one single domain");

				throw new ComputingException("if no kpi is defined, must have one single domain");
			}
			Space space = universe.S(domains.get(0));
			dash.setMainDomain(space);
		} else {
			// build the metrics set from the analysis job's
			for (Metric metricData : metrics) {
				MetricPK metricId = metricData.getId();
				if (metricId != null) {
					// define a measure
					DomainPK domainPk = new DomainPK(metricId.getCustomerId(), metricId.getProjectId(),
							metricId.getDomainId());
					Domain domain = ProjectManager.INSTANCE.getDomain(ctx, domainPk);
					// Metric is referenced by id
					DomainHierarchy hierarchy = universe.getDomainHierarchy(domain, true);
					Metric metric = hierarchy.getMetric(ctx, metricId.getMetricId());
					if (metric == null)
						throw new ScopeException("cannot lockup metric ID=" + metricId.getMetricId());
					//
					domains.add(domain);// krkn-61: add the direct parent
					Space space = universe.S(domain);
					Measure measure = space.M(metric);
					// override the name using the analysis definition
					if (metricData.getName() != null) {
						measure.withName(metricData.getName());
					} else if (metricData.getLName() != null) {
						measure.withName(metricData.getLName());
					}
					// define a kpi
					dash.add(measure);
				} else {
					// Metric is just an expression
					Expression expr = metricData.getExpression();
					Measure measure = universe.measure(expr.getValue());
					if (metricData.getName() != null) {
						measure.withName(metricData.getName());
					} else if (metricData.getLName() != null) {
						measure.withName(metricData.getLName());
					}
					// add only the root domain
					domains.add(measure.getParent().getRoot());
					// define a kpi
					dash.add(measure);
				}
			}
		}

		// krkn-61: we need a list of domains before intializing the selection
		// setup the selection
		DashboardSelection ds = EngineUtils.getInstance().applyFacetSelection(ctx, universe, domains, selection);
		dash.setSelection(ds);

		// define the axes
		// -- legacy support
		for (Dimension dim : job.readDimensions(ctx)) {
			Domain dom = DomainServiceBaseImpl.getInstance().read(ctx,
					new DomainPK(project.getId(), dim.getId().getDomainId()), true);
			Space axisSpace = universe.S(dom.getName());
			Axis axis = axisSpace.A(dim);
			dash.add(axis);
		}
		// -- pivot support (V2)
		{
			int pos = 0;
			for (FacetExpression expr : job.getFacets()) {
				if (expr.getValue() != null) {
					try {
						Axis axis = readAxis(ctx, universe, expr);
						if (expr.getName() != null) {
							axis.setName(expr.getName());
						}
						dash.add(axis);
					} catch (ScopeException e) {

						long stop = System.currentTimeMillis();
						PerfDB.logPerf(logger, job, "AnalysisJobComputer.buildDashboardAnalysis()", true, stop - start,
								"invalid pivot expression" + expr.getValue() + "at position #" + pos + ": "
										+ e.getMessage());

						throw new ScopeException("invalid pivot expression '" + expr.getValue() + "' at position #"
								+ pos + ": " + e.getMessage());
					}
				} else {
					long stop = System.currentTimeMillis();
					PerfDB.logPerf(logger, job, "AnalysisJobComputer.buildDashboardAnalysis()", true, stop - start,
							"undefined pivot expression at position #" + pos);

					throw new ScopeException("undefined pivot expression (null) at position #" + pos);
				}
			}
		}
		// handles roll-up
		if (job.getRollups() != null && !job.getRollups().isEmpty()) {
			for (RollUp rollup : job.getRollups()) {
				int index = rollup.getCol();
				if (index == -1) {
					// add a grandTotal
					dash.setRollupGrandTotal(true);
				} else if (index >= 0 && index < dash.getGrouping().size()) {
					GroupByAxis axis = dash.getGrouping().get(index);
					if (axis.isRollup()) {
						long stop = System.currentTimeMillis();
						PerfDB.logPerf(logger, job, "AnalysisJobComputer.buildDashboardAnalysis()", true, stop - start,
								"invalid ROLLUP hierarchy " + axis.getAxis().prettyPrint() + " appears twice or more");

						throw new ComputingException("invalid ROLLUP hierarchy, '" + axis.getAxis().prettyPrint()
								+ "' appears twice or more");
					}
					dash.rollup(axis, rollup.getPosition());
				} else {
					long stop = System.currentTimeMillis();
					PerfDB.logPerf(logger, job, "AnalysisJobComputer.buildDashboardAnalysis()", true, stop - start,
							"invalid ROLLUP index  " + index + ": nothing to match");
					throw new ComputingException("invalid ROLLUP index = " + index + ": nothing to match");
				}
			}
		}
		// handles order by
		if (job.getOrderBy() != null && !job.getOrderBy().isEmpty()) {
			List<Property> properties = new ArrayList<Property>();
			for (GroupByAxis item : dash.getGrouping()) {
				properties.add(item.getAxis());
			}
			properties.addAll(dash.getKpis());
			int pos = 0;
			for (OrderBy orderby : job.getOrderBy()) {
				if (orderby.getExpression() != null) {
					// orderBy is defined by an expression
					Expression expr = orderby.getExpression();
					if (expr.getValue() != null) {
						try {

							String val = expr.getValue();
							// T1699
							if (val.startsWith("growth(") && val.endsWith(")")) {
								val = val.substring(7, val.length() - 1);
								ExpressionAST value = universe.expression(val);
								dash.orderByGrowth(value, getOrderByDirection(orderby.getDirection()), expr);
							} else {
								if (val.startsWith("compareTo(") && val.endsWith(")")) {
									val = val.substring(10, val.length() - 1);
									ExpressionAST value = universe.expression(val);
									dash.orderByGrowth(value, getOrderByDirection(orderby.getDirection()), expr);
								} else {
									ExpressionAST value = universe.expression(val);
									dash.orderBy(value, getOrderByDirection(orderby.getDirection()));
								}
							}
						} catch (ScopeException e) {

							long stop = System.currentTimeMillis();
							PerfDB.logPerf(logger, job, "AnalysisJobComputer.buildDashboardAnalysis()", true,
									stop - start, "invalid orderBy expression " + expr.getValue() + " at position #"
											+ pos + ": " + e.getMessage());
							throw new ScopeException("invalid orderBy expression '" + expr.getValue()
									+ "' at position #" + pos + ": " + e.getMessage());
						}
					} else {
						long stop = System.currentTimeMillis();
						PerfDB.logPerf(logger, job, "AnalysisJobComputer.buildDashboardAnalysis()", true, stop - start,
								"undefined orderBy expression at position #" + pos);
						throw new ScopeException("undefined orderBy expression (null) at position #" + pos);
					}
				} else if (orderby.getCol() != null) {
					int index = orderby.getCol();
					if (index >= 0 && index < properties.size()) {
						dash.orderBy(properties.get(index).getReference(), getOrderByDirection(orderby.getDirection()));
					} else if (index < 0) {
						// ignore
					} else {
						long stop = System.currentTimeMillis();
						PerfDB.logPerf(logger, job, "AnalysisJobComputer.buildDashboardAnalysis()", true, stop - start,
								"invalid ORDER BY index  " + index + ": nothing to match");
						throw new ComputingException("invalid ORDER BY index = " + index + ": nothing to match");
					}
				} else {
					long stop = System.currentTimeMillis();
					PerfDB.logPerf(logger, job, "AnalysisJobComputer.buildDashboardAnalysis()", true, stop - start,
							"invalid ORDER BY definition at position #" + pos);
					throw new ComputingException("invalid ORDER BY definition, must use index or expression value");
				}
				pos++;
			}
		}

		// handles limit & offset
		if (job.getLimit() != null) {
			dash.limit(job.getLimit());
			if (job.getOffset() != null) {
				dash.offset(job.getOffset());
			}
		}

		// handles noLimit (T1026)
		if (job.getLimit() != null && job.getBeyondLimit() != null && !job.getBeyondLimit().isEmpty()) {
			for (Index index : job.getBeyondLimit()) {
				int col = index.getCol();
				if (col >= 0 && col < dash.getGrouping().size()) {
					GroupByAxis axis = dash.getGrouping().get(col);
					dash.beyondLimit(axis);
				} else {
					// throw new ScopeException("invalid beyondLimit column
					// index ("+col+"): it must reference an valid axis");
				}
			}
		}

		// handles option keys
		if (job.getOptionKeys() != null) {
			dash.setOptionKeys(job.getOptionKeys());
		}

		// check
		if (dash.getGrouping().isEmpty() && dash.getGroups().isEmpty()) {
			long stop = System.currentTimeMillis();
			PerfDB.logPerf(logger, job, "AnalysisJobComputer.buildDashboardAnalysis()", true, (stop - start),
					"Invalid Analysis: select at least one dimension or metric");

			throw new ComputingException("Invalid Analysis: select at least one dimension or metric");
		}
		long stop = System.currentTimeMillis();
		/*
		 * logger.info("AnalysisJobComputer.buildDashboardAnalysis(): end " +
		 * job.getId().toString() + " in " + (stop - start) + "ms");
		 */
		logger.info("task=AnalysisJobComputer" + " method=AnalysisJobComputer.buildDashboardAnalysis()" + " jobid="
				+ job.getId().getAnalysisJobId() + " duration=" + (stop - start) + " error=false end");
		queryLog = new JobStats(job.getId().getAnalysisJobId(), "AnalysisJobComputer.buildDashboardAnalysis",
				(stop - start), job.getId().getProjectId());
		queryLog.setError(false);
		PerfDB.INSTANCE.save(queryLog);

		return dash;
	}

	private static Axis readAxis(AppContext ctx, Universe universe, Expression expr)
			throws ScopeException, ComputingException, InterruptedException {
		Axis axis = EngineUtils.getInstance().getFacetAxis(ctx, universe, expr.getValue());// universe.axis(expr.getValue());
		// check user ACL
		DimensionIndex index = axis.getIndex();
		if (index == null) {
			// throw new ScopeException("Dimension is not defined");
			ExtractReferences extract = new ExtractReferences();
			List<ExpressionRef> refs = extract.apply(axis.getDefinition());
			if (refs.isEmpty()) {
				// something wrong here, we should not get there
				throw new InvalidCredentialsAPIException("Unable to validate privileges for expression: " + expr,
						ctx.isNoError());
			} else {
				for (ExpressionRef ref : refs) {
					Object xxx = ref.getReference();
					if (xxx instanceof Property) {
						Property prop = (Property) xxx;
						AccessRightsUtils.getInstance().checkRole(ctx, prop.getExpressionObject(), Role.READ);
					} else if (xxx instanceof ExpressionObject<?>) {
						ExpressionObject<?> model = (ExpressionObject<?>) xxx;
						AccessRightsUtils.getInstance().checkRole(ctx, model, Role.READ);
					}
				}
			}
		} else {
			AccessRightsUtils.getInstance().checkRole(ctx, index.getDimension(), Role.READ);
		}
		return axis;
	}

	private static ORDERING getOrderByDirection(Direction direction) {
		if (direction == Direction.ASC) {
			return ORDERING.ASCENT;
		} else {
			return ORDERING.DESCENT;
		}
	}

}
