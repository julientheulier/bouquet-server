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
package com.squid.kraken.v4.api.core.simpleanalysisjob;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.StreamingOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.domain.DomainNumericConstant;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.sort.DomainSort;
import com.squid.core.domain.sort.DomainSort.SortDirection;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.ExpressionLeaf;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.BaseServiceRest;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl.OutputCompression;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl.OutputFormat;
import com.squid.kraken.v4.api.core.projectanalysisjob.AnalysisJobServiceBaseImpl;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.SegmentManager;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.scope.SpaceExpression;
import com.squid.kraken.v4.core.analysis.scope.UniverseScope;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.reference.DomainReference;
import com.squid.kraken.v4.core.expression.scope.DomainExpressionScope;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.Analysis;
import com.squid.kraken.v4.model.Analysis.AnalysisFacet;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Expression;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.FacetExpression;
import com.squid.kraken.v4.model.FacetMemberString;
import com.squid.kraken.v4.model.FacetSelection;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectAnalysisJob;
import com.squid.kraken.v4.model.ProjectAnalysisJob.Direction;
import com.squid.kraken.v4.model.ProjectAnalysisJob.OrderBy;
import com.squid.kraken.v4.model.ProjectAnalysisJob.Position;
import com.squid.kraken.v4.model.ProjectAnalysisJob.RollUp;
import com.squid.kraken.v4.model.ProjectAnalysisJobPK;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.SimpleAnalysis;
import com.squid.kraken.v4.persistence.AppContext;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.Authorization;

@Produces({ MediaType.APPLICATION_JSON })
@Api(value = "analyses", hidden = true, authorizations = { @Authorization(value = "kraken_auth", type = "oauth2") })
public class SimpleAnalysisJobServiceRest extends BaseServiceRest {

	private static final Logger logger = LoggerFactory
			.getLogger(SimpleAnalysisJobServiceRest.class);

	private AnalysisJobServiceBaseImpl delegate = AnalysisJobServiceBaseImpl
			.getInstance();

	public SimpleAnalysisJobServiceRest(AppContext userContext) {
		super(userContext);
	}

	@GET
	@Path("/")
	@ApiOperation(value = "Compute an Analysis")
	public Response computeAnalysis(
			@PathParam("projectId") String projectId,
			@QueryParam("domain") String domainExpr,
			@QueryParam("facet") String[] facetExpressions,
			@QueryParam("filter") String[] filterExpressions,
			@QueryParam("orderby") String[] orderExpressions,
			@QueryParam("rollup") String[] rollupExpressions,
			@QueryParam("limit") Long limit,
			@ApiParam(value = "response timeout in milliseconds in case the job is not yet computed. If no timeout set, the method will return according to current job status.") @QueryParam("timeout") Integer timeout,
			@ApiParam(value = "paging size") @QueryParam("maxResults") Integer maxResults,
			@ApiParam(value = "paging start index") @QueryParam("startIndex") Integer startIndex,
			@ApiParam(value = "if true, get the analysis only if already in cache", defaultValue = "false") @QueryParam("lazy") boolean lazy,
			@ApiParam(value = "output format", allowableValues = "json,csv,vxls", defaultValue = "json") @QueryParam("format") String format,
			@ApiParam(value = "output compression", allowableValues = "gzip, none, null", defaultValue = "none") @QueryParam("compression") String compression,
			@ApiParam(value = "output filename") @DefaultValue("/default") @QueryParam("filename") String filename
			)
			throws ScopeException {

		Analysis analysis = new SimpleAnalysis();
		analysis.setDomain(domainExpr);
		if (facetExpressions != null) {
			List<AnalysisFacet> facets = new ArrayList<AnalysisFacet>();
			for (int i = 0; i < facetExpressions.length; i++) {
				AnalysisFacet f = new SimpleAnalysis.SimpleFacet();
				f.setExpression(facetExpressions[i]);// if the name is provided by the expression, we will get it latter when it's parsed
				facets.add(f);
			}
			analysis.setFacets(facets);
		}
		if (filterExpressions != null) {
			analysis.setFilters(Arrays.asList(filterExpressions));
		}
		if (orderExpressions != null) {
			List<OrderBy> orders = new ArrayList<OrderBy>();
			for (int i = 0; i < orderExpressions.length; i++) {
				OrderBy order = new OrderBy();
				order.setExpression(new Expression(orderExpressions[i]));
				orders.add(order);
			}
			analysis.setOrderBy(orders);
		}
		if (rollupExpressions != null) {
			List<RollUp> rollups = new ArrayList<RollUp>();
			int pos = 1;
			for (int i = 0; i < rollupExpressions.length; i++) {
				// ok, do it quick...
				RollUp rollup = new RollUp();
				String expr = rollupExpressions[i].toLowerCase();
				Position position = Position.FIRST;//default
				if (expr.startsWith("last(")) {
					position = Position.LAST;
				}
				expr = expr.replaceAll("", "");
				try {
					int index = Integer.parseInt(expr);
					// rollup can use -1 to compute grand-total
					if (index<-1 || index>=facetExpressions.length) {
						throw new ScopeException("invalid rollup expression at position "+pos+": the index specified ("+index+") is not defined");
					}
					rollup.setCol(index);
					rollup.setPosition(position);
				} catch (NumberFormatException e) {
					throw new ScopeException("invalid rollup expression at position "+pos+": must be a valid indexe N or the expression FIRST(N) or LAST(N) to set the rollup position");
				}
				rollups.add(rollup);
			}
			analysis.setRollups(rollups);
		}
		analysis.setLimit(limit);

		ProjectAnalysisJob analysisJob = createAnalysisJob(projectId, analysis,
				timeout, maxResults, startIndex, lazy, format, compression);

		// and run the job
		boolean saveAs=true;
		if (filename==null || filename.equals("")) {
			// user does not want to save as
			saveAs = false;
		} else if (filename.equals("/default")) {
			// default is to let us figure out the name
			filename = null;
		}
		return getResults(projectId, analysisJob, timeout, maxResults,
				startIndex, lazy, format, compression, saveAs, filename);
	}

	@POST
	@Path("/")
	@ApiOperation(value = "Compute an Analysis")
	public Response computeAnalysis(
			@PathParam("projectId") String projectId,
			@ApiParam(required = true) Analysis analysis,
			@ApiParam(value = "response timeout in milliseconds in case the job is not yet computed. If no timeout set, the method will return according to current job status.") @QueryParam("timeout") Integer timeout,
			@ApiParam(value = "paging size") @QueryParam("maxResults") Integer maxResults,
			@ApiParam(value = "paging start index") @QueryParam("startIndex") Integer startIndex,
			@ApiParam(value = "if true, get the analysis only if already in cache", defaultValue = "false") @QueryParam("lazy") boolean lazy,
			@ApiParam(value = "output format", allowableValues = "json,csv,vxls", defaultValue = "json") @QueryParam("format") String format,
			@ApiParam(value = "output compression", allowableValues = "gzip, none, null", defaultValue = "none") @QueryParam("compression") String compression,
			@ApiParam(value = "output filename") @DefaultValue("/default") @QueryParam("filename") String filename
			)
			throws ScopeException {
		ProjectAnalysisJob analysisJob = createAnalysisJob(projectId, analysis,
				timeout, maxResults, startIndex, lazy, format, compression);

		// and run the job
		boolean saveAs=true;
		if (filename==null || filename.equals("")) {
			// user does not want to save as
			saveAs = false;
		} else if (filename.equals("/default")) {
			// default is to let us figure out the name
			filename = null;
		}
		return getResults(projectId, analysisJob, timeout, maxResults,
				startIndex, lazy, format, compression, saveAs, filename);
	}

	private ProjectAnalysisJob createAnalysisJob(String projectId,
			Analysis analysis, Integer timeout, Integer maxResults,
			Integer startIndex, boolean lazy, String format, String compression)
			throws ScopeException {

		ProjectPK projectPK = new ProjectPK(userContext.getCustomerId(),
				projectId);
		Project project = ProjectManager.INSTANCE.getProject(userContext,
				projectPK);
		AccessRightsUtils.getInstance().checkRole(userContext, project,
				AccessRight.Role.READ);
		Universe universe = new Universe(userContext, project);
		// read the domain reference
		if (analysis.getDomain() == null) {
			throw new ScopeException(
					"incomplete specification, you must specify the data domain expression");
		}
		// -- using the universe scope for now; will change when merge with T821
		// to also support query
		UniverseScope scope = new UniverseScope(universe);
		ExpressionAST domainExpression = scope.parseExpression(analysis
				.getDomain());
		if (!(domainExpression instanceof SpaceExpression)) {
			throw new ScopeException(
					"invalid specification, the domain expression must resolve to a Space");
		}
		Space ref = ((SpaceExpression) domainExpression).getSpace();
		Domain domain = ref.getDomain();
		AccessRightsUtils.getInstance().checkRole(userContext, domain,
				AccessRight.Role.READ);
		// the rest of the ACL is delegated to the AnalysisJob
		Space root = universe.S(domain);

		// handle the columns
		List<Metric> metrics = new ArrayList<Metric>();
		List<FacetExpression> facets = new ArrayList<FacetExpression>();
		DomainExpressionScope domainScope = new DomainExpressionScope(universe,
				domain);
		int facetCount = 0;
		int legacyFacetCount = 0;// count how much real facets we have to translate indexes
		int legacyMetricCount = 0;
		HashMap<Integer, Integer> lookup = new HashMap<>();// convert simple indexes into analysisJob indexes
		HashSet<Integer> metricSet = new HashSet<>();// mark metrics
		for (AnalysisFacet facet : analysis.getFacets()) {
			ExpressionAST colExpression = domainScope.parseExpression(facet
					.getExpression());
			if (colExpression.getName()!=null) {
				if (facet.getName()!=null && !facet.equals(colExpression.getName())) {
					throw new ScopeException("the facet name is ambiguous: "+colExpression.getName()+"/"+facet.getName()+" for expresion: "+facet
							.getExpression());
				}
				// else
				facet.setName(colExpression.getName());
			}
			IDomain image = colExpression.getImageDomain();
			if (image.isInstanceOf(IDomain.AGGREGATE)) {
				// it's a metric, we need to relink with the domain
				if (!(colExpression instanceof ExpressionLeaf)) {
					// add parenthesis if it is not a simple expression so A+B => domain.(A+B)
					colExpression = ExpressionMaker.GROUP(colExpression);
				}
				// relink with the domain
				ExpressionAST relink = ExpressionMaker.COMPOSE(
						new DomainReference(universe, domain), colExpression);
				// now it can be transformed into a measure
				Measure m = universe.asMeasure(relink);
				if (m == null) {
					throw new ScopeException("cannot use expression='"
							+ facet.getExpression() + "'");
				}
				Metric metric = new Metric();
				metric.setExpression(new Expression(m.prettyPrint()));
				String name = facet.getName();
				if (name == null) {
					name = m.prettyPrint();
				}
				metric.setName(name);
				metrics.add(metric);
				//
				lookup.put(facetCount, legacyMetricCount++);
				metricSet.add(facetCount);
				facetCount++;
			} else {
				// it's a dimension
				Axis axis = root.getUniverse().asAxis(colExpression);
				if (axis == null) {
					throw new ScopeException("cannot use expression='"
							+ colExpression.prettyPrint() + "'");
				}
				ExpressionAST facetExp = ExpressionMaker.COMPOSE(
						new SpaceExpression(root), colExpression);
				String name = facet.getName();
				if (name == null) {
					name = formatName(axis.getDimension() != null ? axis
							.getName() : axis.getDefinitionSafe().prettyPrint());
				}
				facets.add(new FacetExpression(facetExp.prettyPrint(), name));
				//
				lookup.put(facetCount, legacyFacetCount++);
				facetCount++;
			}
		}

		// handle filters
		FacetSelection selection = new FacetSelection();
		for (String filter : analysis.getFilters()) {
			ExpressionAST filterExpr = domainScope.parseExpression(filter);
			if (!filterExpr.getImageDomain().isInstanceOf(IDomain.CONDITIONAL)) {
				throw new ScopeException("invalid filter, must be a condition");
			}
			Facet segment = SegmentManager.newSegmentFacet(domain);
			FacetMemberString openFilter = SegmentManager.newOpenFilter(
					filterExpr, filter);
			segment.getSelectedItems().add(openFilter);
			selection.getFacets().add(segment);
		}
		
		// handle orderBy
		List<OrderBy> orderBy = new ArrayList<>();
		int pos = 1;
		for (OrderBy order : analysis.getOrderBy()) {
			if (order.getExpression()!=null) {
				// let's try to parse it
				try {
					ExpressionAST expr = domainScope.parseExpression(order.getExpression().getValue());
					IDomain image = expr.getImageDomain();
					Direction direction = getDirection(image);
					if (direction!=null) {
						order.setDirection(direction);
					} else if (order.getDirection()==null) {
						// we need direction!
						throw new ScopeException("invalid orderBy expression at position "+pos+": this is not a sort expression, must use either ASC() or DESC() functions");
					}
					if (image.isInstanceOf(DomainNumericConstant.DOMAIN)) {
						// it is a reference to the facets
						DomainNumericConstant num = (DomainNumericConstant)image.getAdapter(DomainNumericConstant.class);
						int index = num.getValue().intValue();
						if (!lookup.containsKey(index)) {
							throw new ScopeException("invalid orderBy expression at position "+pos+": the index specified ("+index+") is out of bounds");
						}
						int legacy = lookup.get(index);
						if (metricSet.contains(index)) {
							legacy += legacyFacetCount;
						}
						orderBy.add(new OrderBy(legacy, direction));
					} else {
						// it's an expression
						orderBy.add(new OrderBy(order.getExpression(), direction));
					}
				} catch (ScopeException e) {
					throw new ScopeException("unable to parse orderBy expression at position "+pos+": "+e.getCause(),e);
				}
			}
			pos++;
		}
		
		// handle rollup - fix indexes
		pos = 1;
		for (RollUp rollup : analysis.getRollups()) {
			if (rollup.getCol()>-1) {// ignore grand-total
				// can't rollup on metric
				if (metricSet.contains(rollup.getCol())) {
					throw new ScopeException("invalid rollup expression at position "+pos+": the index specified ("+rollup.getCol()+") is not valid: cannot rollup on metric");
				}
				if (!lookup.containsKey(rollup.getCol())) {
					throw new ScopeException("invalid rollup expression at position "+pos+": the index specified ("+rollup.getCol()+") is out of bounds");
				}
				int legacy = lookup.get(rollup.getCol());
				rollup.setCol(legacy);
			}
		}
		
		// create
		ProjectAnalysisJobPK pk = new ProjectAnalysisJobPK(projectPK, null);
		ProjectAnalysisJob analysisJob = new ProjectAnalysisJob(pk);
		analysisJob.setDomains(Collections.singletonList(domain.getId()));
		analysisJob.setMetricList(metrics);
		analysisJob.setFacets(facets);
		analysisJob.setOrderBy(orderBy);
		analysisJob.setSelection(selection);
		analysisJob.setRollups(analysis.getRollups());
		analysisJob.setAutoRun(true);
		if (analysis.getLimit() == null) {
			int complexity = analysisJob.getFacets().size();
			if (complexity < 4) {
				analysisJob.setLimit((long)Math.pow(10, complexity + 1));
			} else {
				analysisJob.setLimit(100000L);
			}
		} else {
			analysisJob.setLimit(analysis.getLimit());
		}
		return analysisJob;
	}
	
	private Direction getDirection(IDomain domain) {
		if (domain.isInstanceOf(DomainSort.DOMAIN)) {
			DomainSort sort = (DomainSort)domain.getAdapter(DomainSort.class);
			if (sort!=null) {
				SortDirection direction = sort.getDirection();
				if (direction!=null) {
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
		return null;
	}

	private String formatName(String prettyPrint) {
		return prettyPrint.replaceAll("[(),.]", " ").trim()
				.replaceAll("[^ a-zA-Z_0-9]", "").replace(' ', '_');
	}

	private Response getResults(String projectId, final ProjectAnalysisJob job,
			final Integer timeout, final Integer maxResults,
			final Integer startIndex, final boolean lazy, String format,
			String compression, boolean saveAs, String fileName) {

		final OutputFormat outFormat;
		if (format == null) {
			outFormat = OutputFormat.JSON;
		} else {
			outFormat = OutputFormat.valueOf(format.toUpperCase());
		}

		final OutputCompression outCompression;
		if (compression == null) {
			outCompression = OutputCompression.NONE;
		} else {
			outCompression = OutputCompression.valueOf(compression
					.toUpperCase());
		}

		StreamingOutput stream = new StreamingOutput() {
			@Override
			public void write(OutputStream os) throws IOException,
					WebApplicationException {
				// pass the ouputStream to the delegate
				try {
					delegate.writeResults(os, userContext, job, 1000, timeout,
							true, maxResults, startIndex, lazy, outFormat,
							outCompression, null);
				} catch (InterruptedException e) {
					throw new IOException(e);
				}
			}
		};

		// build the response
		ResponseBuilder response;
		fileName = fileName==null?"job-" + job.getOid():fileName;
		String mediaType;
		switch (outFormat) {
		case CSV:
			mediaType = saveAs?"text/csv":"text";
			fileName += ".csv";
			break;
		case XLS:
			mediaType = "application/vnd.ms-excel";
			fileName += ".xls";
			break;
		default:
			mediaType = MediaType.APPLICATION_JSON_TYPE.toString();
			fileName += ".json";
		}

		switch (outCompression) {
		case GZIP:
			// note : setting "Content-Type:application/octet-stream" should
			// disable interceptor's GZIP compression.
			mediaType = MediaType.APPLICATION_OCTET_STREAM_TYPE.toString();
			fileName += ".gz";
			break;
		default:
			// NONE
		}

		response = Response.ok(stream);
		response.header("Content-Type", mediaType);
		if (saveAs
				&& ((outFormat != OutputFormat.JSON) || (outCompression != OutputCompression.NONE))) {
			logger.info("returnin results as " + mediaType + ", fileName : "
					+ fileName);
			response.header("Content-Disposition", "attachment; filename="
					+ fileName);
		}

		return response.build();

	}
}
