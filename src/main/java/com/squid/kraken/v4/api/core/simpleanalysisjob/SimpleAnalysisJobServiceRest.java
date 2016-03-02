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
import java.util.List;

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

import com.squid.core.domain.IDomain;
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
			@QueryParam("facetName") String[] facetNames,
			@QueryParam("filter") String[] filterExpressions,
			@QueryParam("limit") Long limit,
			@ApiParam(value = "response timeout in milliseconds in case the job is not yet computed. If no timeout set, the method will return according to current job status.") @QueryParam("timeout") Integer timeout,
			@ApiParam(value = "paging size") @QueryParam("maxResults") Integer maxResults,
			@ApiParam(value = "paging start index") @QueryParam("startIndex") Integer startIndex,
			@ApiParam(value = "if true, get the analysis only if already in cache", defaultValue = "false") @QueryParam("lazy") boolean lazy,
			@ApiParam(value = "output format", allowableValues = "json,csv,vxls", defaultValue = "json") @QueryParam("format") String format,
			@ApiParam(value = "output compression", allowableValues = "gzip, none, null", defaultValue = "none") @QueryParam("compression") String compression)
			throws ScopeException {

		Analysis analysis = new SimpleAnalysis();
		analysis.setDomain(domainExpr);
		if (facetExpressions != null) {
			List<AnalysisFacet> facets = new ArrayList<AnalysisFacet>();
			for (int i = 0; i < facetExpressions.length; i++) {
				AnalysisFacet f = new SimpleAnalysis.SimpleFacet();
				f.setExpression(facetExpressions[i]);
				if (facetNames.length > i) {
					f.setName(facetNames[i]);
				}
				facets.add(f);
			}
			analysis.setFacets(facets);
		}
		if (filterExpressions != null) {
			analysis.setFilters(Arrays.asList(filterExpressions));
		}
		analysis.setLimit(limit);

		ProjectAnalysisJob analysisJob = createAnalysisJob(projectId, analysis,
				timeout, maxResults, startIndex, lazy, format, compression);

		// and run the job
		return getResults(projectId, analysisJob, timeout, maxResults,
				startIndex, lazy, format, compression, true);
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
			@ApiParam(value = "output compression", allowableValues = "gzip, none, null", defaultValue = "none") @QueryParam("compression") String compression)
			throws ScopeException {
		ProjectAnalysisJob analysisJob = createAnalysisJob(projectId, analysis,
				timeout, maxResults, startIndex, lazy, format, compression);
		// and run the job
		return getResults(projectId, analysisJob, timeout, maxResults,
				startIndex, lazy, format, compression, true);
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
		for (AnalysisFacet facet : analysis.getFacets()) {
			ExpressionAST colExpression = domainScope.parseExpression(facet
					.getExpression());
			IDomain image = colExpression.getImageDomain();
			if (image.isInstanceOf(IDomain.AGGREGATE)) {
				// it's a metric
				if (!(colExpression instanceof ExpressionLeaf)) {
					// add parenthesis...
					colExpression = ExpressionMaker.GROUP(colExpression);
				}
				ExpressionAST relink = ExpressionMaker.COMPOSE(
						new DomainReference(universe, domain), colExpression);
				Measure m = universe.asMeasure(relink);
				if (m == null) {
					throw new ScopeException("cannot use expresion='"
							+ colExpression.prettyPrint() + "'");
				}
				Metric metric = new Metric();
				metric.setExpression(new Expression(m.prettyPrint()));
				String name = facet.getName();
				if (name == null) {
					name = m.prettyPrint();
				}
				metric.setName(name);
				metrics.add(metric);
			} else {
				// it's a dimension
				Axis axis = root.getUniverse().asAxis(colExpression);
				if (axis == null) {
					throw new ScopeException("cannot use expresion='"
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

		// create
		ProjectAnalysisJobPK pk = new ProjectAnalysisJobPK(projectPK, null);
		ProjectAnalysisJob analysisJob = new ProjectAnalysisJob(pk);
		analysisJob.setDomains(Collections.singletonList(domain.getId()));
		analysisJob.setMetricList(metrics);
		analysisJob.setFacets(facets);
		analysisJob.setSelection(selection);
		analysisJob.setAutoRun(true);
		if (analysis.getLimit() == null) {
			int complexity = analysisJob.getMetricList().size()
					+ analysisJob.getFacets().size();
			if (complexity < 4) {
				analysisJob.setLimit(10L ^ (complexity + 1));
			} else {
				analysisJob.setLimit(100000L);
			}
		} else {
			analysisJob.setLimit(analysis.getLimit());
		}

		return analysisJob;

	}

	private String formatName(String prettyPrint) {
		return prettyPrint.replaceAll("[(),.]", " ").trim()
				.replaceAll("[^ a-zA-Z_0-9]", "").replace(' ', '_');
	}

	private Response getResults(String projectId, final ProjectAnalysisJob job,
			final Integer timeout, final Integer maxResults,
			final Integer startIndex, final boolean lazy, String format,
			String compression, boolean setFileName) {

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
		String fileName = "job-" + job.getOid();
		String mediaType;
		switch (outFormat) {
		case CSV:
			mediaType = "text/csv";
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
		if (setFileName
				&& ((outFormat != OutputFormat.JSON) || (outCompression != OutputCompression.NONE))) {
			logger.info("returnin results as " + mediaType + ", fileName : "
					+ fileName);
			response.header("Content-Disposition", "attachment; filename="
					+ fileName);
		}

		return response.build();

	}
}
