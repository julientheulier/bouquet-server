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
package com.squid.kraken.v4.api.core.bb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.concurrent.TimeoutException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.squid.core.domain.DomainNumericConstant;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.sort.DomainSort;
import com.squid.core.domain.sort.DomainSort.SortDirection;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.ExpressionLeaf;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.ComputingInProgressAPIException;
import com.squid.kraken.v4.api.core.EngineUtils;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl.OutputFormat;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.api.core.bookmark.BookmarkFolderServiceBaseImpl;
import com.squid.kraken.v4.api.core.bookmark.BookmarkServiceBaseImpl;
import com.squid.kraken.v4.api.core.customer.CoreAuthenticatedServiceRest;
import com.squid.kraken.v4.api.core.domain.DomainServiceBaseImpl;
import com.squid.kraken.v4.api.core.metric.MetricServiceBaseImpl;
import com.squid.kraken.v4.api.core.project.ProjectServiceBaseImpl;
import com.squid.kraken.v4.api.core.projectanalysisjob.AnalysisJobComputer;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchy;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.SegmentManager;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingService;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.scope.AxisExpression;
import com.squid.kraken.v4.core.analysis.scope.MeasureExpression;
import com.squid.kraken.v4.core.analysis.scope.SpaceExpression;
import com.squid.kraken.v4.core.analysis.scope.SpaceScope;
import com.squid.kraken.v4.core.analysis.scope.UniverseScope;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.reference.DomainReference;
import com.squid.kraken.v4.core.expression.scope.DomainExpressionScope;
import com.squid.kraken.v4.core.expression.scope.ExpressionSuggestionHandler;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.AnalysisQuery;
import com.squid.kraken.v4.model.AnalysisQuery.AnalysisFacet;
import com.squid.kraken.v4.model.AnalysisQueryImpl;
import com.squid.kraken.v4.model.AnalysisResult;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.BookmarkConfig;
import com.squid.kraken.v4.model.BookmarkFolder;
import com.squid.kraken.v4.model.BookmarkPK;
import com.squid.kraken.v4.model.DataTable;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.Expression;
import com.squid.kraken.v4.model.ExpressionSuggestion;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.FacetExpression;
import com.squid.kraken.v4.model.FacetMemberString;
import com.squid.kraken.v4.model.FacetSelection;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.MetricExt;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectAnalysisJob;
import com.squid.kraken.v4.model.ProjectAnalysisJob.Direction;
import com.squid.kraken.v4.model.ProjectAnalysisJob.OrderBy;
import com.squid.kraken.v4.model.ProjectAnalysisJob.Position;
import com.squid.kraken.v4.model.ProjectAnalysisJob.RollUp;
import com.squid.kraken.v4.model.ProjectAnalysisJobPK;
import com.squid.kraken.v4.model.ProjectFacetJob;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.ValueType;
import com.squid.kraken.v4.persistence.AppContext;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.Authorization;
import com.wordnik.swagger.annotations.AuthorizationScope;

/**
 * Support for the new /bookmark-analysis API endpoint that provides virtual model and data access based on the bookmark definition
 * 
 * 
 * 
 * @author sergefantino
 *
 */
@Path("/bb")
@Api(value = "bookmark-analysis", hidden = false, authorizations = { @Authorization(value = "kraken_auth", type = "oauth2", scopes = { @AuthorizationScope(scope = "access", description = "Access") }) })
@Produces({ MediaType.APPLICATION_JSON })
public class BookmarkAnalysisServiceRest  extends CoreAuthenticatedServiceRest {

	static final Logger logger = LoggerFactory.getLogger(BookmarkAnalysisServiceRest.class);

	private final static String BBID_PARAM_NAME = "BBID";
	private final static String FACETID_PARAM_NAME = "FACETID";

	private BookmarkServiceBaseImpl delegate = BookmarkServiceBaseImpl
			.getInstance();
	
	public BookmarkAnalysisServiceRest() {
	}

	@GET
	@Path("")
	@ApiOperation(
			value = "List available bookmarks",
			notes = "for now it only lists the content of My Bookmark"	)
	public BookmarkFolder listBB(@Context HttpServletRequest request) {
		AppContext userContext = getUserContext(request);
		return BookmarkFolderServiceBaseImpl.getInstance().read(userContext, null);
	}
	
	@GET
	@Path("{" + BBID_PARAM_NAME + "}")
	@ApiOperation(value = "Get a bookmark")
	public Bookmark getBB(@Context HttpServletRequest request, @PathParam(BBID_PARAM_NAME) String BBID) {
		AppContext userContext = getUserContext(request);
		Bookmark bookmark = getBookmark(userContext, BBID);
		// make sure user can read the project
		ProjectPK projectPK = bookmark.getId().getParent();
		ProjectServiceBaseImpl.getInstance().read(userContext, projectPK, false);
		return bookmark;
	}
	
	@GET
	@Path("{" + BBID_PARAM_NAME + "}/domain")
	@ApiOperation(value = "Get the bookmark's domain")
	public Domain getDomain(@Context HttpServletRequest request, @PathParam(BBID_PARAM_NAME) String BBID) {
		AppContext userContext = getUserContext(request);
		Bookmark bookmark = getBookmark(userContext, BBID);
		BookmarkConfig config = readConfig(bookmark);
		String domainId = config.getDomain();
		DomainPK domainPk = new DomainPK(bookmark.getId().getParent(), domainId);
		Domain domain = DomainServiceBaseImpl.getInstance().read(userContext, domainPk);
		return domain;
	}
	
	/*
	@GET
	@Path("{" + PARAM_NAME + "}/dimensions")
	@ApiOperation(
			value = "Gets the bookmark's dimensions", 
			notes = "This is only usefull in case we want to provide editing capabilities from the bookmark. But it can be ambiguous how to use it in conjonction with the /facets operation",
			response = Dimension.class)
	public List<Dimension> getDimensions(@Context HttpServletRequest request, @PathParam(PARAM_NAME) String BBID) {
		AppContext userContext = getUserContext(request);
		Bookmark bookmark = getBookmark(userContext, BBID);
		BookmarkConfig config = readConfig(bookmark);
		String domainId = config.getDomain();
		DomainPK domainPk = new DomainPK(bookmark.getId().getParent(), domainId);
		List<Dimension> dimensions = DimensionServiceBaseImpl.getInstance().readAll(userContext, domainPk);
		return dimensions;
	}
	*/
	
	@GET
	@Path("{" + BBID_PARAM_NAME + "}/metrics")
	@ApiOperation(value = "Get the bookmark's metrics", response = MetricExt.class)
	public List<MetricExt> getMetrics(@Context HttpServletRequest request, @PathParam(BBID_PARAM_NAME) String BBID) {
		AppContext userContext = getUserContext(request);
		Bookmark bookmark = getBookmark(userContext, BBID);
		BookmarkConfig config = readConfig(bookmark);
		String domainId = config.getDomain();
		DomainPK domainPk = new DomainPK(bookmark.getId().getParent(), domainId);
		List<MetricExt> metrics = MetricServiceBaseImpl.getInstance().readAll(userContext, domainPk);
		return metrics;
	}
	
	@GET
	@Path("{" + BBID_PARAM_NAME + "}/facets")
	@ApiOperation(value = "Get the bookmark's facets using the default BB selection")
	public FacetSelection getFacets(
			@Context HttpServletRequest request, 
			@PathParam(BBID_PARAM_NAME) String BBID
		) throws ComputingException {
		AppContext userContext = getUserContext(request);
		return runFacets(userContext, BBID, null);
	}
	
	@POST
	@Path("{" + BBID_PARAM_NAME + "}/facets")
	@ApiOperation(value = "Get the bookmark's facets using a custom selection")
	public FacetSelection postFacets(
			@Context HttpServletRequest request, 
			@PathParam(BBID_PARAM_NAME) String BBID,
			@ApiParam(value="the selection", required=true) FacetSelection selection
		) throws ComputingException {
		AppContext userContext = getUserContext(request);
		return runFacets(userContext, BBID, selection);
	}
	
	public FacetSelection runFacets(
			AppContext userContext, 
			@PathParam(BBID_PARAM_NAME) String BBID,
			FacetSelection selection
		) throws ComputingException {
		Bookmark bookmark = getBookmark(userContext, BBID);
		BookmarkConfig config = readConfig(bookmark);
		String domainId = config.getDomain();
		DomainPK domainPk = new DomainPK(bookmark.getId().getParent(), domainId);
		//
		try {
			Domain domain = ProjectManager.INSTANCE.getDomain(userContext, domainPk);
			ProjectFacetJob job = new ProjectFacetJob();
			job.setDomain(Collections.singletonList(domainPk));
			job.setCustomerId(userContext.getCustomerId());
			if (selection!=null) {
				selection = config.getSelection();
			}
			job.setSelection(selection);
			List<Facet> result = new ArrayList<>();
			ProjectPK projectPK = bookmark.getId().getParent();
			Project project = ProjectServiceBaseImpl.getInstance().read(userContext, projectPK, false);
			Universe universe = new Universe(userContext, project);
			DashboardSelection ds;
			List<Domain> domains = Collections.singletonList(domain);
			ds = EngineUtils.getInstance().applyFacetSelection(userContext,
					universe, domains, selection);
			result.addAll(ComputingService.INSTANCE.glitterFacets(universe,
					domain, ds));
			FacetSelection facetSelectionResult = new FacetSelection();
			facetSelectionResult.setFacets(result);
			// handling compareTo (T947)
			if (ds.hasCompareToSelection()) {
				// create a fresh seelction with the compareTo
				DashboardSelection compareDS = new DashboardSelection();
				Domain domain2 = ds.getCompareToSelection().getDomain();
				compareDS.add(ds.getCompareToSelection());
				ArrayList<Facet> facets = new ArrayList<>();
				for (Axis filter : ds.getCompareToSelection().getFilters()) {
					facets.add(ComputingService.INSTANCE.glitterFacet(universe, domain2, compareDS, filter, null, 0, 100, null));
				}
				facetSelectionResult.setCompareTo(facets);
			}
			return facetSelectionResult;
	
		} catch (ScopeException | ComputingException | InterruptedException e) {
			throw new ComputingException(e.getLocalizedMessage(), e);
		} catch (TimeoutException e) {
			throw new ComputingInProgressAPIException(null,
					userContext.isNoError(), null);
		}
	}
	
	@GET
	@Path("{" + BBID_PARAM_NAME + "}/expression")
	@ApiOperation(value = "Evaluate an expression in the bookmark context and return detailled information")
	public ExpressionSuggestion evaluateExpression(
			@Context HttpServletRequest request, 
			@PathParam(BBID_PARAM_NAME) String BBID,
			@ApiParam(value="the expression to check, or null in order to get scope level suggestions") @QueryParam("value") String expression,
			@ApiParam(value="optionnal caret position in the expression value in order to provide relevant suggestions") @QueryParam("offset") Integer offset,
			@ApiParam(value="optional type to filter the suggestions") @QueryParam("type") ValueType suggestionType
			) throws ScopeException
	{
		if (expression==null) expression="";
		AppContext userContext = getUserContext(request);
		Bookmark bookmark = getBookmark(userContext, BBID);
		BookmarkConfig config = readConfig(bookmark);
		String domainId = config.getDomain();
		DomainPK domainPk = new DomainPK(bookmark.getId().getParent(), domainId);
		Domain domain = ProjectManager.INSTANCE.getDomain(userContext, domainPk);
		ProjectPK projectPK = bookmark.getId().getParent();
		Project project = ProjectServiceBaseImpl.getInstance().read(userContext, projectPK, false);
		Universe universe = new Universe(userContext, project);
		//SpaceScope scope = new SpaceScope(universe.S(domain));
		//ExpressionAST expr = scope.parseExpression(expression);
		DomainExpressionScope scope = new DomainExpressionScope(universe, domain);
		//
		ExpressionSuggestionHandler handler = new ExpressionSuggestionHandler(
				scope);
		if (offset == null) {
			offset = expression.length()+1;
		}
		ExpressionSuggestion suggestions = handler.getSuggestion(expression, offset, suggestionType);
		return suggestions;
	}

	@GET
	@Path("{" + BBID_PARAM_NAME + "}/facets/{" + FACETID_PARAM_NAME + "}")
	@ApiOperation(value = "Get facet content using the default BB selection")
	public Facet getFacet(
			@Context HttpServletRequest request, 
			@PathParam(BBID_PARAM_NAME) String BBID,
			@PathParam(FACETID_PARAM_NAME) String facetId,
			@ApiParam(value="filter the facet values using a list of tokens")@QueryParam("filter") String filter,
			@ApiParam(value="maximum number of items to return per page") @QueryParam("maxResults") Integer maxResults,
			@ApiParam(value="index of the first item to start the page") @QueryParam("startIndex") Integer startIndex,
			@ApiParam(value="optional timeout in milliseconds") @QueryParam("timeout") Integer timeoutMs
			) throws ComputingException {

		AppContext userContext = getUserContext(request);
		Bookmark bookmark = getBookmark(userContext, BBID);
		BookmarkConfig config = readConfig(bookmark);
		String domainId = config.getDomain();
		DomainPK domainPk = new DomainPK(bookmark.getId().getParent(), domainId);
		try {
			Domain domain = ProjectManager.INSTANCE.getDomain(userContext, domainPk);
			ProjectFacetJob job = new ProjectFacetJob();
			job.setDomain(Collections.singletonList(domainPk));
			job.setCustomerId(userContext.getCustomerId());
			FacetSelection selection = config.getSelection();
			job.setSelection(selection);
			//
			ProjectPK projectPK = bookmark.getId().getParent();
			Project project = ProjectServiceBaseImpl.getInstance().read(userContext, projectPK, false);
			Universe universe = new Universe(userContext, project);
			List<Domain> domains = Collections.singletonList(domain);
			DashboardSelection sel = EngineUtils.getInstance().applyFacetSelection(userContext,
					universe, domains, selection);
			//return ComputingService.INSTANCE.glitterFacet(universe, domain, ds, axis, filter, startIndex, maxResults, timeoutMs);
			if (SegmentManager.isSegmentFacet(facetId)) {
				DomainHierarchy hierarchy = universe
						.getDomainHierarchy(domain, true);
				return SegmentManager.createSegmentFacet(universe, hierarchy, domain,
						facetId, filter, maxResults, startIndex, sel);
			} else {
				Axis axis = EngineUtils.getInstance().getFacetAxis(userContext,
						universe, facetId);// universe.axis(facetId);
				Domain domain2 = axis.getParent().getTop().getDomain();
				//
				if (!domain2.equals(domain)) {
					DimensionIndex index = axis.getIndex();
					if (index!=null) {
						throw new ScopeException("cannot list the facet for '"+index.getDimensionName()+"': not in the job scope");
					} else {
						throw new ScopeException("cannot list the facet for '"+axis.prettyPrint()+"': not in the job scope");
					}
				}
				
				Facet facet = ComputingService.INSTANCE.glitterFacet(universe,
						domain, sel, axis, filter,
						startIndex != null ? startIndex : 0,
						maxResults != null ? maxResults : 50, timeoutMs);

				if (facet == null) {
					throw new ObjectNotFoundAPIException(
							"no facet found with id : " + facetId,
							userContext.isNoError());
				}
				return facet;
				// KRKN-53: if cannot compute the facet, just return with error informations
				/*
				if (facet.isError()) {
					throw new APIException(facet.getErrorMessage(),
							userContext.isNoError(), ApiError.COMPUTING_FAILED);
				}
				*/
			}
		} catch (ScopeException | ComputingException | InterruptedException e) {
			throw new ComputingException(e.getLocalizedMessage(), e);
		} catch (TimeoutException e) {
			throw new ComputingInProgressAPIException(null,
					userContext.isNoError(), null);
		}
	}

	@POST
	@Path("{" + BBID_PARAM_NAME + "}/analysis")
	@ApiOperation(value = "Run a new Analysis based on the Bookmark scope")
	public AnalysisResult postAnalysis(
			@Context HttpServletRequest request, 
			@ApiParam(value="the analysis query definition", required=true) AnalysisQuery query,
			@PathParam(BBID_PARAM_NAME) String BBID,
			@ApiParam(value = "paging size for the results") @QueryParam("maxResults") Integer maxResults,
			@ApiParam(value = "paging start index") @QueryParam("startIndex") Integer startIndex,
			@ApiParam(value = "if true, get the analysis only if already in cache, else throw a NotInCacheException; if noError returns a null result if the analysis is not in cache ; else regular analysis", defaultValue = "false") @QueryParam("lazy") String lazy
			) throws ComputingException, ScopeException, InterruptedException {
		AppContext userContext = getUserContext(request);
		return runAnalysis(userContext, BBID, query, maxResults, startIndex, lazy);
	}

	@GET
	@Path("{" + BBID_PARAM_NAME + "}/analysis")
	@ApiOperation(value = "Compute the bookmark's default analysis, using default selection")
	public AnalysisResult getAnalysis(
			@Context HttpServletRequest request, 
			@PathParam(BBID_PARAM_NAME) String BBID,
			// groupBy parameter
			@ApiParam(
					value = "override the default groupBy query parameter by providing a list of facets to group the results by. Facet can be defined using it's ID or any valid expression.",
					allowMultiple = true) 
			@QueryParam("groupBy") String[] groupBy, 
			// metric parameter
			@ApiParam(
					value = "override the default metric query parameter by providing a list of metrics to compute. Metric can be defined using it's ID or any valid expression.",
					allowMultiple = true) 
			@QueryParam("metric") String[] metrics, 
			@QueryParam("filter") String[] filterExpressions,
			@QueryParam("orderby") String[] orderExpressions, @QueryParam("rollup") String[] rollupExpressions,
			@QueryParam("limit") Long limit,
			@ApiParam(value = "paging size") @QueryParam("maxResults") Integer maxResults,
			@ApiParam(value = "paging start index") @QueryParam("startIndex") Integer startIndex,
			@ApiParam(value = "if true, get the analysis only if already in cache, else throw a NotInCacheException; if noError returns a null result if the analysis is not in cache ; else regular analysis", defaultValue = "false") @QueryParam("lazy") String lazy
			) throws ComputingException, ScopeException, InterruptedException {
		AppContext userContext = getUserContext(request);
		// init the analysis query using the query parameters
		AnalysisQueryImpl analysis = new AnalysisQueryImpl();
		int groupByLength = groupBy!=null?groupBy.length:0;
		if (groupByLength > 0) {
			List<AnalysisFacet> facets = new ArrayList<AnalysisFacet>();
			for (int i = 0; i < groupBy.length; i++) {
				AnalysisFacet f = new AnalysisQueryImpl.AnalysisFacetImpl();
				f.setExpression(groupBy[i]);// if the name is provided
														// by the expression, we
														// will get it latter
														// when it's parsed
				facets.add(f);
			}
			analysis.setGroupBy(facets);
		}
		if ((metrics != null) && (metrics.length > 0)) {
			List<AnalysisFacet> facets = new ArrayList<AnalysisFacet>();
			for (int i = 0; i < metrics.length; i++) {
				AnalysisFacet f = new AnalysisQueryImpl.AnalysisFacetImpl();
				f.setExpression(metrics[i]);// if the name is provided
														// by the expression, we
														// will get it latter
														// when it's parsed
				facets.add(f);
			}
			analysis.setMetrics(facets);
		}
		if ((filterExpressions != null) && (filterExpressions.length > 0)) {
			analysis.setFilters(Arrays.asList(filterExpressions));
		}
		if ((orderExpressions != null) && (orderExpressions.length > 0)) {
			List<OrderBy> orders = new ArrayList<OrderBy>();
			for (int i = 0; i < orderExpressions.length; i++) {
				OrderBy order = new OrderBy();
				order.setExpression(new Expression(orderExpressions[i]));
				orders.add(order);
			}
			analysis.setOrderBy(orders);
		}
		if ((rollupExpressions != null) && (rollupExpressions.length > 0)) {
			List<RollUp> rollups = new ArrayList<RollUp>();
			int pos = 1;
			for (int i = 0; i < rollupExpressions.length; i++) {
				// ok, do it quick...
				RollUp rollup = new RollUp();
				String expr = rollupExpressions[i].toLowerCase();
				Position position = Position.FIRST;// default
				if (expr.startsWith("last(")) {
					position = Position.LAST;
				}
				expr = expr.replaceAll("", "");
				try {
					int index = Integer.parseInt(expr);
					// rollup can use -1 to compute grand-total
					if (index < -1 || index >= groupByLength) {
						throw new ScopeException("invalid rollup expression at position " + pos
								+ ": the index specified (" + index + ") is not defined");
					}
					rollup.setCol(index);
					rollup.setPosition(position);
				} catch (NumberFormatException e) {
					throw new ScopeException("invalid rollup expression at position " + pos
							+ ": must be a valid indexe N or the expression FIRST(N) or LAST(N) to set the rollup position");
				}
				rollups.add(rollup);
			}
			analysis.setRollups(rollups);
		}
		analysis.setLimit(limit);
		return runAnalysis(userContext, BBID, analysis, maxResults, startIndex, lazy);
	}
	
	public AnalysisResult runAnalysis(
			AppContext userContext,
			String BBID,
			AnalysisQuery query,
			Integer maxResults,
			Integer startIndex,
			String lazy
			) throws ComputingException, ScopeException, InterruptedException {
		Bookmark bookmark = getBookmark(userContext, BBID);
		BookmarkConfig config = readConfig(bookmark);
		String domainId = config.getDomain();
		if (domainId==null || domainId.length()==0) {
			throw new ScopeException("invalid Bookmark configuration, the Domain is not defined");
		}
		DomainPK domainPk = new DomainPK(bookmark.getId().getParent(), domainId);
		Project project = ProjectManager.INSTANCE.getProject(userContext, domainPk.getParent());
		Domain domain = ProjectManager.INSTANCE.getDomain(userContext, domainPk);
		//
		// using the default selection
		FacetSelection selection = config.getSelection();
		//
		Universe universe = new Universe(project);
		initDefaultAnalysis(universe, domain, query, config);
		ProjectAnalysisJob job = createAnalysisJob(userContext, project, query, selection, OutputFormat.JSON);
		//
		boolean lazyFlag = (lazy != null) && (lazy.equals("true") || lazy.equals("noError"));
		//
		DataTable data = AnalysisJobComputer.INSTANCE.compute(userContext, job, maxResults, startIndex, lazyFlag);
		//
		job.setResults(data);
		//
		// create the AnalysisResult
		AnalysisResult result = new AnalysisResult();
		result.setSelection(selection);
		result.setQuery(query);
		result.setData(data);
		//
		return result;
	}
	
	private ProjectAnalysisJob createAnalysisJob(AppContext ctx, Project project, AnalysisQuery analysis, FacetSelection selection, OutputFormat format) throws ScopeException {
		Universe universe = new Universe(ctx, project);
		// read the domain reference
		if (analysis.getDomain() == null) {
			throw new ScopeException("incomplete specification, you must specify the data domain expression");
		}
		Domain domain = getDomain(universe, analysis.getDomain());
		AccessRightsUtils.getInstance().checkRole(ctx, domain, AccessRight.Role.READ);
		// the rest of the ACL is delegated to the AnalysisJob
		Space root = universe.S(domain);

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
		if ((analysis.getGroupBy() == null || analysis.getGroupBy().isEmpty())
		&& (analysis.getMetrics() == null || analysis.getMetrics().isEmpty())) {
			throw new ScopeException("there is no defined facet, can't run the analysis");
		}
		// now we are going to use the domain Space scope
		// -- note that it won't limit the actual expression scope to the bookmark scope - but let's keep that for latter
		SpaceScope scope = new SpaceScope(universe.S(domain));
		// quick fix to support the old facet mechanism
		ArrayList<AnalysisFacet> analysisFacets = new ArrayList<>();
		if (analysis.getGroupBy()!=null) analysisFacets.addAll(analysis.getGroupBy());
		if (analysis.getMetrics()!=null) analysisFacets.addAll(analysis.getMetrics());
		for (AnalysisFacet facet : analysisFacets) {
			ExpressionAST colExpression = scope.parseExpression(facet.getExpression());
			if (colExpression.getName() != null) {
				if (facet.getName() != null && !facet.equals(colExpression.getName())) {
					throw new ScopeException("the facet name is ambiguous: " + colExpression.getName() + "/"
							+ facet.getName() + " for expresion: " + facet.getExpression());
				}
				// else
				facet.setName(colExpression.getName());
			}
			IDomain image = colExpression.getImageDomain();
			if (image.isInstanceOf(IDomain.AGGREGATE)) {
				// it's a metric, we need to relink with the domain
				if (!(colExpression instanceof ExpressionLeaf)) {
					// add parenthesis if it is not a simple expression so A+B
					// => domain.(A+B)
					colExpression = ExpressionMaker.GROUP(colExpression);
				}
				// relink with the domain
				ExpressionAST relink = ExpressionMaker.COMPOSE(new DomainReference(universe, domain), colExpression);
				// now it can be transformed into a measure
				Measure m = universe.asMeasure(relink);
				if (m == null) {
					throw new ScopeException("cannot use expression='" + facet.getExpression() + "'");
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
					throw new ScopeException("cannot use expression='" + colExpression.prettyPrint() + "'");
				}
				//ExpressionAST facetExp = ExpressionMaker.COMPOSE(new SpaceExpression(root), colExpression);
				String name = facet.getName();
				if (name == null) {
					name = formatName(
							axis.getDimension() != null ? axis.getName() : axis.getDefinitionSafe().prettyPrint());
				}
				facets.add(new FacetExpression(axis.prettyPrint(), name));
				//
				lookup.put(facetCount, legacyFacetCount++);
				facetCount++;
			}
		}

		// handle filters
		if (analysis.getFilters() != null) {
			if (selection == null) {
				selection = new FacetSelection();
			}
			for (String filter : analysis.getFilters()) {
				ExpressionAST filterExpr = scope.parseExpression(filter);
				if (!filterExpr.getImageDomain().isInstanceOf(IDomain.CONDITIONAL)) {
					throw new ScopeException("invalid filter, must be a condition");
				}
				Facet segment = SegmentManager.newSegmentFacet(domain);
				FacetMemberString openFilter = SegmentManager.newOpenFilter(filterExpr, filter);
				segment.getSelectedItems().add(openFilter);
				selection.getFacets().add(segment);
			}
		}

		// handle orderBy
		List<OrderBy> orderBy = new ArrayList<>();
		int pos = 1;
		if (analysis.getOrderBy() != null) {
			for (OrderBy order : analysis.getOrderBy()) {
				if (order.getExpression() != null) {
					// let's try to parse it
					try {
						ExpressionAST expr = scope.parseExpression(order.getExpression().getValue());
						IDomain image = expr.getImageDomain();
						Direction direction = getDirection(image);
						if (direction != null) {
							order.setDirection(direction);
						} else if (order.getDirection() == null) {
							// we need direction!
							throw new ScopeException("invalid orderBy expression at position " + pos
									+ ": this is not a sort expression, must use either ASC() or DESC() functions");
						}
						if (image.isInstanceOf(DomainNumericConstant.DOMAIN)) {
							// it is a reference to the facets
							DomainNumericConstant num = (DomainNumericConstant) image
									.getAdapter(DomainNumericConstant.class);
							int index = num.getValue().intValue();
							if (!lookup.containsKey(index)) {
								throw new ScopeException("invalid orderBy expression at position " + pos
										+ ": the index specified (" + index + ") is out of bounds");
							}
							int legacy = lookup.get(index);
							if (metricSet.contains(index)) {
								legacy += legacyFacetCount;
							}
							orderBy.add(new OrderBy(legacy, direction));
						} else {
							// it's an expression which is now scoped into the bookmark
							// but job is expecting it to be scoped in the universe... (OMG)
							String universalExpression = prettyPrint(expr, null);
							orderBy.add(new OrderBy(new Expression(universalExpression), direction));
						}
					} catch (ScopeException e) {
						throw new ScopeException(
								"unable to parse orderBy expression at position " + pos + ": " + e.getCause(), e);
					}
				}
				pos++;
			}
		}
		// handle rollup - fix indexes
		pos = 1;
		if (analysis.getRollups() != null) {
			for (RollUp rollup : analysis.getRollups()) {
				if (rollup.getCol() > -1) {// ignore grand-total
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
			}
		}

		// create
		ProjectAnalysisJobPK pk = new ProjectAnalysisJobPK(project.getId(), null);
		ProjectAnalysisJob analysisJob = new ProjectAnalysisJob(pk);
		analysisJob.setDomains(Collections.singletonList(domain.getId()));
		analysisJob.setMetricList(metrics);
		analysisJob.setFacets(facets);
		analysisJob.setOrderBy(orderBy);
		analysisJob.setSelection(selection);
		analysisJob.setRollups(analysis.getRollups());
		analysisJob.setAutoRun(true);

		// automatic limit?
		if (analysis.getLimit() == null && format == OutputFormat.JSON) {
			int complexity = analysisJob.getFacets().size();
			if (complexity < 4) {
				analysisJob.setLimit((long) Math.pow(10, complexity + 1));
			} else {
				analysisJob.setLimit(100000L);
			}
		} else {
			analysisJob.setLimit(analysis.getLimit());
		}
		return analysisJob;
	}
	
	private Domain getDomain(Universe universe, String definiiton) throws ScopeException {
		// -- using the universe scope for now; will change when merge with T821
		// -- to also support query
		UniverseScope scope = new UniverseScope(universe);
		ExpressionAST domainExpression = scope.parseExpression(definiiton);
		if (!(domainExpression instanceof SpaceExpression)) {
			throw new ScopeException("invalid specification, the domain expression must resolve to a Space");
		}
		Space ref = ((SpaceExpression) domainExpression).getSpace();
		Domain domain = ref.getDomain();
		return domain;
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
		return null;
	}

	private String formatName(String prettyPrint) {
		return prettyPrint.replaceAll("[(),.]", " ").trim().replaceAll("[^ a-zA-Z_0-9]", "").replace(' ', '_');
	}
	
	private void initDefaultAnalysis(Universe universe, Domain domain, AnalysisQuery analysis, BookmarkConfig config) throws ScopeException {
		UniverseScope globalScope = new UniverseScope(universe);
		Space root = universe.S(domain);
		if (analysis.getDomain() == null) {
			analysis.setDomain("@'" + config.getDomain() + "'");
		}
		if (analysis.getLimit() == null) {
			analysis.setLimit(config.getLimit());
		}
		if (analysis.getGroupBy() == null && config.getChosenDimensions() != null) {
			List<AnalysisFacet> groupBy = new ArrayList<AnalysisFacet>();
			for (String chosenDimension : config.getChosenDimensions()) {
				AnalysisFacet f = new AnalysisQueryImpl.AnalysisFacetImpl();
				if (chosenDimension.startsWith("@")) {
					// need to fix the scope
					ExpressionAST expr = globalScope.parseExpression(chosenDimension);
					f.setExpression(prettyPrint(expr, root));
				} else {
					f.setExpression("@'" + chosenDimension + "'");
				}
				groupBy.add(f);
			}
			analysis.setGroupBy(groupBy);
		}
		if (analysis.getMetrics() == null && config.getChosenMetrics() != null) {
			List<AnalysisFacet> metrics = new ArrayList<AnalysisFacet>();
			for (String chosenMetric : config.getChosenMetrics()) {
				AnalysisFacet f = new AnalysisQueryImpl.AnalysisFacetImpl();
				f.setExpression("@'" + chosenMetric + "'");
				metrics.add(f);
			}
			analysis.setMetrics(metrics);
		}
		if (analysis.getOrderBy() == null) {
			for (OrderBy orderBy : config.getOrderBy()) {
				ExpressionAST expr = globalScope.parseExpression(orderBy.getExpression().getValue());
				orderBy.getExpression().setValue(prettyPrint(expr, root));
			}
			analysis.setOrderBy(config.getOrderBy());
		}
		if (analysis.getRollups() == null) {
			analysis.setRollups(config.getRollups());
		}
	}
	
	private String prettyPrint(ExpressionAST expr, Space scope) {
		if (expr instanceof AxisExpression) {
			AxisExpression ref = ((AxisExpression)expr);
			Axis axis = ref.getAxis();
			return axis.prettyPrint(scope);
		} else if (expr instanceof MeasureExpression) {
			MeasureExpression ref = ((MeasureExpression)expr);
			Measure measure = ref.getMeasure();
			return measure.prettyPrint(scope);
		} else {
			return expr.prettyPrint();
		}
	}
	
	private BookmarkConfig readConfig(Bookmark bookmark) {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		try {
			BookmarkConfig config = mapper.readValue(bookmark.getConfig(), BookmarkConfig.class);
			return config;
		} catch (Exception e) {
			throw new APIException(e);
		}
	}
	
	private Bookmark getBookmark(AppContext userContext, String BBID) {
		BookmarkPK bookmarkPk = parseBBID(userContext, BBID);
		return delegate.read(userContext, bookmarkPk);
	}
	
	private BookmarkPK parseBBID(AppContext userContext, String BBID) {
		// the BBID is made of ProjectID:BookmarkID - it's actually the bookmarkPK.toUUID()
		try {
			StringTokenizer tokenizer = new StringTokenizer(BBID, ":");
			String projectId = tokenizer.nextToken();
			ProjectPK projectPk = new ProjectPK(userContext.getCustomerPk(), projectId);
			String bookmarkId = tokenizer.nextToken();
			if (tokenizer.hasMoreTokens()) {
				throw new APIException("invalid ID");
			}
			BookmarkPK bookmarkPk = new BookmarkPK(projectPk, bookmarkId);
			return bookmarkPk;
		} catch (NoSuchElementException e) {
			throw new APIException("invalid ID");
		}
	}

}
