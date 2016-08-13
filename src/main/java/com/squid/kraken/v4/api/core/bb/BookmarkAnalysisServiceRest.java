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
import java.util.List;

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

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.customer.CoreAuthenticatedServiceRest;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.model.AnalysisQuery;
import com.squid.kraken.v4.model.AnalysisQuery.AnalysisFacet;
import com.squid.kraken.v4.model.AnalysisQueryImpl;
import com.squid.kraken.v4.model.AnalysisResult;
import com.squid.kraken.v4.model.Expression;
import com.squid.kraken.v4.model.ExpressionSuggestion;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.ObjectType;
import com.squid.kraken.v4.model.ProjectAnalysisJob.OrderBy;
import com.squid.kraken.v4.model.ProjectAnalysisJob.Position;
import com.squid.kraken.v4.model.ProjectAnalysisJob.RollUp;
import com.squid.kraken.v4.persistence.AppContext;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.Authorization;
import com.wordnik.swagger.annotations.AuthorizationScope;

/**
 * The new BB API
 * 
 * @author sergefantino
 *
 */
@Path("/bb")
@Api(value = "bookmark-analysis", hidden = false, authorizations = { @Authorization(value = "kraken_auth", type = "oauth2", scopes = { @AuthorizationScope(scope = "access", description = "Access") }) })
@Produces({ MediaType.APPLICATION_JSON })
public class BookmarkAnalysisServiceRest  extends CoreAuthenticatedServiceRest {

	private BookmarkAnalysisServiceBaseImpl delegate = BookmarkAnalysisServiceBaseImpl
			.INSTANCE;

	static final Logger logger = LoggerFactory.getLogger(BookmarkAnalysisServiceRest.class);

	private final static String BBID_PARAM_NAME = "BBID";
	private final static String FACETID_PARAM_NAME = "FACETID";
	
	public BookmarkAnalysisServiceRest() {
	}

	@GET
	@Path("")
	@ApiOperation(
			value = "List available content",
			notes = "it provides a comprehensive view including projects, domains, folders and bookmarks")
	public List<NavigationItem> listContent(
			@Context HttpServletRequest request,
			@QueryParam("parent") String parent,
			@ApiParam(value="if flat flag is true, the complete folder hierarchy is returned") @QueryParam("flat") boolean isFlat
		) throws ScopeException {
		AppContext userContext = getUserContext(request);
		return delegate.listContent(userContext, parent, isFlat);
	}
	
	@GET
	@Path("{" + BBID_PARAM_NAME + "}")
	@ApiOperation(value = "Get an item, can be a Domain or a Bookmark")
	public Object getItem(@Context HttpServletRequest request, @PathParam(BBID_PARAM_NAME) String BBID) throws ScopeException {
		AppContext userContext = getUserContext(request);
		return delegate.getItem(userContext, BBID);
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
	
	/*
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
	*/
	
	/*
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
	*/
	
	@GET
	@Path("{" + BBID_PARAM_NAME + "}/scope")
	@ApiOperation(
			value = "Provide information about the expressions available in the bookmark scope",
			notes = "It also allows to check if a given expression is valid in the scope, and further explore the scope if the expression is an object. Using the offset parameter you can get suggestion at the caret position instead of the complete expression value.")
	public ExpressionSuggestion evaluateExpression(
			@Context HttpServletRequest request, 
			@PathParam(BBID_PARAM_NAME) String BBID,
			@ApiParam(value="optionally the expression to check and get suggestion for, or null in order to get scope level suggestions") 
			@QueryParam("value") String expression,
			@ApiParam(value="optionnal caret position in the expression value in order to provide relevant suggestions based on the caret position. By default the suggestion are based on the full expression if provided, or else the entire bookmark scope.") 
			@QueryParam("offset") Integer offset,
			@ApiParam(
					value="optional expression kind to filter the suggestions. If undefined all valid expression in the context are returned. ",
					allowMultiple=true,
					allowableValues="DIMENSION, METRIC, DOMAIN, COLUMN, FUNCTION") 
			@QueryParam("kind") ObjectType[] kindFilters
			) throws ScopeException
	{
		AppContext userContext = getUserContext(request);
		return delegate.evaluateExpression(userContext, BBID, expression, offset, kindFilters);
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
		return delegate.getFacet(userContext, BBID, facetId, filter, maxResults, startIndex, timeoutMs);
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
		return delegate.runAnalysis(userContext, BBID, query, maxResults, startIndex, lazy);
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
					allowMultiple = true
					) 
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
		return delegate.runAnalysis(userContext, BBID, analysis, maxResults, startIndex, lazy);
	}
	
	

}
