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

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.InvalidIdAPIException;
import com.squid.kraken.v4.api.core.InvalidTokenAPIException;
import com.squid.kraken.v4.api.core.customer.CoreAuthenticatedServiceRest;
import com.squid.kraken.v4.caching.redis.queryworkerserver.QueryWorkerJobStatus;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.model.AnalyticsQuery;
import com.squid.kraken.v4.model.AnalyticsQueryImpl;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.DataLayout;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.NavigationQuery.HierarchyMode;
import com.squid.kraken.v4.model.NavigationQuery.Style;
import com.squid.kraken.v4.model.NavigationQuery.Visibility;
import com.squid.kraken.v4.model.ObjectType;
import com.squid.kraken.v4.model.ValueType;
import com.squid.kraken.v4.model.ViewQuery;
import com.squid.kraken.v4.persistence.AppContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;

/**
 * The new BB API
 * 
 * @author sergefantino
 *
 */
@Path("")
@Api(
		value = "analytics", 
		hidden = false, 
		description = "this is the new analytics API intented to provide all the fun without the pain",
		authorizations = { @Authorization(value = "kraken_auth", scopes = { @AuthorizationScope(scope = "access", description = "Access") }) })
@Produces({ MediaType.APPLICATION_JSON })
public class AnalyticsServiceRest  extends CoreAuthenticatedServiceRest implements AnalyticsServiceConstants {

	static final Logger logger = LoggerFactory.getLogger(AnalyticsServiceRest.class);
	
	@Context
	UriInfo uriInfo;
	
	private AnalyticsServiceBaseImpl delegate(AppContext userContxt) {
		return new AnalyticsServiceBaseImpl(uriInfo, userContxt);
	}
	
	public AnalyticsServiceRest() {
	}

	@GET
	@Path("/analytics")
	@ApiOperation(
			value = "List available content",
			notes = "It provides a comprehensive view including projects, domains, folders and bookmarks."
					+ "You can use it to navigate the entire available content, or access a specific content by defining the parent parameter."
					+ "The root parents are /PROJECTS for listing projects and domains, /MYBOOKMARKS to list the user bookmarks and folders, and /SHARED to list the shared bookmarks and folders."
					+ "By default it lists ony the content directly under the parent, but you can set the hierarchy parameter to view content recursively.")
	public Response listContent(
			@Context HttpServletRequest request,
			// parent
			@ApiParam(value="filter the content under the parent path") @QueryParam(PARENT_PARAM) String parent,
			// q (filter)
			@ApiParam(value="filter the content by name; q can be a multi-token search string separated by comma") 
			@QueryParam("q") String search,
			// hierarchy
			@ApiParam(
					value="define the hierarchy mode. FLAT mode return the hierarchy as a flat list, whereas TREE returns it as a folded structure (NIY)",
					allowableValues="TREE, FLAT") 
			@QueryParam("hierarchy") HierarchyMode hierarchyMode,
			// visibility
			@ApiParam(
					value="filter the result depending on the object visibility", 
					allowableValues="VISIBLE, ALL, HIDDEN", defaultValue="VISIBLE")
			@QueryParam(VISIBILITY_PARAM) Visibility visibility,
			// style
			@ApiParam(
					value="define the result style. If HUMAN, the API will try to use natural reference for objects, like 'My First Project', 'Account', 'Total Sales'... If ROBOT the API will use canonical references that are invariant, e.g. @'5603ca63c531d744b50823a3bis'. If LEGACY the API will also provide internal compound key to lookup objects in the management API.", 
					allowableValues="HTML, ROBOT, HUMAN, LEGACY", defaultValue="HUMAN")
			@QueryParam(STYLE_PARAM) String style,
			// envelope
			@ApiParam(
					value="define the result envelope",
					allowableValues="ALL,RESULT")
			@QueryParam(ENVELOPE_PARAM) String envelope
		) throws ScopeException {
		AppContext userContext = getUserContext(request);
		return delegate(userContext).listContent(userContext, parent, search, hierarchyMode, visibility, computeStyle(style), envelope);
	}

	@GET
	@Path("/analytics/{" + BBID_PARAM_NAME + "}")
	@ApiOperation(value = "Get an item, can be a Domain or a Bookmark")
	public Object getItem(
			@Context HttpServletRequest request, 
			@PathParam(BBID_PARAM_NAME) String BBID) throws ScopeException {
		AppContext userContext = getUserContext(request);
		return delegate(userContext).getItem(userContext, BBID);
	}

	@POST
	@Path("/analytics/{" + BBID_PARAM_NAME + "}")
	@ApiOperation(
			value = "Create/Update a Bookmark",
			notes = "")
	public Bookmark createBookmark(
			@Context HttpServletRequest request,
			@ApiParam(value="the analysis query definition", required=true) AnalyticsQuery query,
			@PathParam(BBID_PARAM_NAME) String BBID,
			@ApiParam(value="the new bookmark name", required=true) @QueryParam("name") String name,
			@ApiParam(value="the new bookmark folder, can be /MYBOOKMARKS, /MYBOOKMARKS/any/folders or /SHARED/any/folders") @QueryParam(PARENT_PARAM) String parent)
	{
		AppContext userContext = getUserContext(request);
		return delegate(userContext).storeBookmark(userContext, query, BBID, null, name, parent);
	}
	
	@POST
	@Path("/analytics/{" + BBID_PARAM_NAME + "}/bookmark")
	@ApiOperation(value = "Create/Update a Bookmark")
	public Bookmark createBookmarkFromForm(
			@Context HttpServletRequest request, 
			@PathParam(BBID_PARAM_NAME) String BBID,
			// bookmark info
			@ApiParam(value="the new bookmark name", required=true) @FormParam("name") String name,
			@ApiParam(value="the new bookmark folder, can be /MYBOOKMARKS, /MYBOOKMARKS/any/folders or /SHARED/any/folders") @FormParam(PARENT_PARAM) String parent,
			// groupBy parameter
			@ApiParam(
					value = GROUPBY_DOC,
					allowMultiple = true
					) 
			@FormParam(GROUP_BY_PARAM) String[] groupBy, 
			// metric parameter
			@ApiParam(
					value = METRICS_DOC,
					allowMultiple = true) 
			@FormParam(METRICS_PARAM) String[] metrics, 
			// filters
			@ApiParam(
					value = FILTERS_DOC,
					allowMultiple = true) 
			@FormParam(FILTERS_PARAM) String[] filterExpressions,
			// period
			@ApiParam(value=PERIOD_DOC) 
			@FormParam(PERIOD_PARAM) String period,
			// timeframe
			@ApiParam(value=TIMEFRAME_DOC, allowMultiple = true) 
			@FormParam(TIMEFRAME_PARAM) String[] timeframe,
			// compareTo
			@ApiParam(value=COMPARETO_DOC, allowMultiple = true) 
			@FormParam(COMPARETO_PARAM) String[] compareframe,
			// orderBy
			@ApiParam(value=ORDERBY_DOC, allowMultiple = true) 
			@FormParam(ORDERBY_PARAM) String[] orderExpressions,
			// rollup
			@ApiParam(value=ROLLUP_DOC, allowMultiple = true) 
			@FormParam(ROLLUP_PARAM) String[] rollupExpressions,
			// limit
			@ApiParam(value=LIMIT_DOC)
			@FormParam(LIMIT_PARAM) Long limit,
			// offset
			@ApiParam(value=OFFSET_DOC)
			@FormParam(OFFSET_PARAM) Long offset,
			// beyondLimit
			@ApiParam(
					value="exclude some dimensions from the limit",
					allowMultiple=true
					)
			@FormParam(BEYOND_LIMIT_PARAM) String[] beyondLimit,
			// maxResults
			@ApiParam(value = MAX_RESULTS_DOC) 
			@FormParam(MAX_RESULTS_PARAM) Integer maxResults,
			// startIndex
			@ApiParam(value = START_INDEX_DOC) 
			@FormParam(START_INDEX_PARAM) Integer startIndex,
			// lazy
			@ApiParam(value = "if true, get the analysis only if already in cache, else throw a NotInCacheException; if noError returns a null result if the analysis is not in cache ; else regular analysis", defaultValue = "false") 
			@FormParam(LAZY_PARAM) String lazy,
			// data
			@ApiParam(
					value="define the analysis data output format.",
					allowableValues="TABLE,RECORDS,TRANSPOSE,SQL,LEGACY")
			@FormParam(DATA_PARAM) String data,
			// apply formatting
			@ApiParam(
					value="apply formatting to the output data")
			@FormParam(APPLY_FORMATTING_PARAM) boolean applyFormatting,
			// style
			@ApiParam(
					value="define the response style. If HUMAN, the API will try to use natural reference for objects, like 'My First Project', 'Account', 'Total Sales'... If MACHINE the API will use canonical references that are invariant, e.g. @'5603ca63c531d744b50823a3bis'. If LEGACY the API will also provide internal compound key to lookup objects in the management API.", 
					allowableValues="LEGACY, MACHINE, HUMAN", defaultValue="HUMAN")
			@FormParam(STYLE_PARAM) String style,
			// envelope
			@ApiParam(
					value="define the result envelope",
					allowableValues="ALL,RESULT,DATA")
			@FormParam(ENVELOPE_PARAM) String envelope,
			// timeout
			@ApiParam(value = "response timeout in milliseconds. If no timeout set, the method will return according to current job status.") 
			@FormParam(TIMEOUT_PARAM) Integer timeout,
			// state
			@FormParam("state") String state
			) throws ComputingException, ScopeException, InterruptedException {
		AppContext userContext = getUserContext(request);
		AnalyticsQuery query = createAnalysisFromParams(null, BBID, groupBy, metrics, filterExpressions, period, timeframe, compareframe, orderExpressions, rollupExpressions, limit, offset, beyondLimit, maxResults, startIndex, lazy, computeStyle(style));
		return delegate(userContext).storeBookmark(userContext, query, BBID, state, name, parent);
	}
	
	@GET
	@Path("/analytics/{" + BBID_PARAM_NAME + "}/bookmark")
	@ApiOperation(value = "Create/Update a Bookmark")
	public Bookmark createBookmarkFromQuery(
			@Context HttpServletRequest request, 
			@PathParam(BBID_PARAM_NAME) String BBID,
			// bookmark info
			@ApiParam(value="the new bookmark name", required=true) @QueryParam("name") String name,
			@ApiParam(value="the new bookmark folder, can be /MYBOOKMARKS, /MYBOOKMARKS/any/folders or /SHARED/any/folders") @QueryParam(PARENT_PARAM) String parent,
			// groupBy parameter
			@ApiParam(
					value = GROUPBY_DOC,
					allowMultiple = true
					) 
			@QueryParam(GROUP_BY_PARAM) String[] groupBy, 
			// metric parameter
			@ApiParam(
					value = METRICS_DOC,
					allowMultiple = true) 
			@QueryParam(METRICS_PARAM) String[] metrics, 
			// filters
			@ApiParam(
					value = FILTERS_DOC,
					allowMultiple = true) 
			@QueryParam(FILTERS_PARAM) String[] filterExpressions,
			// period
			@ApiParam(value=PERIOD_DOC) 
			@QueryParam(PERIOD_PARAM) String period,
			// timeframe
			@ApiParam(value=TIMEFRAME_DOC, allowMultiple = true) 
			@QueryParam(TIMEFRAME_PARAM) String[] timeframe,
			// compareTo
			@ApiParam(value=COMPARETO_DOC, allowMultiple = true) 
			@QueryParam(COMPARETO_PARAM) String[] compareframe,
			// orderBy
			@ApiParam(value=ORDERBY_DOC, allowMultiple = true) 
			@QueryParam(ORDERBY_PARAM) String[] orderExpressions,
			// rollup
			@ApiParam(value=ROLLUP_DOC, allowMultiple = true) 
			@QueryParam(ROLLUP_PARAM) String[] rollupExpressions,
			// limit
			@ApiParam(value=LIMIT_DOC)
			@QueryParam(LIMIT_PARAM) Long limit,
			// offset
			@ApiParam(value=OFFSET_DOC)
			@QueryParam(OFFSET_PARAM) Long offset,
			// beyondLimit
			@ApiParam(
					value="exclude some dimensions from the limit",
					allowMultiple=true
					)
			@QueryParam(BEYOND_LIMIT_PARAM) String[] beyondLimit,
			// maxResults
			@ApiParam(value = MAX_RESULTS_DOC) 
			@QueryParam(MAX_RESULTS_PARAM) Integer maxResults,
			// startIndex
			@ApiParam(value = START_INDEX_DOC) 
			@QueryParam(START_INDEX_PARAM) Integer startIndex,
			// lazy
			@ApiParam(value = "if true, get the analysis only if already in cache, else throw a NotInCacheException; if noError returns a null result if the analysis is not in cache ; else regular analysis", defaultValue = "false") 
			@QueryParam(LAZY_PARAM) String lazy,
			// data
			@ApiParam(
					value="define the analysis data output format.",
					allowableValues="TABLE,RECORDS,TRANSPOSE,SQL,LEGACY")
			@QueryParam(DATA_PARAM) String data,
			// apply formatting
			@ApiParam(
					value="apply formatting to the output data")
			@QueryParam(APPLY_FORMATTING_PARAM) boolean applyFormatting,
			// style
			@ApiParam(
					value="define the response style. If HUMAN, the API will try to use natural reference for objects, like 'My First Project', 'Account', 'Total Sales'... If MACHINE the API will use canonical references that are invariant, e.g. @'5603ca63c531d744b50823a3bis'. If LEGACY the API will also provide internal compound key to lookup objects in the management API.", 
					allowableValues="LEGACY, MACHINE, HUMAN", defaultValue="HUMAN")
			@QueryParam(STYLE_PARAM) String style,
			// envelope
			@ApiParam(
					value="define the result envelope",
					allowableValues="ALL,RESULT,DATA")
			@QueryParam(ENVELOPE_PARAM) String envelope,
			// timeout
			@ApiParam(value = "response timeout in milliseconds. If no timeout set, the method will return according to current job status.") 
			@QueryParam(TIMEOUT_PARAM) Integer timeout,
			// state
			@QueryParam("state") String state
			) throws ComputingException, ScopeException, InterruptedException {
		AppContext userContext = getUserContext(request);
		AnalyticsQuery query = createAnalysisFromParams(null, BBID, groupBy, metrics, filterExpressions, period, timeframe, compareframe, orderExpressions, rollupExpressions, limit, offset, beyondLimit, maxResults, startIndex, lazy, computeStyle(style));
		return delegate(userContext).storeBookmark(userContext, query, BBID, state, name, parent);
	}
	
	@GET
	@Path("/analytics/{" + BBID_PARAM_NAME + "}/scope")
	@ApiOperation(
			value = "Provide information about the expressions available in the bookmark scope",
			notes = "It also allows to check if a given expression is valid in the scope, and further explore the scope if the expression is an object. Using the offset parameter you can get suggestion at the caret position instead of the complete expression value.")
	public Response scopeAnalysis(
			@Context HttpServletRequest request, 
			@PathParam(BBID_PARAM_NAME) String BBID,
			@ApiParam(value="(optional) if you want the scope for a relation, this identify the target domain") 
			@QueryParam("target") String target,
			@ApiParam(value="(optional) the expression to check and get suggestion for, or null in order to get scope level suggestions") 
			@QueryParam("value") String value,
			@ApiParam(value="(optionnal) caret position in the expression value in order to provide relevant suggestions based on the caret position. By default the suggestion are based on the full expression if provided, or else the entire bookmark scope.") 
			@QueryParam("offset") Integer offset,
			@ApiParam(
					value="(optional) the expression type to filter the suggestions. If undefined all valid expression in the context are returned. ",
					allowMultiple=true,
					allowableValues="DIMENSION, METRIC, RELATION, COLUMN, FUNCTION") 
			@QueryParam("types") ObjectType[] types,
			@ApiParam(
					value="(optional) the expression value to filter the suggestions. If undefined all valid expression in the context are returned. ",
					allowMultiple=true,
					allowableValues="OBJECT, NUMERIC, AGGREGATE, DATE, STRING, CONDITION, DOMAIN, OTHER, ERROR") 
			@QueryParam("values") ValueType[] values,
			@ApiParam(
					value="define the response style. If HUMAN, the API will try to use natural reference for objects, like 'My First Project', 'Account', 'Total Sales'... If MACHINE the API will use canonical references that are invariant, e.g. @'5603ca63c531d744b50823a3bis'. If LEGACY the API will also provide internal compound key to lookup objects in the management API.", 
					allowableValues="LEGACY, MACHINE, HUMAN", defaultValue="HUMAN")
			@QueryParam(STYLE_PARAM) String style
			) throws ScopeException
	{
		AppContext userContext = getUserContext(request);
		return delegate(userContext).scopeAnalysis(userContext, BBID, target, value, offset, types, values, computeStyle(style));
	}

	@GET
	@Path("/analytics/{" + BBID_PARAM_NAME + "}/facets/{" + FACETID_PARAM_NAME + "}")
	@ApiOperation(value = "Get facet content using the default BB selection")
	public Facet getFacet(
			@Context HttpServletRequest request, 
			@PathParam(BBID_PARAM_NAME) String BBID,
			@PathParam(FACETID_PARAM_NAME) String facetId,
			@ApiParam(value="search the facet values using a list of tokens")@QueryParam("q") String search,
			@ApiParam(
					value = "Define the filters to apply to results. A filter must be a valid conditional expression. If empty, the subject default parameters will apply. You can use the * token to extend the subject default parameters.",
					allowMultiple = true) 
			@QueryParam(FILTERS_PARAM) String[] filters,
			@ApiParam(value="maximum number of items to return per page") @QueryParam(MAX_RESULTS_PARAM) Integer maxResults,
			@ApiParam(value="index of the first item to start the page") @QueryParam(START_INDEX_PARAM) Integer startIndex,
			@ApiParam(value="optional timeout in milliseconds") @QueryParam(TIMEOUT_PARAM) Integer timeoutMs
			) throws ComputingException {

		AppContext userContext = getUserContext(request);
		return delegate(userContext).getFacet(userContext, BBID, facetId, search, filters, maxResults, startIndex, timeoutMs);
	}

	@POST
	@Path("/analytics/{" + BBID_PARAM_NAME + "}/query")
	@ApiOperation(value = "Run a new Analysis based on the Bookmark scope")
	public Response postAnalysis(
			@Context HttpServletRequest request, 
			@ApiParam(value="the analysis query definition", required=true) AnalyticsQuery query,
			// data
			@PathParam(BBID_PARAM_NAME) String BBID,
			@ApiParam(
					value="define the analysis data format.",
					allowableValues="LEGACY,SQL,RECORDS,TABLE")
			@QueryParam(DATA_PARAM) String data,
			// compareTo growth option
			@ApiParam(value=COMPARETO_COMPUTE_GROWTH_DOC, allowMultiple = false) 
			@QueryParam(COMPARETO_COMPUTE_GROWTH_PARAM) boolean computeGrowth,
			// apply formatting
			@ApiParam(
					value="apply formatting to the output data")
			@QueryParam(APPLY_FORMATTING_PARAM) boolean applyFormatting,
			// envelope
			@ApiParam(
					value="define the result envelope",
					allowableValues="ALL,RESULT,DATA")
			@QueryParam(ENVELOPE_PARAM) String envelope,
			@ApiParam(value = "response timeout in milliseconds. If no timeout set, the method will return according to current job status.") 
			@QueryParam(TIMEOUT_PARAM) Integer timeout,
			// state
			@QueryParam("state") String state
			) throws ComputingException, ScopeException, InterruptedException {
		AppContext userContext = getUserContext(request);
		return delegate(userContext).runAnalysis(userContext, BBID, state, query, getDataLayout(data), computeGrowth, applyFormatting, envelope, timeout);
	}

	@GET
	@Path("/analytics/{" + BBID_PARAM_NAME + "}/query")
	@ApiOperation(value = "Compute an analysis for the subject")
	public Response runAnalysis(
			@Context HttpServletRequest request, 
			@PathParam(BBID_PARAM_NAME) String BBID,
			// groupBy parameter
			@ApiParam(
					value = GROUPBY_DOC,
					allowMultiple = true
					) 
			@QueryParam(GROUP_BY_PARAM) String[] groupBy, 
			// metric parameter
			@ApiParam(
					value = METRICS_DOC,
					allowMultiple = true) 
			@QueryParam(METRICS_PARAM) String[] metrics, 
			// filters
			@ApiParam(
					value = FILTERS_DOC,
					allowMultiple = true) 
			@QueryParam(FILTERS_PARAM) String[] filterExpressions,
			// period
			@ApiParam(value=PERIOD_DOC) 
			@QueryParam(PERIOD_PARAM) String period,
			// timeframe
			@ApiParam(value=TIMEFRAME_DOC, allowMultiple = true) 
			@QueryParam(TIMEFRAME_PARAM) String[] timeframe,
			// compareTo
			@ApiParam(value=COMPARETO_DOC, allowMultiple = true) 
			@QueryParam(COMPARETO_PARAM) String[] compareframe,
			// compareTo growth option
			@ApiParam(value=COMPARETO_COMPUTE_GROWTH_DOC, allowMultiple = false) 
			@QueryParam(COMPARETO_COMPUTE_GROWTH_PARAM) boolean computeGrowth,
			// orderBy
			@ApiParam(value=ORDERBY_DOC, allowMultiple = true) 
			@QueryParam(ORDERBY_PARAM) String[] orderExpressions,
			// rollup
			@ApiParam(value=ROLLUP_DOC, allowMultiple = true) 
			@QueryParam(ROLLUP_PARAM) String[] rollupExpressions,
			// limit
			@ApiParam(value=LIMIT_DOC)
			@QueryParam(LIMIT_PARAM) Long limit,
			// offset
			@ApiParam(value=OFFSET_DOC)
			@QueryParam(OFFSET_PARAM) Long offset,
			// beyondLimit
			@ApiParam(
					value="exclude some dimensions from the limit",
					allowMultiple=true
					)
			@QueryParam(BEYOND_LIMIT_PARAM) String[] beyondLimit,
			// maxResults
			@ApiParam(value = MAX_RESULTS_DOC) 
			@QueryParam(MAX_RESULTS_PARAM) Integer maxResults,
			// startIndex
			@ApiParam(value = START_INDEX_DOC) 
			@QueryParam(START_INDEX_PARAM) Integer startIndex,
			// lazy
			@ApiParam(value = "if true, get the analysis only if already in cache, else throw a NotInCacheException; if noError returns a null result if the analysis is not in cache ; else regular analysis", defaultValue = "false") 
			@QueryParam(LAZY_PARAM) String lazy,
			// data
			@ApiParam(
					value="define the analysis data output format.",
					allowableValues="TABLE,RECORDS,TRANSPOSE,SQL,LEGACY")
			@QueryParam(DATA_PARAM) String data,
			// apply formatting
			@ApiParam(
					value="apply formatting to the output data")
			@QueryParam(APPLY_FORMATTING_PARAM) boolean applyFormatting,
			// style
			@ApiParam(
					value="define the response style. If HUMAN, the API will try to use natural reference for objects, like 'My First Project', 'Account', 'Total Sales'... If MACHINE the API will use canonical references that are invariant, e.g. @'5603ca63c531d744b50823a3bis'. If LEGACY the API will also provide internal compound key to lookup objects in the management API.", 
					allowableValues="LEGACY, MACHINE, HUMAN", defaultValue="HUMAN")
			@QueryParam(STYLE_PARAM) String style,
			// envelope
			@ApiParam(
					value="define the result envelope",
					allowableValues="ALL,RESULT,DATA")
			@QueryParam(ENVELOPE_PARAM) String envelope,
			// timeout
			@ApiParam(value = "response timeout in milliseconds. If no timeout set, the method will return according to current job status.") 
			@QueryParam(TIMEOUT_PARAM) Integer timeout,
			// state
			@QueryParam("state") String state
			) throws ComputingException, ScopeException, InterruptedException {
		AppContext userContext = getUserContext(request);
		AnalyticsQuery analysis = createAnalysisFromParams(null, BBID, groupBy, metrics, filterExpressions, period, timeframe, compareframe, orderExpressions, rollupExpressions, limit, offset, beyondLimit, maxResults, startIndex, lazy, computeStyle(style));
		return delegate(userContext).runAnalysis(userContext, BBID, state, analysis, getDataLayout(data), computeGrowth, applyFormatting, envelope, timeout);
	}

	@GET
	@Path("/analytics/{" + BBID_PARAM_NAME + "}/view")
	@ApiOperation(value = "Generate a dataviz specs from a query")
	public Response viewAnalysis(
			@Context HttpServletRequest request, 
			@PathParam(BBID_PARAM_NAME) String BBID,
			@ApiParam(
					value="set the x axis channel. This must be a valid expression or the special alias __PERIOD to refer to the main period.")
			@QueryParam(VIEW_X_PARAM) String x,
			@ApiParam(
					value="set the y axis channel. This must be a valid expression.")
			@QueryParam(VIEW_Y_PARAM) String y,
			@ApiParam(
					value="set a series channel, displayed using a color palette. This must be a valid expression.")
			@QueryParam(VIEW_COLOR_PARAM) String color,
			@ApiParam(
					value="set a series channel, displayed using the marker size. This must be a valid expression.")
			@QueryParam(VIEW_SIZE_PARAM) String size,
			@ApiParam(
					value="set a facetted channel, displayed as columns. This must be a valid expression.")
			@QueryParam(VIEW_COLUMN_PARAM) String column,
			@ApiParam(
					value="set a facetted channel, displayed as rows. This must be a valid expression.")
			@QueryParam(VIEW_ROW_PARAM) String row,
			// groupBy parameter
			@ApiParam(
					value = GROUPBY_DOC,
					allowMultiple = true
					) 
			@QueryParam(GROUP_BY_PARAM) String[] groupBy, 
			// metric parameter
			@ApiParam(
					value = METRICS_DOC,
					allowMultiple = true) 
			@QueryParam(METRICS_PARAM) String[] metrics, 
			@ApiParam(
					value = FILTERS_DOC,
					allowMultiple = true) 
			@QueryParam(FILTERS_PARAM) String[] filterExpressions,
			@ApiParam(value=PERIOD_DOC) 
			@QueryParam(PERIOD_PARAM) String period,
			@ApiParam(value=TIMEFRAME_DOC, allowMultiple = true) 
			@QueryParam(TIMEFRAME_PARAM) String[] timeframe,
			@ApiParam(value=COMPARETO_DOC, allowMultiple = true) 
			@QueryParam(COMPARETO_PARAM) String[] compareframe,
			@ApiParam(value=ORDERBY_DOC, allowMultiple = true) 
			@QueryParam(ORDERBY_PARAM) String[] orderby, 
			@ApiParam(value=LIMIT_DOC)
			@QueryParam(LIMIT_PARAM) Long limit,
			@ApiParam(value=OFFSET_DOC)
			@QueryParam(OFFSET_PARAM) Long offset,
			@ApiParam(
					value="exclude some dimensions from the limit",
					allowMultiple=true
					)
			@QueryParam(BEYOND_LIMIT_PARAM) String[] beyondLimit,
			@ApiParam(value = "paging size") @QueryParam(MAX_RESULTS_PARAM) Integer maxResults,
			@ApiParam(value = "paging start index") @QueryParam(START_INDEX_PARAM) Integer startIndex,
			@ApiParam(
					value="define how to provide the data, either EMBEDED or through an URL",
					allowableValues="EMBEDED,URL", defaultValue="EMBEDED")
			@QueryParam(DATA_PARAM) String data,
			@ApiParam(
					value="define the response style. If HUMAN, the API will try to use natural reference for objects, like 'My First Project', 'Account', 'Total Sales'... If MACHINE the API will use canonical references that are invariant, e.g. @'5603ca63c531d744b50823a3bis'. If LEGACY the API will also provide internal compound key to lookup objects in the management API.", 
					allowableValues="MACHINE, HUMAN", defaultValue="HUMAN")
			@QueryParam(STYLE_PARAM) String style,
			@ApiParam(
					value="define the result envelope",
					allowableValues="ALL,RESULT")
			@QueryParam(ENVELOPE_PARAM) String envelope,
			// vegalite options
			@ApiParam(value="display options")
			@QueryParam("options") String options
	) throws ScopeException, ComputingException, InterruptedException
	{
		AppContext userContext = getUserContext(request);
		ViewQuery view = new ViewQuery();
		createAnalysisFromParams(view, BBID, groupBy, metrics, filterExpressions, period, timeframe, compareframe, orderby, null, limit, offset, beyondLimit, maxResults, startIndex, null, null);
		view.setX(x);
		view.setY(y);
		view.setColor(color);
		view.setSize(size);
		view.setColumn(column);
		view.setRow(row);
		view.setOptions(options);
		return delegate(userContext).viewAnalysis(userContext, BBID, view, data, computeStyle(style), envelope);
	}

	@GET
	@Path("/analytics/{" + BBID_PARAM_NAME + "}/export/" + "{filename}")
	@ApiOperation(value = "Export an analysis results")
	public Response exportAnalysis(
			@Context HttpServletRequest request, 
			@PathParam(BBID_PARAM_NAME) String BBID,
			@PathParam("filename") String filename,
			// groupBy parameter
			@ApiParam(
					value = GROUPBY_DOC,
					allowMultiple = true
					) 
			@QueryParam(GROUP_BY_PARAM) String[] groupBy, 
			// metric parameter
			@ApiParam(
					value = METRICS_DOC,
					allowMultiple = true) 
			@QueryParam(METRICS_PARAM) String[] metrics, 
			@ApiParam(
					value = FILTERS_DOC,
					allowMultiple = true) 
			@QueryParam(FILTERS_PARAM) String[] filterExpressions,
			@ApiParam(value=PERIOD_DOC)
			@QueryParam(PERIOD_PARAM) String period,
			@ApiParam(value=TIMEFRAME_DOC, allowMultiple = true) 
			@QueryParam(TIMEFRAME_PARAM) String[] timeframe,
			@ApiParam(value=COMPARETO_DOC, allowMultiple = true) 
			@QueryParam(COMPARETO_PARAM) String[] compareframe,
			@ApiParam(allowMultiple = true, value=ORDERBY_DOC) 
			@QueryParam(ORDERBY_PARAM) String[] orderExpressions, 
			@ApiParam(value=ROLLUP_DOC, allowMultiple = true) 
			@QueryParam(ROLLUP_PARAM) String[] rollupExpressions,
			@ApiParam(value=LIMIT_DOC)
			@QueryParam(LIMIT_PARAM) Long limit,
			@ApiParam(value=OFFSET_DOC)
			@QueryParam(OFFSET_PARAM) Long offset,
			@ApiParam(
					value="exclude some dimensions from the limit",
					allowMultiple=true
					)
			@QueryParam(BEYOND_LIMIT_PARAM) String[] beyondLimit
			) throws ComputingException, ScopeException, InterruptedException {
		AppContext userContext = getUserContext(request);
		String[] split = filename.split("\\.");
		String filepart = null;
		String fileext = null;
		String compression = null;
		if (split.length > 0) {
			filepart = split[0];
		}
		if (split.length > 1) {
			fileext = split[1];
		}
		if (split.length > 2) {
			compression = split[2];
			if (compression.equals("gz")) {
				compression = "gzip";
			}
		}
		AnalyticsQuery analysis = createAnalysisFromParams(null, BBID, groupBy, metrics, filterExpressions, period, timeframe, compareframe, orderExpressions, rollupExpressions, limit, offset, beyondLimit, null, null, null, null);
		return delegate(userContext).exportAnalysis(userContext, BBID, analysis, filepart, fileext, compression);
	}
	
	@GET
    @Path("/status/{"+"QUERYID"+"}")
	@ApiOperation(value = "get the ongoing status of the analysis identified by its QueryID")
	public List<QueryWorkerJobStatus> getStatus(
			@Context HttpServletRequest request, 
			@ApiParam(value="this is the AnalysisQuery QueryID") @PathParam("QUERYID") String key) {
		AppContext userContext = getUserContext(request);
		return delegate(userContext).getStatus(userContext, key);
	}
	
	@GET
    @Path("/status/{"+"QUERYID"+"}/cancel")
	@ApiOperation(value = "cancel the execution of the analysis identified by its QueryID")
	public boolean cancelQuery(
			@Context HttpServletRequest request, 
			@ApiParam(value="this is the AnalysisQuery QueryID") @PathParam("QUERYID") String key) {
		AppContext userContext = getUserContext(request);
		return delegate(userContext).cancelQuery(userContext, key);
	}
	
	public Style computeStyle(String style) {
		if (style==null || style.trim().equals("")) {
			return Style.HUMAN;
		} else {
			try {
				return Style.valueOf(style.toUpperCase());
			} catch (IllegalArgumentException e) {
				throw new APIException("Illegal argument exception: '"+style+"' is not a valid argument for STYLE, must be HUMAN, ROBOT, HTML");
			}
		}
	}
	
	/**
	 * transform the GET query parameters into a AnalysisQuery similar to the one used for POST
	 * @param bBID 
	 * @param groupBy
	 * @param metrics
	 * @param filterExpressions
	 * @param orderExpressions
	 * @param rollupExpressions
	 * @param limit
	 * @param offset 
	 * @return
	 * @throws ScopeException
	 */
	private AnalyticsQuery createAnalysisFromParams(
			AnalyticsQuery query,
			String BBID, 
			String[] groupBy, 
			String[] metrics, 
			String[] filterExpressions,
			String period,
			String[] timeframe,
			String[] compareframe,
			String[] orderExpressions, 
			String[] rollupExpressions, 
			Long limit,
			Long offset, 
			String[] beyondLimit,
			Integer maxResults, 
			Integer startIndex, 
			String lazy, 
			Style style
		) throws ScopeException {
		// init the analysis query using the query parameters
		if (query==null) query = new AnalyticsQueryImpl();
		query.setBBID(BBID);
		int groupByLength = groupBy!=null?groupBy.length:0;
		if (groupByLength > 0) {
			query.setGroupBy(new ArrayList<String>());
			for (String value : groupBy) {
				// skip empty strings
				if (value!=null && value.length()>0) query.getGroupBy().add(value);
			}
		}
		if ((metrics != null) && (metrics.length > 0)) {
			query.setMetrics(new ArrayList<String>());
			for (String value : metrics) {
				// skip empty strings
				if (value!=null && value.length()>0) query.getMetrics().add(value);
			}
		}
		if ((filterExpressions != null) && (filterExpressions.length > 0)) {
			query.setFilters(new ArrayList<String>());
			for (String value : filterExpressions) {
				// skip empty strings
				if (value!=null && value.length()>0) query.getFilters().add(value);
			}
		}
		if (period!=null) {
			query.setPeriod(period);
		}
		if (timeframe != null && timeframe.length>0) {
			query.setTimeframe(new ArrayList<String>());
			for (String value : timeframe) {
				// skip empty strings
				if (value!=null && value.length()>0) query.getTimeframe().add(value);
			}
		}
		if (compareframe != null && compareframe.length>0) {
			query.setCompareTo(new ArrayList<String>());
			for (String value : compareframe) {
				// skip empty strings
				if (value!=null && value.length()>0) query.getCompareTo().add(value);
			}
		}
		if ((orderExpressions != null) && (orderExpressions.length > 0)) {
			query.setOrderBy(new ArrayList<String>());
			for (String value : orderExpressions) {
				// skip empty strings
				if (value!=null && value.length()>0) query.getOrderBy().add(value);
			}
		}
		if ((rollupExpressions != null) && (rollupExpressions.length > 0)) {
			List<String> rollups = new ArrayList<>();
			for (int i = 0; i < rollupExpressions.length; i++) {
				// do not check for integrity here
				String expr = rollupExpressions[i];
				if (expr!=null && !expr.equals("")) {
					rollups.add(expr);
				}
			}
			query.setRollups(rollups);
		}
		if (limit!=null && limit>0) query.setLimit(limit);
		if (offset!=null && offset>0) query.setOffset(offset);
		if (beyondLimit!=null && beyondLimit.length > 0) {
			query.setBeyondLimit(new ArrayList<String>());
			for (String value : beyondLimit) {
				// skip empty strings
				if (value!=null && value.length()>0) query.getBeyondLimit().add(value);
			}
		}
		if (maxResults!=null && maxResults>0) query.setMaxResults(maxResults);
		if (startIndex!=null && startIndex>0) query.setStartIndex(startIndex);
		if (lazy!=null) query.setLazy(lazy);
		if (style!=null) query.setStyle(style);
		return query;
	}
	
	@Override
	protected AppContext getUserContext(HttpServletRequest request) {
		try {
			return super.getUserContext(request);
		} catch (InvalidTokenAPIException e) {
			// add the redirect information
			UriBuilder builder = delegate(null).getPublicBaseUriBuilder();
			String path = cleanPath(uriInfo.getRequestUri().getPath());
			builder.path(path);
			MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
			boolean style =false;
			for (Entry<String, List<String>> entry : queryParams.entrySet()) {
				if (!entry.getKey().equals(ACCESS_TOKEN_PARAM) && !entry.getValue().isEmpty()) {
					if (entry.getKey().equals(STYLE_PARAM)) style=true;
					for (String value : entry.getValue()) {
						builder.queryParam(entry.getKey(), value);
					}
				}
			}
			if (!style) builder.queryParam(STYLE_PARAM, Style.HTML);
			String full = uriInfo.getRequestUri().toString();
			if (full.contains("#")) {
				String fragment = full.substring(full.lastIndexOf("#"));
				builder.fragment(fragment);
			}
			throw new InvalidTokenAPIException(e.getMessage(), builder.build(), "admin_console", e.isNoError(), KrakenConfig.getAuthServerEndpoint());
		}
	}
	
	private String cleanPath(String path) {
		int pos = path.indexOf("/analytics");
		path = pos>0?path.substring(pos):path;
		while (path.contains("&access_token")) {
			int x = path.lastIndexOf("&access_token=");
			if (path.indexOf("&", x+1)>0) {
				path = path.substring(0, x)+path.substring(path.indexOf("&", x+1));
			} else if (path.indexOf("#", x+1)>0) {
				path = path.substring(0, x)+path.substring(path.indexOf("#", x+1));
			} else {
				path = path.substring(0, x);
			}
		}
		return path;
	}


	/**
	 * @param data
	 * @return
	 */
	private DataLayout getDataLayout(String data) {
		if (data==null || data.equals("")) {
			return null;
		} else {
			try {
				return DataLayout.valueOf(data);
			} catch (IllegalArgumentException e) {
				throw new InvalidIdAPIException("invalid value for parameter "+DATA_PARAM+", must be: "+DATA_PARAM_VALUES, true);
			}
		}
	}

}
