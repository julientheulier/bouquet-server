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
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPOutputStream;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriBuilderException;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.cxf.jaxrs.impl.UriBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.squid.core.concurrent.ExecutionManager;
import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.domain.DomainNumericConstant;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.operators.IntrinsicOperators;
import com.squid.core.domain.operators.OperatorDefinition;
import com.squid.core.domain.sort.DomainSort;
import com.squid.core.domain.sort.DomainSort.SortDirection;
import com.squid.core.domain.sort.SortOperatorDefinition;
import com.squid.core.domain.vector.VectorOperatorDefinition;
import com.squid.core.expression.ConstantValue;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.Operator;
import com.squid.core.expression.PrettyPrintOptions;
import com.squid.core.expression.PrettyPrintOptions.ReferenceStyle;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.poi.ExcelFile;
import com.squid.core.poi.ExcelSettingsBean;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.RenderingException;
import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.ComputingInProgressAPIException;
import com.squid.kraken.v4.api.core.EngineUtils;
import com.squid.kraken.v4.api.core.InvalidIdAPIException;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl.OutputCompression;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl.OutputFormat;
import com.squid.kraken.v4.api.core.JobStats;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.api.core.PerfDB;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.api.core.bookmark.BookmarkServiceBaseImpl;
import com.squid.kraken.v4.api.core.projectanalysisjob.AnalysisJobComputer;
import com.squid.kraken.v4.caching.NotInCacheException;
import com.squid.kraken.v4.caching.redis.RedisCacheManager;
import com.squid.kraken.v4.caching.redis.queryworkerserver.QueryWorkerJobStatus;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.datamatrix.IDataMatrixConverter;
import com.squid.kraken.v4.core.analysis.datamatrix.RecordConverter;
import com.squid.kraken.v4.core.analysis.datamatrix.TransposeConverter;
import com.squid.kraken.v4.core.analysis.engine.bookmark.BookmarkManager;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex.Status;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchy;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchyManager;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.SegmentManager;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingService;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.model.DashboardAnalysis;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
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
import com.squid.kraken.v4.core.expression.scope.DomainExpressionScope;
import com.squid.kraken.v4.core.expression.scope.ExpressionSuggestionHandler;
import com.squid.kraken.v4.core.model.domain.DomainDomain;
import com.squid.kraken.v4.export.ExportSourceWriter;
import com.squid.kraken.v4.export.ExportSourceWriterCSV;
import com.squid.kraken.v4.export.ExportSourceWriterXLSX;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.AnalyticsQuery;
import com.squid.kraken.v4.model.AnalyticsQueryImpl;
import com.squid.kraken.v4.model.AnalyticsReply;
import com.squid.kraken.v4.model.AnalyticsResult;
import com.squid.kraken.v4.model.AnalyticsResult.Info;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.BookmarkConfig;
import com.squid.kraken.v4.model.BookmarkFolderPK;
import com.squid.kraken.v4.model.BookmarkPK;
import com.squid.kraken.v4.model.DataTable;
import com.squid.kraken.v4.model.DataTable.Col;
import com.squid.kraken.v4.model.DataTable.Row;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Expression;
import com.squid.kraken.v4.model.ExpressionSuggestion;
import com.squid.kraken.v4.model.ExpressionSuggestionItem;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.FacetExpression;
import com.squid.kraken.v4.model.FacetMember;
import com.squid.kraken.v4.model.FacetMemberInterval;
import com.squid.kraken.v4.model.FacetMemberString;
import com.squid.kraken.v4.model.FacetSelection;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.NavigationItem;
import com.squid.kraken.v4.model.NavigationQuery;
import com.squid.kraken.v4.model.NavigationQuery.HierarchyMode;
import com.squid.kraken.v4.model.NavigationQuery.Style;
import com.squid.kraken.v4.model.NavigationQuery.Visibility;
import com.squid.kraken.v4.model.NavigationReply;
import com.squid.kraken.v4.model.NavigationResult;
import com.squid.kraken.v4.model.ObjectType;
import com.squid.kraken.v4.model.Problem;
import com.squid.kraken.v4.model.Problem.Severity;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectAnalysisJob;
import com.squid.kraken.v4.model.ProjectAnalysisJob.Direction;
import com.squid.kraken.v4.model.ProjectAnalysisJob.Index;
import com.squid.kraken.v4.model.ProjectAnalysisJob.OrderBy;
import com.squid.kraken.v4.model.ProjectAnalysisJob.RollUp;
import com.squid.kraken.v4.model.ProjectAnalysisJobPK;
import com.squid.kraken.v4.model.ProjectFacetJob;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.ValueType;
import com.squid.kraken.v4.model.ViewQuery;
import com.squid.kraken.v4.model.ViewReply;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.ProjectDAO;
import com.squid.kraken.v4.vegalite.VegaliteConfigurator;
import com.squid.kraken.v4.vegalite.VegaliteSpecs;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Data;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.DataType;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Encoding;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Format;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.FormatType;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Mark;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Operation;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Order;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Sort;

/**
 * @author sergefantino
 *
 */
public class AnalyticsServiceBaseImpl implements AnalyticsServiceConstants {

	static final Logger logger = LoggerFactory
			.getLogger(AnalyticsServiceBaseImpl.class);

	private UriInfo uriInfo = null;
	
	private URI publicBaseUri = null;
	private AppContext userContext = null;
	
	protected AnalyticsServiceBaseImpl(UriInfo uriInfo, AppContext userContext) {
		this.uriInfo = uriInfo;
		this.publicBaseUri = getPublicBaseUri(uriInfo);
		this.userContext = userContext;
	}
	
	public UriBuilder getPublicBaseUriBuilder() {
		return new UriBuilderImpl(publicBaseUri);
	}

	/**
	 * @param uriInfo2
	 * @return
	 */
	private URI getPublicBaseUri(UriInfo uriInfo) {
		// first check if there is a publicBaseUri parameter
		String uri = KrakenConfig.getProperty(KrakenConfig.publicBaseUri, true);
		if (uri!=null) {
			try {
				return new URI(uri);
			} catch (URISyntaxException e) {
				// let's try the next
			}
		}
		// second, try to use the OAuth endpoint
		String oauthEndpoint = KrakenConfig.getProperty(KrakenConfig.krakenOAuthEndpoint,true);
		if (oauthEndpoint!=null) {
			try {
				URI check = new URI(oauthEndpoint);
				// check that it is not using the ob.io central auth
				if (!"auth.openbouquet.io".equalsIgnoreCase(check.getHost())) {
					while (oauthEndpoint.endsWith("/")) {
						oauthEndpoint = oauthEndpoint.substring(0, oauthEndpoint.length()-1);
					}
					if (oauthEndpoint.endsWith("/auth/oauth")) {
						oauthEndpoint = oauthEndpoint.substring(0,oauthEndpoint.length() - "/auth/oauth".length());
						return new URI(oauthEndpoint+"/v4.2");
					}
				}
			} catch (URISyntaxException e) {
				// let's try the next
			}
		}
		// last, use the uriInfo
		return uriInfo.getBaseUri();
	}

	private static final NavigationItem ROOT_FOLDER = new NavigationItem("Root", "list all your available content, organize by Projects and Bookmarks", null, "/", "FOLDER");
	private static final NavigationItem PROJECTS_FOLDER = new NavigationItem("Projects", "list all your Projects", "/", "/PROJECTS", "FOLDER");
	private static final NavigationItem SHARED_FOLDER = new NavigationItem("Shared Bookmarks", "list all the bookmarks shared with you", "/", "/SHARED", "FOLDER");
	private static final NavigationItem MYBOOKMARKS_FOLDER = new NavigationItem("My Bookmarks", "list all your bookmarks", "/", "/MYBOOKMARKS", "FOLDER");

	public Response listContent(
			AppContext userContext,
			String parent,
			String search,
			HierarchyMode hierarchyMode, 
			Visibility visibility,
			Style style,
			String envelope
		) throws ScopeException {
		if (parent !=null && parent.endsWith("/")) {
			parent = parent.substring(0, parent.length()-1);// remove trailing /
		}
		NavigationQuery query = new NavigationQuery();
		query.setParent(parent);
		query.setQ(search);
		query.setHiearchy(hierarchyMode);
		query.setStyle(style!=null?style:Style.HUMAN);
		query.setVisibility(visibility!=null?visibility:Visibility.ALL);
		if (envelope==null) {
			if (query.getStyle()==Style.HUMAN || query.getStyle()==Style.HTML) {
				envelope = "RESULT";
			} else {
				envelope = "ALL";
			}
		}
		//
		// tokenize the search string
		String[] filters = null;
		if (search!=null) filters = search.toLowerCase().split(" ");
		//
		NavigationResult result = new NavigationResult();
		result.setChildren(new ArrayList<NavigationItem>());
		if (parent==null || parent.length()==0) {
			result.setParent(createLinkableFolder(userContext, query, ROOT_FOLDER));
			// this is the root
			result.getChildren().add(createLinkableFolder(userContext, query, PROJECTS_FOLDER));
			if (hierarchyMode!=null) listProjects(userContext, query, PROJECTS_FOLDER.getSelfRef(), filters, hierarchyMode, result.getChildren());
			result.getChildren().add(createLinkableFolder(userContext, query, SHARED_FOLDER));
			if (hierarchyMode!=null) listSharedBoomarks(userContext, query, SHARED_FOLDER.getSelfRef(), filters, hierarchyMode, result.getChildren());
			result.getChildren().add(createLinkableFolder(userContext, query, MYBOOKMARKS_FOLDER));
			if (hierarchyMode!=null) listMyBoomarks(userContext, query, MYBOOKMARKS_FOLDER.getSelfRef(), filters, hierarchyMode, result.getChildren());
		} else {
			// need to list parent's content
			if (parent.startsWith(PROJECTS_FOLDER.getSelfRef())) {
				result.setParent(listProjects(userContext, query, parent, filters, hierarchyMode, result.getChildren()));
			} else if (parent.startsWith(SHARED_FOLDER.getSelfRef())) {
				result.setParent(listSharedBoomarks(userContext, query, parent, filters, hierarchyMode, result.getChildren()));
			} else if (parent.startsWith(MYBOOKMARKS_FOLDER.getSelfRef())) {
				result.setParent(listMyBoomarks(userContext, query, parent, filters, hierarchyMode, result.getChildren()));
			} else {
				// invalid
				throw new ObjectNotFoundAPIException("invalid parent reference", true);
			}
		}
		// sort content
		sortNavigationContent(result.getChildren());
		// parent
		if (result.getParent()!=null) {
			result.getParent().setLink(createLinkToFolder(userContext, query, result.getParent().getSelfRef()));// self link
			if (result.getParent().getParentRef()!=null && !result.getParent().getParentRef().equals("")) {
				result.getParent().setUpLink(createLinkToFolder(userContext, query, result.getParent().getParentRef()));// self link
			}
		}
		// create results
		if (style==Style.HTML) {
			return createHTMLPageList(userContext, query, result);
		} else if (envelope.equalsIgnoreCase("ALL")) {
			return Response.ok(new NavigationReply(query, result)).build();
		} else {// RESULT
			return Response.ok(result).build();
		}
	}
	
	private NavigationItem createLinkableFolder(AppContext userContext, NavigationQuery query, NavigationItem folder) {
		if (query.getStyle()==Style.HUMAN || query.getStyle()==Style.HTML) {
			NavigationItem copy = new NavigationItem(folder);
			copy.setLink(createLinkToFolder(userContext, query, copy));
			return copy;
		} else {
			return folder;
		}
	}
	
	private static final List<String> topLevelOrder = Arrays.asList(new String[]{PROJECTS_FOLDER.getSelfRef(), SHARED_FOLDER.getSelfRef(), MYBOOKMARKS_FOLDER.getSelfRef()});
	private static final List<String> typeOrder = Arrays.asList(new String[]{NavigationItem.FOLDER_TYPE, NavigationItem.BOOKMARK_TYPE, NavigationItem.DOMAIN_TYPE});
	
	/**
	 * @param content
	 */
	private void sortNavigationContent(List<NavigationItem> content) {
		Collections.sort(content, new Comparator<NavigationItem>() {
			@Override
			public int compare(NavigationItem o1, NavigationItem o2) {
				if (o1.getParentRef()==ROOT_FOLDER.getSelfRef() && o2.getParentRef()==ROOT_FOLDER.getSelfRef()) {
					// special rule for top level
					return Integer.compare(topLevelOrder.indexOf(o1.getSelfRef()),topLevelOrder.indexOf(o2.getSelfRef()));
				} else if (o1.getParentRef().equals(o2.getParentRef())) {
					if (o1.getType().equals(o2.getType())) {
						// sort by name
						return o1.getName().compareTo(o2.getName());
					} else {
						// sort by type
						return Integer.compare(typeOrder.indexOf(o1.getType()),typeOrder.indexOf(o2.getType()));
					}
				} else {
					// sort by parent
					return o1.getParentRef().compareTo(o2.getParentRef());
				}
			}
		});
	}

	/**
	 * list the projects and their content (domains) depending on the parent path.
	 * Note that this method does not support the flat mode because of computing constraints.
	 * @param userContext
	 * @param query 
	 * @param parent
	 * @param content
	 * @throws ScopeException
	 */
	private NavigationItem listProjects(AppContext userContext, NavigationQuery query, String parent, String[] filters, HierarchyMode hierarchyMode, List<NavigationItem> content) throws ScopeException {
		// list project related resources
		if (parent==null || parent.equals("") || parent.equals("/") || parent.equals(PROJECTS_FOLDER.getSelfRef())) {
			// return available project
			List<Project> projects = ((ProjectDAO) DAOFactory.getDAOFactory().getDAO(Project.class))
					.findByCustomer(userContext, userContext.getCustomerPk());
			for (Project project : projects) {
				if (filters==null || filter(project, filters)) {
					NavigationItem folder = new NavigationItem(query, project, parent);
					if (query.getStyle()==Style.HUMAN || query.getStyle()==Style.HTML) {
						folder.setLink(createLinkToFolder(userContext, query, folder));
						folder.setObjectLink(createObjectLink(userContext, query, project));
					}
					HashMap<String, String> attrs = new HashMap<>();
					attrs.put("jdbc", project.getDbUrl());
					folder.setAttributes(attrs);
					content.add(folder);
				}
			}
			return createLinkableFolder(userContext, query, PROJECTS_FOLDER);
		} else if (parent.startsWith(PROJECTS_FOLDER.getSelfRef())) {
			String projectRef = parent.substring(PROJECTS_FOLDER.getSelfRef().length()+1);// remove /PROJECTS/ part
			Project project = findProject(userContext, projectRef);
			List<Domain> domains = ProjectManager.INSTANCE.getDomains(userContext, project.getId());
			Visibility visibility = query.getVisibility();
			for (Domain domain : domains) {
				if (visibility==Visibility.ALL || isVisible(userContext, domain, visibility)) {
					String name = domain.getName();
					if (filters==null || filter(name, filters)) {
						NavigationItem item = new NavigationItem(query, project, domain, parent);
						if (query.getStyle()==Style.HUMAN || query.getStyle()==Style.HTML) {
							item.setLink(createLinkToAnalysis(userContext, query, item));
							item.setObjectLink(createObjectLink(userContext, query, domain));
							item.setViewLink(createLinkToView(userContext, query, item));
						}
						HashMap<String, String> attrs = new HashMap<>();
						attrs.put("project", project.getName());
						item.setAttributes(attrs);
						content.add(item);
					}
				}
			}
			return new NavigationItem(query, project, PROJECTS_FOLDER.getSelfRef());
		} else {
			// what's this parent anyway???
			return null;
		}
	}
	
	/**
	 * @param userContext
	 * @param domain
	 * @param visibility
	 * @return
	 */
	private boolean isVisible(AppContext ctx, Domain domain, Visibility visibility) {
		boolean visible = ProjectManager.INSTANCE.isVisible(ctx, domain);
		if (!visible) return false;
		if (visibility==Visibility.ALL) return true;
		if (visibility==Visibility.VISIBLE) {
			return !domain.isDynamic();
		} else if (visibility==Visibility.HIDDEN) {
			return domain.isDynamic();
		} else {
			return false;
		}
	}

	/**
	 * create a link to the NavigationItem
	 * @param item
	 * @return
	 */
	private URI createLinkToFolder(AppContext userContext, NavigationQuery query, NavigationItem item) {
		return createNavigationQuery(userContext, query).build(item.getSelfRef());
	}
	
	private URI createLinkToFolder(AppContext userContext, NavigationQuery query, String selfRef) {
		return createNavigationQuery(userContext, query).build(selfRef!=null?selfRef:"");
	}
	
	private URI createLinkToAnalysis(AppContext userContext, NavigationQuery query, NavigationItem item) {
		UriBuilder builder =
			getPublicBaseUriBuilder().path("/analytics/{"+BBID_PARAM_NAME+"}/query");
		if (query.getStyle()!=null) builder.queryParam(STYLE_PARAM, query.getStyle());
		builder.queryParam("access_token", userContext.getToken().getOid());
		return builder.build(item.getSelfRef());
	}
	
	private URI createLinkToView(AppContext userContext, NavigationQuery query, NavigationItem item) {
		UriBuilder builder =
				getPublicBaseUriBuilder().path("/analytics/{"+BBID_PARAM_NAME+"}/view");
		if (query.getStyle()!=null) builder.queryParam(STYLE_PARAM, query.getStyle());
		builder.queryParam("access_token", userContext.getToken().getOid());
		return builder.build(item.getSelfRef());
	}
	
	private UriBuilder createNavigationQuery(AppContext userContext, NavigationQuery query) {
		UriBuilder builder = 
				getPublicBaseUriBuilder().path("/analytics").queryParam("parent", "{PARENT}");
		if (query.getHiearchy()!=null) builder.queryParam("hierarchy", query.getHiearchy());
		if (query.getStyle()!=null) builder.queryParam(STYLE_PARAM, query.getStyle());
		if (query.getVisibility()!=null) builder.queryParam(VISIBILITY_PARAM, query.getVisibility());
		builder.queryParam("access_token", userContext.getToken().getOid());
		return builder;
	}
	
	private URI createObjectLink(AppContext userContext, NavigationQuery query, Project project) {
		UriBuilder builder = 
				getPublicBaseUriBuilder().path("/rs/projects/{projectID}");
		if (query.getStyle()!=null) builder.queryParam(STYLE_PARAM, query.getStyle());
		builder.queryParam("access_token", userContext.getToken().getOid());
		return builder.build(project.getOid());
	}
	
	private URI createObjectLink(AppContext userContext, NavigationQuery query, Domain domain) {
		UriBuilder builder = 
				getPublicBaseUriBuilder().path("/rs/projects/{projectID}/domains/{domainID}");
		if (query.getStyle()!=null) builder.queryParam(STYLE_PARAM, query.getStyle());
		builder.queryParam("access_token", userContext.getToken().getOid());
		return builder.build(domain.getId().getProjectId(), domain.getOid());
	}
	
	private URI createObjectLink(AppContext userContext, NavigationQuery query, Bookmark bookmark) {
		UriBuilder builder = 
				getPublicBaseUriBuilder().path("/rs/projects/{projectID}/bookmarks/{domainID}");
		if (query.getStyle()!=null) builder.queryParam(STYLE_PARAM, query.getStyle());
		builder.queryParam("access_token", userContext.getToken().getOid());
		return builder.build(bookmark.getId().getProjectId(), bookmark.getOid());
	}
	
	/**
	 * @param userContext
	 * @param projectRef
	 * @return
	 * @throws ScopeException 
	 */
	private Project findProject(AppContext userContext, String projectRef) throws ScopeException {
		if (projectRef.startsWith("@")) {
			// using ID
			String projectId = projectRef.substring(1);
			ProjectPK projectPk = new ProjectPK(userContext.getClientId(), projectId);
			return ProjectManager.INSTANCE.getProject(userContext, projectPk);
		} else {
			// using name
			List<Project> projects = ((ProjectDAO) DAOFactory.getDAOFactory().getDAO(Project.class))
					.findByCustomer(userContext, userContext.getCustomerPk());
			for (Project project : projects) {
				if (project.getName()!=null && project.getName().equals(projectRef)) {
					return project;
				}
			}
			throw new ScopeException("cannot find project with name='"+projectRef+"'");
		}
	}

	/**
	 * list the MyBookmarks content (bookmarks and folders)
	 * @param userContext
	 * @param query 
	 * @param parent
	 * @param isFlat
	 * @param content
	 * @return the parent folder
	 * @throws ScopeException
	 */
	private NavigationItem listMyBoomarks(AppContext userContext, NavigationQuery query, String parent, String[] filters, HierarchyMode hierarchyMode, List<NavigationItem> content) throws ScopeException {
		// list mybookmark related resources
		String fullPath = BookmarkManager.INSTANCE.getMyBookmarkPath(userContext);
		NavigationItem parentFolder = null;
		if (parent.equals(MYBOOKMARKS_FOLDER.getSelfRef())) {
			// just keep the fullpath
			parentFolder = createLinkableFolder(userContext, query, MYBOOKMARKS_FOLDER);
		} else {
			// add the remaining path to fullpath
			fullPath += parent.substring(MYBOOKMARKS_FOLDER.getSelfRef().length());
			String name = parent.substring(parent.lastIndexOf("/"));
			String grandParent = parent.substring(0, parent.lastIndexOf("/"));
			parentFolder = new NavigationItem(name, "", grandParent, parent, NavigationItem.FOLDER_TYPE);
		}
		listBoomarks(userContext, query, parent, filters, hierarchyMode, fullPath, content);
		return parentFolder;
	}
	
	/**
	 * List the Shared bookmarks and folders
	 * @param userContext
	 * @param query 
	 * @param parent
	 * @param isFlat
	 * @param content
	 * @return the parent folder
	 * @throws ScopeException
	 */
	private NavigationItem listSharedBoomarks(AppContext userContext, NavigationQuery query, String parent, String[] filters, HierarchyMode hierarchyMode, List<NavigationItem> content) throws ScopeException {
		// list mybookmark related resources
		String fullPath = Bookmark.SEPARATOR + Bookmark.Folder.SHARED;
		NavigationItem parentFolder = null;
		if (parent.equals(SHARED_FOLDER.getSelfRef())) {
			parentFolder = createLinkableFolder(userContext, query, SHARED_FOLDER);
		} else {
			// add the remaining path to fullpath
			fullPath += parent.substring(SHARED_FOLDER.getSelfRef().length());
			String name = parent.substring(parent.lastIndexOf("/"));
			String grandParent = parent.substring(0, parent.lastIndexOf("/"));
			parentFolder = new NavigationItem(name, "", grandParent, parent, NavigationItem.FOLDER_TYPE);
		}
		listBoomarks(userContext, query, parent, filters, hierarchyMode, fullPath, content);
		return parentFolder;
	}

	/**
	 * 
	 * @param userContext
	 * @param query
	 * @param parent
	 * @param filters
	 * @param hierarchyMode
	 * @param fullPath
	 * @param content
	 * @throws ScopeException
	 */
	private void listBoomarks(AppContext userContext, NavigationQuery query, String parent, String[] filters, HierarchyMode hierarchyMode, String fullPath, List<NavigationItem> content) throws ScopeException {
		// list the content first
		List<Bookmark> bookmarks = BookmarkManager.INSTANCE.findBookmarksByParent(userContext, fullPath);
		HashSet<String> folders = new HashSet<>();
		for (Bookmark bookmark : bookmarks) {
			Project project = ProjectManager.INSTANCE.getProject(userContext, bookmark.getId().getParent());
			String path = bookmark.getPath();
			// only handle the exact path
			boolean checkParent = (hierarchyMode==HierarchyMode.FLAT)?path.startsWith(fullPath):path.equals(fullPath);
			if (checkParent) {
				if (filters==null || filter(bookmark, project, filters)) {
					String actualParent = parent;
					if (hierarchyMode==HierarchyMode.FLAT) {
						actualParent = (parent + path.substring(fullPath.length()));
					}
					NavigationItem item = new NavigationItem(query, project, bookmark, actualParent);
					if (query.getStyle()==Style.HUMAN || query.getStyle()==Style.HTML) {
						item.setLink(createLinkToAnalysis(userContext, query, item));
						item.setViewLink(createLinkToView(userContext, query, item));
						item.setObjectLink(createObjectLink(userContext, query, bookmark));
					}
					HashMap<String, String> attrs = new HashMap<>();
					attrs.put("project", project.getName());
					item.setAttributes(attrs);
					content.add(item);
				}
			} else {
				// it's a sub folder
				path = bookmark.getPath().substring(fullPath.length()+1);// remove first /
				String[] split = path.split("/");
				if (split.length>0) {
					String name = "/"+(hierarchyMode!=null?path:split[0]);
					String selfpath = parent+name;
					if (!folders.contains(selfpath)) {
						if (filters==null || filter(name, filters)) {
							String oid = Base64
									.encodeBase64URLSafeString(selfpath.getBytes());
							// legacy folder PK support
							BookmarkFolderPK id = new BookmarkFolderPK(bookmark.getId().getCustomerId(), oid);
							NavigationItem folder = new NavigationItem(query.getStyle()==Style.LEGACY?id:null, name, "", parent, selfpath, NavigationItem.FOLDER_TYPE);
							if (query.getStyle()==Style.HUMAN || query.getStyle()==Style.HTML) {
								folder.setLink(createLinkToFolder(userContext, query, folder));
							}
							content.add(folder);
							folders.add(selfpath);
						}
					}
				}
			}
		}
	}
	
	private boolean filter(Project project, String[] filters) {
		final String name = project.getName();
		final String description = project.getDescription();
		final String attr = project.getDbUrl();
		for (String filter : filters) {
			if (name==null || !name.toLowerCase().contains(filter)) {
				if (description==null || !description.toLowerCase().contains(filter)) {
					if (attr==null || !attr.toLowerCase().contains(filter)) {
						return false;
					}
				}
			}
		}
		return true;
	}
	
	private boolean filter(Bookmark bookmark, Project project, String[] filters) {
		final String name = bookmark.getName();
		final String description = bookmark.getDescription();
		final String attr = project.getName();
		for (String filter : filters) {
			if (name==null || !name.toLowerCase().contains(filter)) {
				if (description==null || !description.toLowerCase().contains(filter)) {
					if (attr==null || !attr.toLowerCase().contains(filter)) {
						return false;
					}
				}
			}
		}
		return true;
	}
	
	private boolean filter(String name, String[] filters) {
		if (name==null) return false;
		for (String filter : filters) {
			if (!name.toLowerCase().contains(filter)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * @param userContext
	 * @param query
	 * @param parent
	 * @param parent2 
	 * @return
	 */
	public Bookmark createBookmark(AppContext userContext, AnalyticsQuery query, String BBID, String name, String parent) {
		try {
			Space space = getSpace(userContext, BBID);
			if (query==null) {
				throw new APIException("undefined query");
			}
			if (query.getDomain()==null || query.getDomain()=="") {
				throw new APIException("undefined domain");
			}
			String domainID = "@'"+space.getDomain().getOid()+"'";
			if (!query.getDomain().equals(domainID)) {
				throw new APIException("invalid domain definition for the query, doesn't not match the REFERENCE");
			}
			Bookmark bookmark = new Bookmark();
			BookmarkPK bookmarkPK = new BookmarkPK(space.getUniverse().getProject().getId());
			bookmark.setId(bookmarkPK);
			BookmarkConfig config = createBookmarkConfig(space, query);
			ObjectMapper mapper = new ObjectMapper();
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			String json = mapper.writeValueAsString(config);
			JsonNode tree = mapper.readTree(json);
			bookmark.setConfig(tree);
			bookmark.setDescription("created using the super cool new Bookmark API");
			bookmark.setName(name);
			String path = "";
			if (parent.startsWith(MYBOOKMARKS_FOLDER.getSelfRef())) {
				path = parent.substring(MYBOOKMARKS_FOLDER.getSelfRef().length());
				path = BookmarkManager.INSTANCE.getMyBookmarkPath(userContext)+path;
			} else if (parent.startsWith(SHARED_FOLDER.getSelfRef())) {
				path = parent.substring(SHARED_FOLDER.getSelfRef().length());
				path += Bookmark.SEPARATOR + Bookmark.Folder.SHARED + path;
			} else if (!parent.startsWith("/")) {
				path = BookmarkManager.INSTANCE.getMyBookmarkPath(userContext)+"/"+parent;
			} else {
				throw new ObjectNotFoundAPIException("unable to save a bookmark in this path: "+parent, true);
			}
			bookmark.setPath(path);
			//
			BookmarkServiceBaseImpl.getInstance().store(userContext, bookmark);
			return bookmark;
		} catch (IOException e) {
			throw new APIException("cannot create the bookmark: JSON error: "+e.getMessage());
		} catch (ScopeException e) {
			throw new ObjectNotFoundAPIException("invalid REFERENCE :" + e.getMessage(), true);
		}
	}
	
	public Object getItem(AppContext userContext, String BBID) throws ScopeException {
		Space space = getSpace(userContext, BBID);
		if (space.getParent()!=null) {
			throw new ScopeException("invalid ID");
		}
		if (space.hasBookmark()) {
			return space.getBookmark();
		} else {
			return space.getDomain();
		}
	}
	
	private Space getSpace(AppContext userContext, String BBID) {
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
	
	public Facet getFacet(
			AppContext userContext, 
			String BBID,
			String facetId,
			String search,
			String[] filters, 
			Integer maxResults,
			Integer startIndex,
			Integer timeoutMs
			) throws ComputingException {
		try {
			Space space = getSpace(userContext, BBID);
			Domain domain = space.getDomain();
			//
			AnalyticsQuery query = new AnalyticsQueryImpl();
			if ((filters != null) && (filters.length > 0)) {
				query.setFilters(Arrays.asList(filters));
			}
			//
			ProjectFacetJob job = new ProjectFacetJob();
			job.setDomain(Collections.singletonList(domain.getId()));
			job.setCustomerId(userContext.getCustomerId());
			//
			BookmarkConfig config = null;
			if (space.hasBookmark()) {
				config = BookmarkManager.INSTANCE.readConfig(space.getBookmark());
			}
			// merge the config with the query
			mergeBoomarkConfig(space, query, config);
			// extract the facet selection
			FacetSelection selection = createFacetSelection(space, query);
			job.setSelection(selection);
			//
			Universe universe = space.getUniverse();
			List<Domain> domains = Collections.singletonList(domain);
			DashboardSelection sel = EngineUtils.getInstance().applyFacetSelection(userContext,
					universe, domains, job.getSelection());
			//return ComputingService.INSTANCE.glitterFacet(universe, domain, ds, axis, filter, startIndex, maxResults, timeoutMs);
			if (SegmentManager.isSegmentFacet(facetId)) {
				DomainHierarchy hierarchy = universe
						.getDomainHierarchy(domain, true);
				return SegmentManager.createSegmentFacet(universe, hierarchy, domain,
						facetId, search, maxResults, startIndex, sel);
			} else {
				/*
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
				*/
				// using the bookmark scope instead
				SpaceScope scope = new SpaceScope(universe.S(domain));
				ExpressionAST expr = scope.parseExpression(facetId);
				Axis axis = universe.asAxis(expr);
				if (axis==null) {
					throw new ScopeException("the expression '" + facetId + "' doesn't resolve to an valid facet in this bookmark scope");
				}
				//
				Facet facet = ComputingService.INSTANCE.glitterFacet(universe,
						domain, sel, axis, search,
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
	
	public Response scopeAnalysis(
			AppContext userContext, 
			String BBID,
			String value,
			Integer offset,
			ObjectType[] types,
			ValueType[] values, 
			Style style
			) throws ScopeException
	{
		if (value==null) value="";
		Space space = getSpace(userContext, BBID);
		//
		DomainExpressionScope scope = new DomainExpressionScope(space.getUniverse(), space.getDomain());
		//SpaceScope scope = new SpaceScope(space);
		//
		ExpressionSuggestionHandler handler = new ExpressionSuggestionHandler(
				scope);
		if (offset == null) {
			offset = value.length()+1;
		}
		Collection<ObjectType> typeFilters = null;
		if (types!=null) {
			if (types.length>1) {
				typeFilters = new HashSet<>(Arrays.asList(types));
			} else if (types.length==1) {
				typeFilters = Collections.singletonList(types[0]);
			}
		}
		Collection<ValueType> valueFilters = null;
		if (values!=null) {
			if (values.length>1) {
				valueFilters = new HashSet<>(Arrays.asList(values));
			} else if (values.length==1) {
				valueFilters = Collections.singletonList(values[0]);
			}
		}
		ExpressionSuggestion suggestions = handler.getSuggestion(value, offset, typeFilters, valueFilters);
		if (style==Style.HTML) {
			return createHTMLPageScope(space, suggestions, BBID, value, types, values);
		} else {
			return Response.ok(suggestions).build();
		}
	}

	public Response runAnalysis(
			final AppContext userContext,
			String BBID,
			final AnalyticsQuery query, 
			String data,
			String envelope,
			Integer timeout
			)
	{
		Space space = null;// if we can initialize it, fine to report in the catch block
		try {
			//
			if (envelope==null) {
				envelope = computeEnvelope(query);
			}
			//
			space = getSpace(userContext, BBID);
			//
			Bookmark bookmark = space.getBookmark();
			BookmarkConfig config = BookmarkManager.INSTANCE.readConfig(bookmark);
			//
			// merge the bookmark config with the query
			mergeBoomarkConfig(space, query, config);
			if (query.getLimit()==null) {
				query.setLimit((long) 100);
			}
			// create the facet selection
			FacetSelection selection = createFacetSelection(space, query);
			final ProjectAnalysisJob job = createAnalysisJob(space.getUniverse(), query, selection, OutputFormat.JSON);
			//
			final boolean lazyFlag = (query.getLazy() != null) && (query.getLazy().equals("true") || query.getLazy().equals("noError"));
			//
			// create the AnalysisResult
			AnalyticsReply reply = new AnalyticsReply();
			if (query.getStyle()==Style.LEGACY) {
				// legacy may use the facetSelection
				reply.setSelection(selection);
			}
			reply.setQuery(query);
			//
			if (data==null || data.equals("")) data="LEGACY";
			if (data.equalsIgnoreCase("SQL")) {
				// bypassing the ComputingService
				AnalysisJobComputer computer = new AnalysisJobComputer();
				String sql = computer.viewSQL(userContext, job);
				reply.setResult(sql);
			} else {
				if (query.getStyle()==Style.HTML) {
					// change data format to legacy
					data="LEGACY";
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
					if (data==null || data.equals("") || data.equalsIgnoreCase("LEGACY")) {
						DataTable legacy = matrix.toDataTable(userContext, query.getMaxResults(), query.getStartIndex(), false, null);
						reply.setResult(legacy);
					} else {
						IDataMatrixConverter<Object[]> converter = getConverter(data);
						AnalyticsResult result = new AnalyticsResult();
						Object[] output = converter.convert(query, matrix);
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
					if (query.getStyle()==Style.HUMAN || query.getStyle()==Style.HTML) {
						// wrap the exception in a Problem
						Throwable cause = getCauseException(e);
						query.add(new Problem(Severity.ERROR, "SQL", "Failed to run the query: "+cause.getMessage(), cause));
					} else {
						// just let if go
						throwCauseException(e);
					}
				} catch (TimeoutException e) {
					if (query.getStyle()==Style.HUMAN || query.getStyle()==Style.HTML) {
						URI link = getPublicBaseUriBuilder().path("/status/{queryID}").queryParam("access_token", userContext.getToken().getOid()).build(query.getQueryID());
						throw new ComputingInProgressAPIException("computing in progress", true, timeout*2, query.getQueryID(), link);
					} else {
						throw new ComputingInProgressAPIException("computing in progress", true, timeout*2, query.getQueryID());
					}
				}
			}
			if (query.getStyle()==Style.HTML && data.equalsIgnoreCase("SQL")) {
					return createHTMLsql(reply.getResult().toString());
			} else if (query.getStyle()==Style.HTML && data.equalsIgnoreCase("LEGACY")) {
					return createHTMLPageTable(userContext, space, query, (DataTable)reply.getResult());
			} else if (envelope.equalsIgnoreCase("ALL")) {
				return Response.ok(reply).build();
			} else if (envelope.equalsIgnoreCase("RESULT")) {
				return Response.ok(reply.getResult()).build();
			} else if (envelope.equalsIgnoreCase("DATA")) {
				if (reply.getResult() instanceof AnalyticsResult) {
					return Response.ok(((AnalyticsResult)reply.getResult()).getData()).build();
				} else if (reply.getResult() instanceof DataTable) {
					return Response.ok(((DataTable)reply.getResult()).getRows()).build();
				} else {
					// return result instead
					return Response.ok(reply.getResult()).build();
				}
			} 
			//else
			return Response.ok(reply).build();
		} catch (DatabaseServiceException | ComputingException | InterruptedException | ScopeException | SQLScopeException | RenderingException e) {
			if (query.getStyle()==Style.HTML) {
				query.add(new Problem(Severity.ERROR, "query", "unable to run the query, fatal error: " + e.getMessage(), e));
				return createHTMLPageTable(userContext, space, query, null);
			} else {
				throw new APIException(e.getMessage(), true);
			}
		}
	}
	
	/**
	 * Try to find the most relevant exception in the stack
	 * @param the execution exception
	 */
	private void throwCauseException(ExecutionException e) {
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

	private String computeEnvelope(AnalyticsQuery query) {
		if (query.getStyle()==Style.HUMAN || query.getStyle()==Style.HTML) {
			return "ALL";
		} else if (query.getStyle()==Style.LEGACY) {
			return "RESULT";
		} else {//MACHINE
			return "ALL";
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

	/**
	 * @param userContext 
	 * @param dataTable 
	 * @return
	 */
	private Response createHTMLPageTable(AppContext userContext, Space space, AnalyticsQuery query, DataTable data) {
		String title = space!=null?getPageTitle(space):null;
		StringBuilder html = createHTMLHeader("Query: "+title);
		createHTMLtitle(html, title, query.getBBID(), getParentLink(space));
		createHTMLproblems(html, query.getProblems());
		if (data!=null) {
			html.append("<table class='data'><tr>");
			html.append("<th></th>");
			for (Col col : data.getCols()) {
				html.append("<th>"+col.getName()+"</th>");
			}
			html.append("</tr>");
			int i=1;
			if (query.getStartIndex()!=null) {
				i=query.getStartIndex()+1;
			}
			for (Row row : data.getRows()) {
				html.append("<tr>");
				html.append("<td valign='top'>#"+(i++)+"</td>");
				for (Col col : data.getCols()) {
					Object value = row.getV()[col.getPos()];
					if (value!=null) {
						if (col.getFormat()!=null && col.getFormat().length()>0) {
							try {
								value = String.format(col.getFormat(), value);
							} catch (IllegalFormatException e) {
								// ignore
							}
						}
					} else {
						value="";
					}
					html.append("<td valign='top'>"+value+"</td>");
				}
				html.append("</tr>");
			}
			html.append("</table>");
			createHTMLpagination(html, query, data);
		} else {
			html.append("<i>Result is not available, it's probably due to an error</i>");
			html.append("<p>");
			createHTMLdataLinks(html, query);
			html.append("</p<br>");
		}
		html.append("<form>");
		createHTMLfilters(html, query);
		html.append("<table>");
		html.append("<tr><td valign='top'>groupBy</td><td>");
		createHTMLinputArray(html, "text", "groupBy", query.getGroupBy());
		html.append("</td><td valign='top'><p><i>Define the group-by facets to apply to results. Facet can be defined using it's ID or any valid expression. If empty, the subject default parameters will apply. You can use the * token to extend the subject default parameters.</i></p>");
		html.append("</td></tr>");
		html.append("<tr><td valign='top'>metrics</td><td>");
		createHTMLinputArray(html, "text", "metrics", query.getMetrics());
		html.append("</td><td valign='top'><p><i>Define the metrics to compute. Metric can be defined using it's ID or any valid expression. If empty, the subject default parameters will apply. You can use the * token to extend the subject default parameters.</i></p>");
		html.append("</td></tr>");
		html.append("<tr><td valign='top'>orderBy</td><td>");
		createHTMLinputArray(html, "text", "orderBy", query.getOrderBy());
		html.append("</td></tr>");
		html.append("<tr><td>limit</td><td>");
		html.append("<input type=\"text\" name=\"limit\" value=\""+getFieldValue(query.getLimit(),0)+"\">");
		html.append("</td></tr>");
		html.append("<tr><td>maxResults</td><td>");
		html.append("<input type=\"text\" name=\"maxResults\" value=\""+getFieldValue(query.getMaxResults(),100)+"\">");
		html.append("</td></tr>");
		html.append("<tr><td>startIndex</td><td>");
		html.append("<input type=\"text\" name=\"startIndex\" value=\""+getFieldValue(query.getStartIndex(),0)+"\"></td><td><i>index is zero-based, so use the #count of the last row to view the next page</i>");
		html.append("</td></tr>");
		html.append("</table>"
				+ "<input type=\"hidden\" name=\"style\" value=\"HTML\">"
				+ "<input type=\"hidden\" name=\"access_token\" value=\""+userContext.getToken().getOid()+"\">"
				+ "<input type=\"submit\" value=\"Refresh\">"
				+ "</form>");
		if (space!=null) createHTMLscope(html, space, query);
		createHTMLAPIpanel(html, "runAnalysis");
		html.append("</body></html>");
		return Response.ok(html.toString(),"text/html").build();
	}
	
	private void createHTMLinputArray(StringBuilder html, String type, String name, List<? extends Object> values) {
		if (values==null || values.isEmpty()) {
			html.append("<input type=\""+type+"\" size=100 name=\""+name+"\" value=\"\" placeholder=\"type formula\">");
		} else {
			boolean first = true;
			for (Object value : values) {
				if (!first) html.append("<br>"); else first=false;
				html.append("<input type=\""+type+"\" size=100 name=\""+name+"\" value=\""+getFieldValue(value.toString())+"\">");
			}
			if (!first) html.append("<br>");
			html.append("<input type=\""+type+"\" size=100 name=\""+name+"\" value=\"\" placeholder=\"type formula\">");
		}
	}
	
	private void createHTMLpagination(StringBuilder html, AnalyticsQuery query, DataTable data) {
		long lastRow = (data.getStartIndex()+data.getRows().size());
		long firstRow = data.getRows().size()>0?(data.getStartIndex()+1):0;
		html.append("<br><div>rows from "+firstRow+" to "+lastRow+" out of "+data.getTotalSize()+" records");
		if (data.getFullset()) {
			html.append(" (the query is complete)");
		} else {
			html.append(" (the query has more data)");
		}
		if (lastRow<data.getTotalSize()) {
			// go to next page
			HashMap<String, Object> override = new HashMap<>();
			override.put(START_INDEX_PARAM, lastRow);
			URI nextLink = buildAnalyticsQueryURI(userContext, query, null, null, Style.HTML, override);
			html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(nextLink.toString())+"\">next</a>]");
		}
		html.append("</div><div>");
		if (data.isFromSmartCache()) {
			html.append("data from smart-cache, last computed "+data.getExecutionDate());
		} else if (data.isFromCache()) {
			html.append("data from cache, last computed "+data.getExecutionDate());
		} else {
			html.append("fresh data just computed at "+data.getExecutionDate());
		}
		// add links
		createHTMLdataLinks(html, query);
		html.append("</div><br>");
	}
	
	private void createHTMLdataLinks(StringBuilder html, AnalyticsQuery query) {
		// add links
		{ // for View
			HashMap<String, Object> override = new HashMap<>();
			override.put(LIMIT_PARAM, null);
			override.put(MAX_RESULTS_PARAM, null);
			URI sqlLink = buildAnalyticsViewURI(userContext, new ViewQuery(query), null, "ALL", Style.HTML, override);//(userContext, query, "SQL", null, Style.HTML, null);
			html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(sqlLink.toString())+"\">View</a>]");
		}
		{ // for SQL
			URI sqlLink = buildAnalyticsQueryURI(userContext, query, "SQL", null, Style.HTML, null);
			html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(sqlLink.toString())+"\">SQL</a>]");
		}
		{ // for CSV export
			URI csvExport = buildAnalyticsExportURI(userContext, query, ".csv");
			html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(csvExport.toString())+"\">Export CSV</a>]");
		}
		{ // for XLS export
			URI xlsExport = buildAnalyticsExportURI(userContext, query, ".xls");
			html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(xlsExport.toString())+"\">Export XLS</a>]");
		}
	}
	
	private void createHTMLpagination(StringBuilder html, ViewQuery query, Info info) {
		long lastRow = (info.getStartIndex()+info.getPageSize());
		long firstRow = info.getTotalSize()>0?(info.getStartIndex()+1):0;
		html.append("<br><div>rows from "+firstRow+" to "+lastRow+" out of "+info.getTotalSize()+" records");
		if (info.isComplete()) {
			html.append(" (the query is complete)");
		} else {
			html.append(" (the query has more data)");
		}
		if (lastRow<info.getTotalSize()) {
			// go to next page
			HashMap<String, Object> override = new HashMap<>();
			override.put(START_INDEX_PARAM, lastRow);
			URI nextLink = buildAnalyticsViewURI(userContext, query, null, null, Style.HTML, override);
			html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(nextLink.toString())+"\">next</a>]");
		}
		html.append("</p>");
		if (info.isFromSmartCache()) {
			html.append("<p>data from smart-cache, last computed "+info.getExecutionDate()+"</p>");
		} else if (info.isFromCache()) {
			html.append("<p>data from cache, last computed "+info.getExecutionDate()+"</p>");
		} else {
			html.append("<p>fresh data just computed at "+info.getExecutionDate()+"</p>");
		}
	}

	/**
	 * @param string
	 * @return
	 */
	private Response createHTMLsql(String sql) {
		StringBuilder html = new StringBuilder("<html><head>");
		html.append(
				"<script src='https://cdn.rawgit.com/google/code-prettify/master/loader/run_prettify.js?lang=sql'></script>");
		html.append("</head><body>");
		html.append(
				"<pre class='prettyprint lang-sql' style='white-space: pre-wrap;white-space: -moz-pre-wrap;white-space: -pre-wrap;white-space: -o-pre-wrap;word-wrap: break-word;padding:0px;margin:0px'>");
		html.append(StringEscapeUtils.escapeHtml4(sql));
		html.append("</pre>");
		html.append("</body></html>");
		return Response.ok(html.toString(),"text/html").build();
	}

	public Response exportAnalysis(
			final AppContext userContext,
			String BBID,
			final AnalyticsQuery query,
			final String filename,
			String fileext,
			String compression
			)
	{
		try {
			Space space = getSpace(userContext, BBID);
			//
			Bookmark bookmark = space.getBookmark();
			BookmarkConfig config = BookmarkManager.INSTANCE.readConfig(bookmark);
			//
			// merge the bookmark config with the query
			mergeBoomarkConfig(space, query, config);
			// create the facet selection
			FacetSelection selection = createFacetSelection(space, query);
			final ProjectAnalysisJob job = createAnalysisJob(space.getUniverse(), query, selection, OutputFormat.JSON);
			//
			final OutputFormat outFormat;
			if (fileext == null) {
				outFormat = OutputFormat.CSV;
			} else {
				outFormat = OutputFormat.valueOf(fileext.toUpperCase());
			}

			final OutputCompression outCompression;
			if (compression == null) {
				outCompression = OutputCompression.NONE;
			} else {
				outCompression = OutputCompression.valueOf(compression.toUpperCase());
			}
			
			final ExportSourceWriter writer = getWriter(outFormat);
			if (writer==null) {
				throw new APIException("unable to handle the format='"+outFormat+"'", true);
			}

			StreamingOutput stream = new StreamingOutput() {
				@Override
				public void write(OutputStream os) throws IOException, WebApplicationException {
					try {
						if (outCompression == OutputCompression.GZIP) {
							try {
								os = new GZIPOutputStream(os);
							} catch (IOException e) {
								throw new RuntimeException(e);
							}
						}
						AnalysisJobComputer.INSTANCE.compute(userContext, job, os, writer, false);
					} catch (InterruptedException | ComputingException e) {
						throw new IOException(e);
					}
				}
			};

			// build the response
			ResponseBuilder response;
			String outname = filename;
			if (filename==null || filename.equals("")) {
				outname = "job-" + (space.hasBookmark()?space.getBookmark().getName():space.getRoot().getName());
			}
			String mediaType;
			switch (outFormat) {
			case CSV:
				mediaType = "text/csv";
				outname += "."+(fileext!=null?fileext:"csv");
				break;
			case XLS:
				mediaType = "application/vnd.ms-excel";
				outname += "."+(fileext!=null?fileext:"xls");
				break;
			case XLSX:
				mediaType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
				outname += "."+(fileext!=null?fileext:"xlsx");
				break;
			default:
				mediaType = MediaType.APPLICATION_JSON_TYPE.toString();
				outname += "."+(fileext!=null?fileext:"json");
			}

			switch (outCompression) {
			case GZIP:
				// note : setting "Content-Type:application/octet-stream" should
				// disable interceptor's GZIP compression.
				mediaType = MediaType.APPLICATION_OCTET_STREAM_TYPE.toString();
				outname += "."+(compression!=null?compression:"gz");
				break;
			default:
				// NONE
			}

			response = Response.ok(stream);
			// response.header("Content-Type", mediaType);
			if ((filename!=null && filename.length()>0) && ((outFormat != OutputFormat.JSON) || (outCompression != OutputCompression.NONE))) {
				logger.info("returning results as " + mediaType + ", fileName : " + outname);
				response.header("Content-Disposition", "attachment; filename=" + outname);
			}

			return response.type(mediaType + "; charset=UTF-8").build();
		} catch (ComputingException | InterruptedException | ScopeException e) {
			throw new APIException(e.getMessage(), true);
		}
	}
	
	private ExportSourceWriter getWriter(OutputFormat outFormat) {
		if (outFormat==OutputFormat.CSV) {
			ExportSourceWriterCSV exporter = new ExportSourceWriterCSV();
		    //settings.setQuotechar('\0');
			return exporter;
		} else if (outFormat==OutputFormat.XLS) {
			ExcelSettingsBean settings = new ExcelSettingsBean();
			settings.setExcelFile(ExcelFile.XLS);
			return new ExportSourceWriterXLSX(settings);
		} else if (outFormat==OutputFormat.XLSX) {
			ExcelSettingsBean settings = new ExcelSettingsBean();
			settings.setExcelFile(ExcelFile.XLSX);
			return new ExportSourceWriterXLSX(settings);
		} else {
			return null;
		}
	}
	
	/**
	 * @param space
	 * @param query
	 * @param config
	 * @return
	 * @throws ScopeException 
	 */
	private FacetSelection createFacetSelection(Space space, AnalyticsQuery query) throws ScopeException {
		FacetSelection selection = new FacetSelection();
		SpaceScope scope = new SpaceScope(space);
		Domain domain = space.getDomain();
		// handle period & timeframe
		if (query.getPeriod()!=null && query.getTimeframe()!=null && query.getTimeframe().size()>0) {
			ExpressionAST expr = scope.parseExpression(query.getPeriod());
			Facet facet = createFacetInterval(space, expr, query.getTimeframe());
			selection.getFacets().add(facet);
		}
		// handle compareframe
		if (query.getPeriod()!=null && query.getCompareTo()!=null && query.getCompareTo().size()>0) {
			ExpressionAST expr = scope.parseExpression(query.getPeriod());
			Facet compareFacet = createFacetInterval(space, expr, query.getCompareTo());
			selection.setCompareTo(Collections.singletonList(compareFacet));
		}
		// handle filters
		if (query.getFilters() != null) {
			Facet segment = SegmentManager.newSegmentFacet(domain);
			for (String filter : query.getFilters()) {
				filter = filter.trim();
				if (!filter.equals("")) {
					try {
						ExpressionAST filterExpr = scope.parseExpression(filter);
						if (!filterExpr.getImageDomain().isInstanceOf(IDomain.CONDITIONAL)) {
							throw new ScopeException("invalid filter, must be a condition");
						}
						Facet facet = createFacet(filterExpr);
						if (facet!=null) {
							selection.getFacets().add(facet);
						} else {
							// use open-filter
							FacetMemberString openFilter = SegmentManager.newOpenFilter(filterExpr, filter);
							segment.getSelectedItems().add(openFilter);
						}
					} catch (ScopeException e) {
						query.add(new Problem(Severity.ERROR, filter, "invalid filter definition: \n"+e.getMessage(), e));
					}
				}
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
					Object constant = ((ConstantValue)value).getValue();
					if (constant!=null) {
						facet.getSelectedItems().add(new FacetMemberString(constant.toString(), constant.toString()));
						return facet;
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
		// else
		return null;
	}
	
	private Facet createFacetInterval(Space space, ExpressionAST expr, List<String> values) throws ScopeException {
		Facet facet = new Facet();
		facet.setId(rewriteExpressionToGlobalScope(expr, space));
		String lowerbound = values.get(0);
		String upperbound = values.size()==2?values.get(1):lowerbound;
		FacetMemberInterval member = new FacetMemberInterval(lowerbound, upperbound);
		facet.getSelectedItems().add(member);
		return facet;
	}

	private ProjectAnalysisJob createAnalysisJob(Universe universe, AnalyticsQuery query, FacetSelection selection, OutputFormat format) throws ScopeException {
		// read the domain reference
		if (query.getDomain() == null) {
			throw new ScopeException("incomplete specification, you must specify the data domain expression");
		}
		Domain domain = getDomain(universe, query.getDomain());
		AccessRightsUtils.getInstance().checkRole(universe.getContext(), domain, AccessRight.Role.READ);
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
		if ((query.getGroupBy() == null || query.getGroupBy().isEmpty())
		&& (query.getMetrics() == null || query.getMetrics().isEmpty())) {
			throw new ScopeException("there is no defined facet, can't run the analysis");
		}
		// now we are going to use the domain Space scope
		// -- note that it won't limit the actual expression scope to the bookmark scope - but let's keep that for latter
		SpaceScope scope = new SpaceScope(universe.S(domain));
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
				if (image.isInstanceOf(IDomain.AGGREGATE)) {
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
							// also we must remove the sort operator to avoid nasty SQL error when generating the SQL
							if (expr.getImageDomain().isInstanceOf(DomainSort.DOMAIN) && expr instanceof Operator) {
								// remove the first operator
								Operator op = (Operator)expr;
								if (op.getArguments().size()==1 
										&& (op.getOperatorDefinition().getExtendedID()==SortOperatorDefinition.ASC_ID
										|| op.getOperatorDefinition().getExtendedID()==SortOperatorDefinition.DESC_ID)) 
								{
									expr = op.getArguments().get(0);
								}
							}
							String universalExpression = rewriteExpressionToGlobalScope(expr, root);
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
		if (query.getRollups() != null) {
			for (RollUp rollup : query.getRollups()) {
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
		analysisJob.setRollups(query.getRollups());
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
				}
				if (x==null || x<0) {
					throw new ScopeException("invalid beyondLimit parameter: "+value+": must be an valid integer position or a groupBy expression");
				}
				indexes.add(new Index(x));
			}
			analysisJob.setBeyondLimit(indexes);
		}
		return analysisJob;
	}
	
	private Integer getIntegerValue(String value) {
		try {
			return Integer.parseInt(value);
		} catch (NumberFormatException e) {
			return null;
		}
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
		// no desc | asc operator provided: use default
		if (domain.isInstanceOf(IDomain.NUMERIC) || domain.isInstanceOf(IDomain.TEMPORAL)) {
			return Direction.DESC;
		} else { //if (image.isInstanceOf(IDomain.STRING)) {
			return Direction.ASC;
		} 
	}
	
	private BookmarkConfig createBookmarkConfig(Space space, AnalyticsQuery query) throws ScopeException {
		SpaceScope scope = new SpaceScope(space);
		BookmarkConfig config = new BookmarkConfig();
		// config use the Domain OID
		config.setDomain(space.getDomain().getOid());
		//config.setSelection();
		config.setLimit(query.getLimit());
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
				// add the domain scope
				ExpressionAST expr = scope.parseExpression(facet);
				choosenMetrics.add(rewriteExpressionToGlobalScope(expr, space));
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
				OrderBy copy = new OrderBy(new Expression(rewriteExpressionToGlobalScope(expr, space)), direction);
				config.getOrderBy().add(copy);
			}
		}
		//
		if (query.getRollups() != null) {
			config.setRollups(query.getRollups());
		}
		// add the selection
		FacetSelection selection = createFacetSelection(space, query);
		config.setSelection(selection);
		return config;
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
	private void mergeBoomarkConfig(Space space, AnalyticsQuery query, BookmarkConfig config) throws ScopeException, ComputingException, InterruptedException {
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
						if (index.getStatus()==Status.DONE) {
							query.setTimeframe(Collections.singletonList("__CURRENT_MONTH"));
						} else {
							query.setTimeframe(Collections.singletonList("__ALL"));
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
			} else if (config.getChosenDimensions() != null) {
				for (String chosenDimension : config.getChosenDimensions()) {
					try {
						String f = null;
						if (chosenDimension.startsWith("@")) {
							// need to fix the scope
							ExpressionAST expr = globalScope.parseExpression(chosenDimension);
							f = expr.prettyPrint(localOptions);//rewriteExpressionToLocalScope(expr, space);
						} else {
							// legacy support raw ID
							// parse to validate and apply prettyPrint options
							ExpressionAST expr = localScope.parseExpression("@'" + chosenDimension + "'");
							f = expr.prettyPrint(localOptions);
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
				boolean someIntrinsicMetric = false;
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
				if (!someIntrinsicMetric) {
					metrics.add("count() // default metric");
				}
			} else if (config.getChosenMetrics() != null) {
				for (String chosenMetric : config.getChosenMetrics()) {
					// parse to validate and reprint
					try {
						ExpressionAST expr = localScope.parseExpression("@'" + chosenMetric + "'");
						metrics.add(expr.prettyPrint(localOptions));
					} catch (ScopeException e) {
						query.add(new Problem(Severity.WARNING, chosenMetric, "failed to parse bookmark metric: " + e.getMessage(), e));
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
			if (config!=null) {
				query.setRollups(config.getRollups());
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
		if (!selection.getFacets().isEmpty()) {// always iterate over selection at least to capture the period
			boolean keepConfig = filterWildcard || filters.isEmpty();
			String period = null;
			if (query.getPeriod()!=null && query.getTimeframe()==null) {
				ExpressionAST expr = localScope.parseExpression(query.getPeriod());
				period = expr.prettyPrint(new PrettyPrintOptions(ReferenceStyle.IDENTIFIER, null));
			}
			// look for the selection
			for (Facet facet : selection.getFacets()) {
				if (!facet.getSelectedItems().isEmpty()) {
					if (facet.getId().equals(period)) {
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
		}
		query.setFilters(filters);
		//
		// check timeframe again
		if (query.getPeriod()!=null && (query.getTimeframe()==null || query.getTimeframe().size()==0)) {
			// add a default timeframe
			query.setTimeframe(Collections.singletonList("__CURRENT_MONTH"));
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
			return expr.prettyPrint();
		}
	}

	/**
	 * @param uriInfo 
	 * @param userContext
	 * @param bBID
	 * @param x
	 * @param y
	 * @param color
	 * @param style 
	 * @param query
	 * @return
	 * @throws InterruptedException 
	 * @throws ComputingException 
	 * @throws ScopeException 
	 */
	public Response viewAnalysis(
			final AppContext userContext, 
			String BBID,
			ViewQuery view,
			String data,
			Style style, 
			String envelope) throws ScopeException, ComputingException, InterruptedException {
		Space space = getSpace(userContext, BBID);
		//
		if (data==null) data=style==Style.HTML?"EMBEDED":"URL";
		boolean preFetch = true;// default to prefetch when data mode is URL
		//
		Bookmark bookmark = space.getBookmark();
		BookmarkConfig config = BookmarkManager.INSTANCE.readConfig(bookmark);
		//
		// handle the limit
		Long explicitLimit = view.getLimit();
		AnalyticsQueryImpl query = new AnalyticsQueryImpl(view);
		// merge the bookmark config with the query
		mergeBoomarkConfig(space, query, config);
		//
		// change the query ref to use the domain one
		// - we don't want to have side-effects
		String domainBBID = "@'"+space.getUniverse().getProject().getOid()+"'.@'"+space.getDomain().getOid()+"'";
		query.setBBID(domainBBID);
		//
		VegaliteConfigurator inputConfig = new VegaliteConfigurator(space, query);
		// first check the provided parameters, because they must override the default
		inputConfig.createChannelDef("x", view.getX());
		inputConfig.createChannelDef("y", view.getY());
		inputConfig.createChannelDef("color", view.getColor());
		inputConfig.createChannelDef("size", view.getSize());
		inputConfig.createChannelDef("column", view.getColumn());
		inputConfig.createChannelDef("row", view.getRow());
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
		// use default dataviz unless x and y are already sets
		if (view.getX()==null || view.getY()==null) {
			// DOMAIN
			if (config==null) {
				// not a bookmark, use default & specs if nothing provided
				// T1935: if explicit groupBy, use it
				if (view.getGroupBy()!=null && view.getGroupBy().size()>0 && query.getGroupBy()!=null && query.getGroupBy().size()>0) {
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
				}
				if (view.getX()==null && !inputConfig.isTimeseries() && query.getPeriod()!=null) {
					view.setX("__PERIOD");
				}
				if (query.getMetrics()==null || query.getMetrics().size()==0) {
					if (!inputConfig.isHasMetric()) {
						// use count() for now
						if (view.getX()==null) {
							view.setX("count()");
						} else {
							view.setY("count()");
						}
					}
				} else {
					// display some metrics
					// - single metric
					if (query.getMetrics().size()==1) {
						if (!inputConfig.isHasMetric()) {
							if (view.getX()==null) {
								view.setX(query.getMetrics().get(0));
							} else {
								view.setY(query.getMetrics().get(0));
							}
						}
					// - multiple metrics
					} else if (view.getY()==null || view.getColor()==null || view.getColumn()==null || view.getRow()==null) {
						// set __VALUE
						if (!inputConfig.isHasMetricValue()) {
							if (view.getX()==null) {
								view.setX("__VALUE");
							} else {
								view.setY("__VALUE");
							}
						}
						// set __METRICS
						if (!inputConfig.isHasMetricSeries()) {
							if (view.getY()==null) {
								view.setY("__METRICS");
							} else if (view.getColor()==null) {
								view.setColor("__METRICS");
							} else if (view.getColumn()==null) {
								view.setColumn("__METRICS");
							} else if (view.getRow()==null) {
								view.setRow("__METRICS");
							}
						}
					}
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
		specs.encoding.x = outputConfig.createChannelDef("x", view.getX());
		specs.encoding.y = outputConfig.createChannelDef("y", view.getY());
		specs.encoding.color = outputConfig.createChannelDef("color", view.getColor());
		specs.encoding.size = outputConfig.createChannelDef("size", view.getSize());
		specs.encoding.column = outputConfig.createChannelDef("column", view.getColumn());
		specs.encoding.row = outputConfig.createChannelDef("row", view.getRow());
		//
		if (specs.encoding.x.type==DataType.nominal && specs.encoding.y.type==DataType.quantitative) {
			// auto sort
			specs.encoding.x.sort = new Sort(specs.encoding.y.field, Operation.max, Order.descending);
		} else if (specs.encoding.y.type==DataType.nominal && specs.encoding.x.type==DataType.quantitative) {
			// auto sort
			specs.encoding.y.sort = new Sort(specs.encoding.x.field, Operation.max, Order.descending);
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
			/*
			// can add page size
			if (query.getLimit()>10 && query.getMaxResults()==null) {
				query.setMaxResults(10);
			}
			*/
		}
		final int startIndex = query.getStartIndex()!=null?query.getStartIndex():0;
		final int maxResults = query.getMaxResults()!=null?query.getMaxResults():query.getLimit().intValue();
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
								&& (op.getOperatorDefinition().getExtendedID()==SortOperatorDefinition.ASC_ID
								|| op.getOperatorDefinition().getExtendedID()==SortOperatorDefinition.DESC_ID)) 
						{
							o = op.getArguments().get(0);
						}
					}
					if (o.equals(m)) {
						check = true;
					}
				}
				if (!check) {
					query.getOrderBy().add(0, "desc("+outputConfig.prettyPrint(m)+")");
				}
			}
		}
		//
		// create the facet selection
		FacetSelection selection = createFacetSelection(space, query);
		final ProjectAnalysisJob job = createAnalysisJob(space.getUniverse(), query, selection, OutputFormat.JSON);
		//
		// handling data
		AnalyticsResult.Info info = null;
		if (data.equals("EMBEDED")) {
			DataMatrix matrix = compute(userContext, job, query.getMaxResults(), query.getStartIndex(), false);
			if (!outputConfig.isHasMetricSeries()) {
				specs.data = transformToVegaData(query, matrix, "RECORDS");
			} else {
				specs.data = transformToVegaData(query, matrix, "TRANSPOSE");
			}
			int end = startIndex+maxResults;
			if (end>matrix.getRows().size()) end=matrix.getRows().size();
			info = getAnalyticsResultInfo(end-startIndex, startIndex, matrix);
		} else if (data.equals("URL")) {
			if (preFetch || style==Style.HTML) {// always prefetch if HTML
				// run the query
				Callable<DataMatrix> task = new Callable<DataMatrix>() {
					@Override
					public DataMatrix call() throws Exception {
						return compute(userContext, job, maxResults, startIndex, false);
					}
				};
				// execute the task, no need to wait for result
				Future<DataMatrix> future = ExecutionManager.INSTANCE.submit(userContext.getCustomerId(), task);
				if (style==Style.HTML) {
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
			if (!outputConfig.isHasMetricSeries()) {
				specs.data.url = buildAnalyticsQueryURI(userContext, query, "RECORDS", "DATA", null/*default style*/, null).toString();
			} else {
				specs.data.url = buildAnalyticsQueryURI(userContext, query, "TRANSPOSE", "DATA", null/*default style*/, null).toString();
			}
			specs.data.format = new Format();
			specs.data.format.type = FormatType.json;// lowercase only!
		} else {
			throw new APIException("undefined value for data parameter, must be EMBEDED or URL");
		}
		// mark
		if (outputConfig.isTimeseries()) {
			specs.mark = Mark.line;
		} else {
			specs.mark = Mark.bar;
		}
		// size
		if (specs.encoding.row==null && specs.encoding.column==null) {
			specs.config.cell = new VegaliteSpecs.Cell(640,400);
		}
		//
		ViewReply reply = new ViewReply();
		reply.setQuery(query);
		reply.setResult(specs);
		//
		if (envelope==null) {
			envelope = computeEnvelope(query);
		}
		//
		if (style!=null && style==Style.HTML) {
			return createHTMLPageView(space, view, info, reply);
		} else if (envelope==null || envelope.equals("") || envelope.equalsIgnoreCase("RESULT")) {
			return Response.ok(reply.getResult(), MediaType.APPLICATION_JSON_TYPE.toString()).build();
		} else if(envelope.equalsIgnoreCase("ALL")) {
			return Response.ok(reply, MediaType.APPLICATION_JSON_TYPE.toString()).build();
		} else {
			throw new InvalidIdAPIException("invalid parameter envelope="+envelope+", must be ALL, RESULT", true);
		}
	}
	
	/**
	 * @param matrix
	 * @return
	 */
	private Info getAnalyticsResultInfo(Integer pageSize, Integer startIndex, DataMatrix matrix) {
		AnalyticsResult.Info info = new Info();
		info.setFromCache(matrix.isFromCache());
		info.setFromSmartCache(matrix.isFromSmartCache());// actually we don't know the origin, see T1851
		info.setExecutionDate(matrix.getExecutionDate().toString());
		info.setStartIndex(startIndex);
		info.setPageSize(pageSize);
		info.setTotalSize(matrix.getRows().size());
		return info;
	}
	
	private String getBookmarkNavigationPath(Bookmark bookmark) {
		String path = bookmark.getPath();
		if (path.startsWith("/USER/")) {
			int pos = path.indexOf("/", 6);
			if (pos>=0) {
				path = path.substring(pos);
			} else {
				path = "";// remove all, the path is in form /USERS/id
			}
			path = MYBOOKMARKS_FOLDER.getSelfRef()+"/"+path;
		}
		if (path.endsWith("/")) path = path.substring(0, path.length()-1);
		return path;
	}

	private String getPageTitle(Space space) {
		if (space.hasBookmark()) {
			String path = getBookmarkNavigationPath(space.getBookmark());
			return path+"/"+space.getBookmark().getName();
		} else {
			return "/PROJECTS/"+space.getUniverse().getProject().getName()+"/"+space.getDomain().getName();
		}
	}

	private URI getParentLink(Space space) {
		if (space==null) return null;
		if (space.hasBookmark()) {
			String path = getBookmarkNavigationPath(space.getBookmark());
			return getPublicBaseUriBuilder().path("/analytics").queryParam(PARENT_PARAM, path).queryParam(STYLE_PARAM, "HTML").queryParam("access_token", userContext.getToken().getOid()).build();
		} else {
			return getPublicBaseUriBuilder().path("/analytics").queryParam(PARENT_PARAM, "/PROJECTS/"+space.getUniverse().getProject().getName()).queryParam(STYLE_PARAM, "HTML").queryParam("access_token", userContext.getToken().getOid()).build();
		}
	}
	
	private void createHTMLtitle(StringBuilder html, String title, String BBID, URI backLink) {
		html.append("<div class=\"logo\"><span>Open Bouquet Analytics Rest <b style='color:#ee7914;'>API</b> Viewer / STYLE=HTML</span><hr/></div>");
		html.append("<h2>");
		if (title!=null) {
			html.append(title);
		} 
		if (BBID!=null) {
			html.append("&nbsp;[ID="+BBID+"]");
		}
		html.append("</h2>");
		if (backLink!=null) html.append("<a href=\""+backLink+"\"><img src='data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAACXBIWXMAAAsTAAALEwEAmpwYAAA4KGlUWHRYTUw6Y29tLmFkb2JlLnhtcAAAAAAAPD94cGFja2V0IGJlZ2luPSLvu78iIGlkPSJXNU0wTXBDZWhpSHpyZVN6TlRjemtjOWQiPz4KPHg6eG1wbWV0YSB4bWxuczp4PSJhZG9iZTpuczptZXRhLyIgeDp4bXB0az0iQWRvYmUgWE1QIENvcmUgNS42LWMwNjcgNzkuMTU3NzQ3LCAyMDE1LzAzLzMwLTIzOjQwOjQyICAgICAgICAiPgogICA8cmRmOlJERiB4bWxuczpyZGY9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkvMDIvMjItcmRmLXN5bnRheC1ucyMiPgogICAgICA8cmRmOkRlc2NyaXB0aW9uIHJkZjphYm91dD0iIgogICAgICAgICAgICB4bWxuczp4bXA9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC8iCiAgICAgICAgICAgIHhtbG5zOmRjPSJodHRwOi8vcHVybC5vcmcvZGMvZWxlbWVudHMvMS4xLyIKICAgICAgICAgICAgeG1sbnM6cGhvdG9zaG9wPSJodHRwOi8vbnMuYWRvYmUuY29tL3Bob3Rvc2hvcC8xLjAvIgogICAgICAgICAgICB4bWxuczp4bXBNTT0iaHR0cDovL25zLmFkb2JlLmNvbS94YXAvMS4wL21tLyIKICAgICAgICAgICAgeG1sbnM6c3RFdnQ9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9zVHlwZS9SZXNvdXJjZUV2ZW50IyIKICAgICAgICAgICAgeG1sbnM6dGlmZj0iaHR0cDovL25zLmFkb2JlLmNvbS90aWZmLzEuMC8iCiAgICAgICAgICAgIHhtbG5zOmV4aWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20vZXhpZi8xLjAvIj4KICAgICAgICAgPHhtcDpDcmVhdG9yVG9vbD5BZG9iZSBQaG90b3Nob3AgQ0MgMjAxNSAoTWFjaW50b3NoKTwveG1wOkNyZWF0b3JUb29sPgogICAgICAgICA8eG1wOkNyZWF0ZURhdGU+MjAxNi0wOS0xMlQyMToxNTo1MiswMjowMDwveG1wOkNyZWF0ZURhdGU+CiAgICAgICAgIDx4bXA6TW9kaWZ5RGF0ZT4yMDE2LTA5LTEyVDIxOjE3OjAxKzAyOjAwPC94bXA6TW9kaWZ5RGF0ZT4KICAgICAgICAgPHhtcDpNZXRhZGF0YURhdGU+MjAxNi0wOS0xMlQyMToxNzowMSswMjowMDwveG1wOk1ldGFkYXRhRGF0ZT4KICAgICAgICAgPGRjOmZvcm1hdD5pbWFnZS9wbmc8L2RjOmZvcm1hdD4KICAgICAgICAgPHBob3Rvc2hvcDpDb2xvck1vZGU+MzwvcGhvdG9zaG9wOkNvbG9yTW9kZT4KICAgICAgICAgPHhtcE1NOkluc3RhbmNlSUQ+eG1wLmlpZDo2YTg4Mjg1ZS0yZDNiLTQzN2YtODdmNC01MWY4NWVlZjE2NjM8L3htcE1NOkluc3RhbmNlSUQ+CiAgICAgICAgIDx4bXBNTTpEb2N1bWVudElEPnhtcC5kaWQ6NmE4ODI4NWUtMmQzYi00MzdmLTg3ZjQtNTFmODVlZWYxNjYzPC94bXBNTTpEb2N1bWVudElEPgogICAgICAgICA8eG1wTU06T3JpZ2luYWxEb2N1bWVudElEPnhtcC5kaWQ6NmE4ODI4NWUtMmQzYi00MzdmLTg3ZjQtNTFmODVlZWYxNjYzPC94bXBNTTpPcmlnaW5hbERvY3VtZW50SUQ+CiAgICAgICAgIDx4bXBNTTpIaXN0b3J5PgogICAgICAgICAgICA8cmRmOlNlcT4KICAgICAgICAgICAgICAgPHJkZjpsaSByZGY6cGFyc2VUeXBlPSJSZXNvdXJjZSI+CiAgICAgICAgICAgICAgICAgIDxzdEV2dDphY3Rpb24+Y3JlYXRlZDwvc3RFdnQ6YWN0aW9uPgogICAgICAgICAgICAgICAgICA8c3RFdnQ6aW5zdGFuY2VJRD54bXAuaWlkOjZhODgyODVlLTJkM2ItNDM3Zi04N2Y0LTUxZjg1ZWVmMTY2Mzwvc3RFdnQ6aW5zdGFuY2VJRD4KICAgICAgICAgICAgICAgICAgPHN0RXZ0OndoZW4+MjAxNi0wOS0xMlQyMToxNTo1MiswMjowMDwvc3RFdnQ6d2hlbj4KICAgICAgICAgICAgICAgICAgPHN0RXZ0OnNvZnR3YXJlQWdlbnQ+QWRvYmUgUGhvdG9zaG9wIENDIDIwMjIgKE1hY2ludG9zaCk8L3N0RXZ0OnNvZnR3YXJlQWdlbnQ+CiAgICAgICAgICAgICAgIDwvcmRmOmxpPgogICAgICAgICAgICA8L3JkZjpTZXE+CiAgICAgICAgIDwveG1wTU06SGlzdG9yeT4KICAgICAgICAgPHRpZmY6T3JpZW50YXRpb24+MTwvdGlmZjpPcmllbnRhdGlvbj4KICAgICAgICAgPHRpZmY6WFJlc29sdXRpb24+NzIwMDAwLzEwMDAwPC90aWZmOlhSZXNvbHV0aW9uPgogICAgICAgICA8dGlmZjpZUmVzb2x1dGlvbj43MjAwMDAvMTAwMDA8L3RpZmY6WVJlc29sdXRpb24+CiAgICAgICAgIDx0aWZmOlJlc29sdXRpb25Vbml0PjI8L3RpZmY6UmVzb2x1dGlvblVuaXQ+CiAgICAgICAgIDxleGlmOkNvbG9yU3BhY2U+NjU1MzU8L2V4aWY6Q29sb3JTcGFjZT4KICAgICAgICAgPGV4aWY6UGl4ZWxYRGltZW5zaW9uPjMyPC9leGlmOlBpeGVsWERpbWVuc2lvbj4KICAgICAgICAgPGV4aWY6UGl4ZWxZRGltZW5zaW9uPjMyPC9leGlmOlBpeGVsWURpbWVuc2lvbj4KICAgICAgPC9yZGY6RGVzY3JpcHRpb24+CiAgIDwvcmRmOlJERj4KPC94OnhtcG1ldGE+CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgCjw/eHBhY2tldCBlbmQ9InciPz73OULzAAAAIGNIUk0AAG11AABzoAAA/N0AAINkAABw6AAA7GgAADA+AAAQkOTsmeoAAAGFSURBVHja5Nc9SxxRFMbxJYQUqawCbqIfwCIaExNFCzuraDAQgmmCILFJ4AdpAoKVX8FKsbOxFEwVsBQsLdOICjaBdDEhL5NmFpZl3Xvvzl4tLB4Yhjnz/GfOPfecWyuKonadql07AFI0jOnEmI5KebiOCxQYu2qAOs5K8wK/8fSqAPpbzBv60wuIGPOTNuYN/cLjXAAh84Yu8KjXAHWcRphXhmh38/4lOQ/pJ55UBah3ad51dcSs9lQlVUfj4gHOe2DerGexAOM9Nm7WixiAKXzAfItm8T5g8A/LmGsT/w7Pq25EdwIAf3PvhIPlbtcJYignwF18CwCM5m5GxwGAmdwAewGAT7kBVgIAX3IDDEXUe3/uieggALCTG2Ai4i+8yT2UrkdALCS8bxwDqcT7ERAbGLkk/h4+YhdL6EsFuIXPkY3oCFvYxHZTq/+Kl92koLU0fyR2xmO8rbIG2o1uqzgsJ6FWw+9l9ayV+a68CDt1zEm8xmL5la/wELeDVXDjT8f/BwCoXjtjk0ltBwAAAABJRU5ErkJggg==\n'" + 
				">back to parent</a>");
		html.append("</div><hr>");
	}
	
	private StringBuilder createHTMLHeader(String title) {
		StringBuilder html = new StringBuilder("<html><title>List: "+title+"</title>");
		html.append("<style>"
				+ "* {font-family: \"Helvetica Neue\",Helvetica,Arial,sans-serif; color: #666; }"
				+ "table.data {border-collapse: collapse;width: 100%;}"
				+ "th, td {text-align: left;padding: 8px; vertical-align: top;}"
				+ ".data tr:nth-child(even) {background-color: #f2f2f2}"
				+ ".data th {background-color: #ee7914;color: white;}"
				+ ".vega-actions a {margin-right:10px;}"
				+ ".tooltip {\n" + 
				"    position: relative;\n" + 
				"    display: inline-block;\n" + 
				"    border-bottom: 1px dotted black;\n" + 
				"}\n" + 
				".tooltip .tooltiptext {\n" + 
				"    visibility: hidden;\n" + 
				"    width: 300px;\n" + 
				"    background-color: black;\n" + 
				"    color: #fff;\n" + 
				"    text-align: center;\n" + 
				"    border-radius: 6px;\n" + 
				"    padding: 5px 0;\n" + 
				"    position: absolute;\n" + 
				"    z-index: 1;\n" + 
				"    top: 150%;\n" + 
				"    left: 50%;\n" + 
				"    margin-left: -30px;\n" + 
				"}\n" + 
				".tooltip .tooltiptext::after {\n" + 
				"    content: \"\";\n" + 
				"    position: absolute;\n" + 
				"    bottom: 100%;\n" + 
				"    left: 50%;\n" + 
				"    margin-left: -5px;\n" + 
				"    border-width: 5px;\n" + 
				"    border-style: solid;\n" + 
				"    border-color: transparent transparent black transparent;\n" + 
				"}\n" + 
				".tooltip:hover .tooltiptext {\n" + 
				"    visibility: visible;\n" + 
				"}"
				+ "hr {border: none; "
				+ "color: Gainsboro ;\n" + 
				"background-color: Gainsboro ;\n" + 
				"height: 3px;}"
				+ "input[type=date], input[type=text], select {\n" + 
				"    padding: 4px 4px;\n" + 
				"    margin: 4px 0;\n" + 
				"    display: inline-block;\n" + 
				"    border: 1px solid #ccc;\n" + 
				"    border-radius: 4px;\n" + 
				"    box-sizing: border-box;\n" + 
				"}\n" + 
				"input[type=submit] {\n" + 
			    "	 font-size: 1.3em;" +
				"    width: 200px;\n" + 
				"    background-color: #ee7914;\n" + 
				"    color: white;\n" + 
				"    padding: 14px 20px;\n" + 
				"    margin: 0 auto;\n" + 
				"    display: block;" + 
				"    border: none;\n" + 
				"    border-radius: 4px;\n" + 
				"    cursor: pointer;\n" + 
				"}\n" +
				"input[type=text].q {\n" + 
				"    box-sizing: border-box;\n" + 
				"    border: 2px solid #ccc;\n" + 
				"    border-radius: 4px;\n" + 
				"    font-size: 16px;\n" + 
				"    background-color: white;\n" + 
				"    background-image: url('data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABQAAAAUCAMAAAC6V+0/AAAAn1BMVEUAAAC9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8e9w8d3jEAbAAAANHRSTlMAAQIDBAYHDhIaIiotLzQ1NkFLTFBRUlRZW3x/goOFlJ6itLe5vMHOz9Xc3uDi6+3z9fn9TV6j9QAAALNJREFUGBllwYlWwjAARcGXUMqubEXBBSNKCi5FuP//bTaNEQ/OKGjNXgG/zHV2VREd50puObtXNATKUSbb3wBzBeYDnFVjBcdctWvYW/3YwFK1NUyU9MGrtoOOEgsYSQfI9QvIJHnoKskAI+kBCiUj2Ko2gKqtyJYwU+Bh21ZgHXy2FPROUBXdvDPZAy9GjfGJP5xRo+eJ3u8AZxQNHsuvt/XY6AZwRhcWwJMuFYD+mfL8DZLcHdhAVd5GAAAAAElFTkSuQmCC');\n" + 
				"    background-position: 10px 10px; \n" + 
				"    background-repeat: no-repeat;\n" + 
				"    padding: 12px 20px 12px 40px;" +
				"}"+
				"input[type=submit]:hover {\n" + 
				"    background-color: #ab570e;\n" + 
				"}\n" +
				"fieldset {\n" + 
				"    margin-top: 40px;\n" + 
				"}\n" + 
				"legend {\n" + 
				"    font-size: 1.3em;\n" + 
				"}\n" + 
				"table.controls {\n" + 
				"    border-collapse:separate; \n" + 
				"    border-spacing: 0 20px;\n" + 
				"}\n" + 
				"body {\n" + 
				"    margin: 8px;\n" + 
				"}\n" + 
				".footer a, .footer a:hover, .footer a:visited {\n" + 
				"    color: White;\n" + 
				"}\n" +
				".footer {\n" + 
				"    background: #5A5A5A;\n" + 
				"    padding: 10px;\n" + 
				"    margin: 20px -8px -8px -8px;\n" + 
				"}\n" + 
				".footer p {\n" + 
				"    text-align: center;\n" + 
				"    color: White;\n" + 
				"    width: 100%;\n" + 
				"}\n" + 
				".header {\n" + 
				"    margin: -8px 0 0 0;\n" + 
				"}\n" + 
				"h2 { margin-bottom:0px;}\n" +
				".logo {\n" + 
				"    margin: 0;\n" + 
				"    height: 63px;\n" + 
				"    background: url('data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAADgAAAA/CAIAAAA64ZzxAAAACXBIWXMAAAsTAAALEwEAmpwYAAAKJWlDQ1BQaG90b3Nob3AgSUNDIHByb2ZpbGUAAHjarZZXUJQJFoXP/3eCDqRuWkBCk5MgOUrOgoJINtF2k6Fp2m7EnBgcwTGgIoIRGRFRcHQEZAyIAQODYMI8IIOKug4GTKjsA1Pj7Nbuw1btqbpVp87DuXXrvnwAK10olWaTakCORC6LDvEXJCQmCRgPQYIPDnRgLRTNk/pFRUXgP4sA3t0CAQDXbYRSaTb+N6mLU+aJACIKQK54nigHIFoBBIqkMjlAUgEYz5dL5QBpAYAnS0hMAshJAHhpYz4KAG/umBcD4MliogMAsgBQYguFsjSAWQhAkC9KkwPMagB2EnGGBGDeAOAtSheKARYPwIScnFwxwPIFYDH3bz1p/9I5969OoTDtLz92CwBAMyIgUCDKVWSnKGSCFLkwO1ciScH/WTnZCtG3P4CdKguOBmABQBcRCEAgBBAhFwpkIwUKyCDAIOQQIhu5kECCQXlKgRwAAnKlC2QZaelygZ9Ump0iCJOIbCcIHOzsnYCExCTB2Jo3fBAACP7lb1leK+BeDBBp3zKhMXD8McB99y0zfg2wNwAnu0QKWf5YRgUAGphQBQ/a0IcxLGADB7jAE74IwmREIgaJmA0R0pEDGeZjMVagCCXYgC2owC7sxX4cwhE04QTO4AKuoAs3cQ+9GMBzDOEdRgiCYBAcgktoEwaEKWFNOBBuhDcRREQQ0UQikUykERJCQSwmVhElRClRQewhaomfiOPEGeIS0U3cIfqIQeI18YmkkGySR+qRZuRE0o30I8PJGHIWmUbmkQvJQnIdWU5WkQfJRvIMeYW8SfaSz8lhCigsCp9iSLGhuFECKJGUJEoqRUZZSimmlFGqKPWUFko75Tqll/KC8pFKp3KpAqoN1ZMaSo2liqh51KXUtdQK6n5qI/Uc9Tq1jzpE/Urj0HRp1jQPWhgtgZZGm08ropXR9tGO0c7TbtIGaO/odDqfbk53pYfSE+mZ9EX0tfQd9AZ6K72b3k8fZjAY2gxrhhcjkiFkyBlFjG2Mg4zTjGuMAcYHJZaSgZKDUrBSkpJEaaVSmdIBpVNK15SeKI0oqymbKnsoRyqLlRcor1euVm5Rvqo8oDzCVGeaM72YMcxM5gpmObOeeZ55n/mGxWIZsdxZ01gZrOWsctZh1kVWH+sjW4NtxQ5gz2Qr2OvYNexW9h32Gw6HY8bx5SRx5Jx1nFrOWc5DzgcVroqtSpiKWGWZSqVKo8o1lZeqyqqmqn6qs1UXqpapHlW9qvpCTVnNTC1ATai2VK1S7bhaj9qwOlfdXj1SPUd9rfoB9UvqTzUYGmYaQRpijUKNvRpnNfq5FK4xN4Ar4q7iVnPPcwd4dJ45L4yXySvhHeJ18oY0NTSdNOM0CzQrNU9q9vIpfDN+GD+bv55/hH+L/2mc3ji/cSnj1oyrH3dt3Hut8Vq+WilaxVoNWje1PmkLtIO0s7Q3ajdpP9Ch6ljpTNOZr7NT57zOi/G88Z7jReOLxx8Zf1eX1LXSjdZdpLtXt0N3WE9fL0RPqrdN76zeC32+vq9+pv5m/VP6gwZcA2+DDIPNBqcNngk0BX6CbEG54JxgyFDXMNRQYbjHsNNwxMjcKNZopVGD0QNjprGbcarxZuM24yETA5MpJotN6kzumiqbupmmm241bTd9b2ZuFm+22qzJ7Km5lnmY+ULzOvP7FhwLH4s8iyqLG5Z0SzfLLMsdll1WpJWzVbpVpdVVa9LaxTrDeod19wTaBPcJkglVE3ps2DZ+Nvk2dTZ9tnzbCNuVtk22LyeaTEyauHFi+8Svds522XbVdvfsNewn26+0b7F/7WDlIHKodLjhyHEMdlzm2Oz4ysnaKcVpp9NtZ67zFOfVzm3OX1xcXWQu9S6Driauya7bXXvceG5RbmvdLrrT3P3dl7mfcP/o4eIh9zji8YenjWeW5wHPp5PMJ6VMqp7U72XkJfTa49XrLfBO9t7t3etj6CP0qfJ55GvsK/bd5/vEz9Iv0++g30t/O3+Z/zH/9wEeAUsCWgMpgSGBxYGdQRpBsUEVQQ+DjYLTguuCh0KcQxaFtIbSQsNDN4b2hOmFicJqw4Ymu05eMvlcODt8enhF+KMIqwhZRMsUcsrkKZum3J9qOlUytSkSkWGRmyIfRJlH5UX9Mo0+LWpa5bTH0fbRi6Pbp3Onz5l+YPq7GP+Y9TH3Yi1iFbFtcapxM+Nq497HB8aXxvcmTExYknAlUScxI7E5iZEUl7QvaXhG0IwtMwZmOs8smnlrlvmsglmXZuvMzp59co7qHOGco8m05PjkA8mfhZHCKuHw3LC52+cOiQJEW0XPxb7izeLBFK+U0pQnqV6ppalP07zSNqUNpvukl6W/yAjIqMh4lRmauSvzfVZkVk3WaHZ8dkOOUk5yznGJhiRLci5XP7cgt1tqLS2S9uZ55G3JG5KFy/bNI+bNmtcs58ml8g6FheI7RV++d35l/of5cfOPFqgXSAo6FlgtWLPgycLghT8uoi4SLWpbbLh4xeK+JX5L9iwlls5d2rbMeFnhsoHlIcv3r2CuyFrx60q7laUr366KX9VSqFe4vLD/u5Dv6opUimRFPas9V+/6nvp9xvedaxzXbFvztVhcfLnErqSs5PNa0drLP9j/UP7D6LrUdZ3rXdbv3EDfINlwa6PPxv2l6qULS/s3TdnUuFmwuXjz2y1ztlwqcyrbtZW5VbG1tzyivHmbybYN2z5XpFfcrPSvbNiuu33N9vc7xDuu7fTdWb9Lb1fJrk+7M3bf3hOyp7HKrKpsL31v/t7H1XHV7T+6/Vi7T2dfyb4vNZKa3v3R+8/VutbWHtA9sL6OrFPUDR6cebDrUOCh5nqb+j0N/IaSwzisOPzsp+Sfbh0JP9J21O1o/c+mP28/xj1W3Eg0Lmgcakpv6m1ObO4+Pvl4W4tny7FfbH+pOWF4ovKk5sn1p5inCk+Nnl54erhV2vriTNqZ/rY5bffOJpy9cW7auc7z4ecvXgi+cLbdr/30Ra+LJy55XDp+2e1y0xWXK40dzh3HfnX+9VinS2fjVderzV3uXS3dk7pPXfO5duZ64PULN8JuXLk59Wb3rdhbt3tm9vTeFt9+eif7zqu7+XdH7i2/T7tf/EDtQdlD3YdVv1n+1tDr0nuyL7Cv49H0R/f6Rf3Pf5/3++eBwsecx2VPDJ7UPnV4emIweLDr2YxnA8+lz0deFP1D/R/bX1q8/PkP3z86hhKGBl7JXo2+XvtG+03NW6e3bcNRww/f5bwbeV/8QfvD/o9uH9s/xX96MjL/M+Nz+RfLLy1fw7/eH80ZHZUKZUIAAAUAmZoKvK4BOIkAtwtgqowx25+MQ/yNdv6LH+M6AIALUOMLxC4HIlqBna2A6XKA3QpEAYjxBeno+Nf8qXmpjg5jXWwZQPswOvpGD2C0AF9ko6MjO0ZHv1QDlDtAa94YKwIAXQ3YrQUAHT1Uw3/ntH8CJLLCK0pn3ZAAADspaVRYdFhNTDpjb20uYWRvYmUueG1wAAAAAAA8P3hwYWNrZXQgYmVnaW49Iu+7vyIgaWQ9Ilc1TTBNcENlaGlIenJlU3pOVGN6a2M5ZCI/Pgo8eDp4bXBtZXRhIHhtbG5zOng9ImFkb2JlOm5zOm1ldGEvIiB4OnhtcHRrPSJBZG9iZSBYTVAgQ29yZSA1LjYtYzA2NyA3OS4xNTc3NDcsIDIwMTUvMDMvMzAtMjM6NDA6NDIgICAgICAgICI+CiAgIDxyZGY6UkRGIHhtbG5zOnJkZj0iaHR0cDovL3d3dy53My5vcmcvMTk5OS8wMi8yMi1yZGYtc3ludGF4LW5zIyI+CiAgICAgIDxyZGY6RGVzY3JpcHRpb24gcmRmOmFib3V0PSIiCiAgICAgICAgICAgIHhtbG5zOnhtcD0iaHR0cDovL25zLmFkb2JlLmNvbS94YXAvMS4wLyIKICAgICAgICAgICAgeG1sbnM6ZGM9Imh0dHA6Ly9wdXJsLm9yZy9kYy9lbGVtZW50cy8xLjEvIgogICAgICAgICAgICB4bWxuczpwaG90b3Nob3A9Imh0dHA6Ly9ucy5hZG9iZS5jb20vcGhvdG9zaG9wLzEuMC8iCiAgICAgICAgICAgIHhtbG5zOnhtcE1NPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvbW0vIgogICAgICAgICAgICB4bWxuczpzdEV2dD0iaHR0cDovL25zLmFkb2JlLmNvbS94YXAvMS4wL3NUeXBlL1Jlc291cmNlRXZlbnQjIgogICAgICAgICAgICB4bWxuczp0aWZmPSJodHRwOi8vbnMuYWRvYmUuY29tL3RpZmYvMS4wLyIKICAgICAgICAgICAgeG1sbnM6ZXhpZj0iaHR0cDovL25zLmFkb2JlLmNvbS9leGlmLzEuMC8iPgogICAgICAgICA8eG1wOkNyZWF0b3JUb29sPkFkb2JlIFBob3Rvc2hvcCBDQyAyMDE1IChNYWNpbnRvc2gpPC94bXA6Q3JlYXRvclRvb2w+CiAgICAgICAgIDx4bXA6Q3JlYXRlRGF0ZT4yMDE2LTA5LTEyVDIwOjUyOjIzKzAyOjAwPC94bXA6Q3JlYXRlRGF0ZT4KICAgICAgICAgPHhtcDpNb2RpZnlEYXRlPjIwMTYtMDktMTJUMjE6MDI6MzYrMDI6MDA8L3htcDpNb2RpZnlEYXRlPgogICAgICAgICA8eG1wOk1ldGFkYXRhRGF0ZT4yMDE2LTA5LTEyVDIxOjAyOjM2KzAyOjAwPC94bXA6TWV0YWRhdGFEYXRlPgogICAgICAgICA8ZGM6Zm9ybWF0PmltYWdlL3BuZzwvZGM6Zm9ybWF0PgogICAgICAgICA8cGhvdG9zaG9wOkNvbG9yTW9kZT4zPC9waG90b3Nob3A6Q29sb3JNb2RlPgogICAgICAgICA8cGhvdG9zaG9wOklDQ1Byb2ZpbGU+TENEIGNvdWxldXIgZXRhbG9ubmU8L3Bob3Rvc2hvcDpJQ0NQcm9maWxlPgogICAgICAgICA8eG1wTU06SW5zdGFuY2VJRD54bXAuaWlkOjMwYzM5Zjc2LTUwZDAtNGVlZS1iMDUxLTVlNDJiMzJlMDMyYjwveG1wTU06SW5zdGFuY2VJRD4KICAgICAgICAgPHhtcE1NOkRvY3VtZW50SUQ+YWRvYmU6ZG9jaWQ6cGhvdG9zaG9wOmQ0YjZhYzM4LWI5YTktMTE3OS04MTljLWM3ZDBhODE4MjZjMDwveG1wTU06RG9jdW1lbnRJRD4KICAgICAgICAgPHhtcE1NOk9yaWdpbmFsRG9jdW1lbnRJRD54bXAuZGlkOjI0YzE2OThiLTU4YTctNDA4Ni05M2JmLTgxZmY2NjYzMmRkZTwveG1wTU06T3JpZ2luYWxEb2N1bWVudElEPgogICAgICAgICA8eG1wTU06SGlzdG9yeT4KICAgICAgICAgICAgPHJkZjpTZXE+CiAgICAgICAgICAgICAgIDxyZGY6bGkgcmRmOnBhcnNlVHlwZT0iUmVzb3VyY2UiPgogICAgICAgICAgICAgICAgICA8c3RFdnQ6YWN0aW9uPmNyZWF0ZWQ8L3N0RXZ0OmFjdGlvbj4KICAgICAgICAgICAgICAgICAgPHN0RXZ0Omluc3RhbmNlSUQ+eG1wLmlpZDoyNGMxNjk4Yi01OGE3LTQwODYtOTNiZi04MWZmNjY2MzJkZGU8L3N0RXZ0Omluc3RhbmNlSUQ+CiAgICAgICAgICAgICAgICAgIDxzdEV2dDp3aGVuPjIwMTYtMDktMTJUMjA6NTI6MjMrMDI6MDA8L3N0RXZ0OndoZW4+CiAgICAgICAgICAgICAgICAgIDxzdEV2dDpzb2Z0d2FyZUFnZW50PkFkb2JlIFBob3Rvc2hvcCBDQyAyMDIyIChNYWNpbnRvc2gpPC9zdEV2dDpzb2Z0d2FyZUFnZW50PgogICAgICAgICAgICAgICA8L3JkZjpsaT4KICAgICAgICAgICAgICAgPHJkZjpsaSByZGY6cGFyc2VUeXBlPSJSZXNvdXJjZSI+CiAgICAgICAgICAgICAgICAgIDxzdEV2dDphY3Rpb24+Y29udmVydGVkPC9zdEV2dDphY3Rpb24+CiAgICAgICAgICAgICAgICAgIDxzdEV2dDpwYXJhbWV0ZXJzPmZyb20gYXBwbGljYXRpb24vdm5kLmFkb2JlLnBob3Rvc2hvcCB0byBpbWFnZS9wbmc8L3N0RXZ0OnBhcmFtZXRlcnM+CiAgICAgICAgICAgICAgIDwvcmRmOmxpPgogICAgICAgICAgICAgICA8cmRmOmxpIHJkZjpwYXJzZVR5cGU9IlJlc291cmNlIj4KICAgICAgICAgICAgICAgICAgPHN0RXZ0OmFjdGlvbj5zYXZlZDwvc3RFdnQ6YWN0aW9uPgogICAgICAgICAgICAgICAgICA8c3RFdnQ6aW5zdGFuY2VJRD54bXAuaWlkOjMwYzM5Zjc2LTUwZDAtNGVlZS1iMDUxLTVlNDJiMzJlMDMyYjwvc3RFdnQ6aW5zdGFuY2VJRD4KICAgICAgICAgICAgICAgICAgPHN0RXZ0OndoZW4+MjAxNi0wOS0xMlQyMTowMjozNiswMjowMDwvc3RFdnQ6d2hlbj4KICAgICAgICAgICAgICAgICAgPHN0RXZ0OnNvZnR3YXJlQWdlbnQ+QWRvYmUgUGhvdG9zaG9wIENDIDIwMjIgKE1hY2ludG9zaCk8L3N0RXZ0OnNvZnR3YXJlQWdlbnQ+CiAgICAgICAgICAgICAgICAgIDxzdEV2dDpjaGFuZ2VkPi88L3N0RXZ0OmNoYW5nZWQ+CiAgICAgICAgICAgICAgIDwvcmRmOmxpPgogICAgICAgICAgICA8L3JkZjpTZXE+CiAgICAgICAgIDwveG1wTU06SGlzdG9yeT4KICAgICAgICAgPHRpZmY6T3JpZW50YXRpb24+MTwvdGlmZjpPcmllbnRhdGlvbj4KICAgICAgICAgPHRpZmY6WFJlc29sdXRpb24+NzIwMDAwLzEwMDAwPC90aWZmOlhSZXNvbHV0aW9uPgogICAgICAgICA8dGlmZjpZUmVzb2x1dGlvbj43MjAwMDAvMTAwMDA8L3RpZmY6WVJlc29sdXRpb24+CiAgICAgICAgIDx0aWZmOlJlc29sdXRpb25Vbml0PjI8L3RpZmY6UmVzb2x1dGlvblVuaXQ+CiAgICAgICAgIDxleGlmOkNvbG9yU3BhY2U+NjU1MzU8L2V4aWY6Q29sb3JTcGFjZT4KICAgICAgICAgPGV4aWY6UGl4ZWxYRGltZW5zaW9uPjU2PC9leGlmOlBpeGVsWERpbWVuc2lvbj4KICAgICAgICAgPGV4aWY6UGl4ZWxZRGltZW5zaW9uPjYzPC9leGlmOlBpeGVsWURpbWVuc2lvbj4KICAgICAgPC9yZGY6RGVzY3JpcHRpb24+CiAgIDwvcmRmOlJERj4KPC94OnhtcG1ldGE+CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICAgICAgICAgICAgCjw/eHBhY2tldCBlbmQ9InciPz5cW51ZAAAAIGNIUk0AAG11AABzoAAA/N0AAINkAABw6AAA7GgAADA+AAAQkOTsmeoAAAuCSURBVHjazFpbjF3nVf6+9f//vpxzZs6ZGXvsxHHqXKxEKpZFE6etRBRVpTRPCCSkqqiJ+oCEBE+8AOIBIZCQ+h6BuDwi4KWIawuRaKFRKFEjQZHbNHGi1HYcO3PxXM51/3v/a/Fwztjj+jrO2PXWfppztOfba33rW99a/6GZYd8vMx2tQhuIhyXbOI8mwhShdI88A8pdPNLjHl1ms1sNmqAJptAE3GVc7g1QAs6DBAXiZOkxGGAKcYA8SEANGG2iicjbqIbVd//SRmumJp2DxYu/j6KLZgJNun0RWsMA52XxGCTc/9QrBuuox7Ql216dfPMvdK0Pgyx3s+d/U7Lc4gBNpR+8qXEAg+Qd6T4M8QDvM1DCeVgGCfC5LHkIoJBFB5eBDnQQj3xOxAOGrH1bStwroGwvMzXoLMJoA7MtQGGFsVwEA8sFaGLeMXEAGFogYAq6mz7xTuVp99e4K0GmAEBBqtN737JqgFBCk22ctaZCyNHUzZn/1EkfMCkW/PGfgxCppni6zExRj1H2wjMvwxd3G9Hd4Ex3lIXXvLelHaCT6o0/060LLLuEQxxYmlgTpez5k1+20IJ4jLeqf/0d3e7DQcp2dvJL8JmN1tk+dFvV8ntUnVv80cn8EYhnPgeKjS4zTaiKrK39VYgDHZpKFo6xXDdQigVkc/AOKTJv7588ibsZG2dYSaQaKSJF0CFVliK0ZsqYd+EyiKAez8gmHgb0Lxphkw3T5raNwN9BBAlt0um/t8k2CGRz/pO/BBdm7EwNYPAFNEnnMCQwtCk0X1qKIMlgF//HUgUJhNEVki1YqpjN+0+9zHLexpvMOvBhPyKaYvWdr+nGj0FI95h/6kW4MGWnpRqqgEOqUS4CDqEESfHQmr6wJjanv66TdbpA12Exb44WxyZ0xz+PUOxrZyKl9wmIByDzR3ZchQAGEEKQIJAiUgVxRiBVSLWZQRvmHTJRAlxhptAGlmZVuO8tlOLpAmB0fop4ylrmO08IbRuu22AVWRvi6QgYYFC1emxxCJdRDUktjWwysDjak0Hxd1TmRo1jrQZwGSaD5vQ/wOdIEaTFITTCAJ+5p37B0YGCFPX91ywO4XJI7g6dkHoEOroMJLS2OJTOIRusouyh6kM8O8vXyPNdRzSNL+twlUXXVO31V6wZgwI6VJetGdtkSxYea//GfyGUM06f/57GEekpIovHoc00LcgCTJEisrZd/jFcjnqEULJzAHD7kXoK6QiQTnpHTeuZzk/mTCc2GUj3iDUVQ2kA6gksAWnWJmLfUgRozlMDLFlTiRp8gWnz9Pkt7MiegJrVI6uHRgg9XEGXz/pVuoRmgjRBirNONvXLTYW6Aj0AG65bU4Gky6xow5LVI6OjzxEKWILP9sxR638IVYbCqmH1+p/rcJWhBfH+6PN45DOgEF5XTutknXQMLTl8EuJssintZYYCAEmEQg6fkDgAPUPGE78MOsBgZqNVpBoUUHTr3BUauIPHbz2iXAd0+xK0Qdm17ZXxN17R9W0IpNdp/+rX2OpaalDH9O4/6eAc6KQ8GI6/iKyN8QbL3kwNADgvC4+iHgOAL93RZ2b2wMwGK9AEX6AeN29/Q6sBTKXsOdtrZ/I51MPlCC23PM+wDQdpz1kcGBNUkRr6gqENOvoCzRgwxCF8flVuDBZHqEc7TLjCQLNqC009FQ26THwJTZDs1iV/I6CpgTbQGk20/sC2AYFhCJcztE0bsJnCBQ2q9C1kJVLNUF5bEJx54WkvuFKSc0egilAiDlIc6mQbZiKCvUaUvSMwRciZtYsXf0/HmwwF6dk/b5sTSCAo848i75Gkb+n6O4ChHmvRbd55dRYqCiWY80g1zazqw2VwGSbb8dt/rMOPQCflYjj1ay6fQzWAz67S5g6Ns0Fn7dGSrX2A1CCfw2Sj/o8/0OEKQ4uuMNDSZFoQNt60VEETs44cPjm1SMw67vEX4HPUY/hclp+G88znbPvS8JVTurEBhRxYnPvd88had9uZmggA4pGibZ23eoy8gziCy5F14AtIhqqPZgrU0xegQBv6kq0liEc9ZmgB02wSIFyACxAPn0vvYWACM+k+9JMzwt6Ams0oZWb1xOIEcEwVs3kB4UuCuv2hxi1KgAQ0taUKWiPVFocQb/VIYPQ5fA4DfCntA9OqZ2sJKc7UIMUZuDtbnFw3M2kCARCmOrxMTSi6GK5P/uZLunUWLpfWgfCzL1nIUU8gHkUPpkg1spJzh2bFa2Yb59BEuAAzXf2R1QO4HIBtnrN6bPVYOoeLL/4Ryi6qgYmX+cO3pqm/qZOnyNzyjidBc+ltXbsMQhY38qPPsXPARhvwWXz3tfzUS7t2NQZfIA7i2f/W8SZbC6hG8fU/0cEAhLTa4bmvMhSYbLHopu1LGG0hjhhytJdAdwuRuqMWSpCBzAAHBtr4MqhW9UHnj5xo3vuOO/A4ROAyAHBj1GP6kllDX8Ioi0dQrEC85D00Y+gEsQ/nGUpkHZDw2d45euNOb9ZX2wIcTBLqMeoC1QguyMJyfP+7bvEYTHc4B1gDnyEV8Dl0ui1TmMIMqYERTYQmmTuMcgHVACLw2VSGp1TkdaG95Vw//YhEquvv/61W2ySQdcOTn0fI0URQEArrr6aVt/wTL8yAZi2MN6tX/1AHKyzmYbCVH1ocmKkUXffk56ZjDI3pwlta9ZGidJbzn/9tlHNShmrMvPfQXiO644bEhU+9dFNi5PP1mW/T5VdNkC9165xuX2AxTwmYSliKCG22D8Bn9MFG2/F7/6hbAyhksRue/bKkJVSqVY4bAb0DaaAApv1LuvWBbn2g/UvXt7vwM79Yn/nWbvdKX9CX9C24AqlGU80m6RSRojUVUpL5THqQHmQ+0JdwBXxpBlX9GHP9lGGwq0K7+3Xnlpv+JWsmnK1l7MpNmFEgAhVQGNrwBb23xmxU2xBQWD41GA3UeJPFzq13T7tsT4oEDEYI3A1mcN280Pzwn938QWRtNDG9/5rFwdS629YFq8egowuwZKmyZiLlUjj168g6iH34Uh56muJQdOJomLXm73a4A+gy3HJckLllXTtj4zXSkYJQQBzEw8xEIAIXDNCPfmBx26q+LTxRPP7Z2Yy163J528yur/r9XDu6h07YZBMkxNtoHc0ELgOMqbZUT1+H+RxEQMe8g9QgALOPZltVEeGNZF/2ESg0ptX3GFoAkaLVQ4sD1KNpz7Q4RD2GKkyhuovrtjt/vElz+tgRnc6ZdCDd8S+gfUh6j7DVc/gizEAHa2ztjG5dlN4RNFXz9qs26SNFdg5BMgCQAO4uh3u0cZ5uZkxBkcVjnDtUf//r4dmv7F71Wmg1H73rjz6HaiAX/lddBlO2lyC8uhbmPT8VIUBQQAJGl1lqMN5E2bvyjfiDf/GPfdYGq4gjqwZWDaCJvtjTkZPsA1CRHddDiAtPPJ9W3r4az+Fl5h06D0uwxuLI4tDiwOr93D3hzvrWNWmTpcfqd/7df+LTs3D+39/lp16GqQG0FF74rdkp3nSKuo9AeT10aR+wasC8o5vn3YEn4fMrJJSlzt39n32VpyuC+uiz6aMfAajf+jf/xPPXdbsr908bqHQftu0Pdf19d/iTt52Df5pAIQ6pqV7/U3/s0zdWCfD2gnQ/gIKcO+SPf+7ujubvJ1A0Z9+4ocl6sIBaHDErUU8edKDp7Bvh5K9AHDQ9wEDNdOOsdA6y6Nl08/hgAtXBiiw/PbXSNlh7cIGmi6fd8lMA2FrQ4fpdpcTukR/ddXQL2HCNZRcAQomqf1fadk8c/nQ03THq2tDlU/mk+NkpzwOUetp02Lc4RNG5aqz2r+r3abiznUV9anYN5jZbCz8YQOUas5e1dPvirPyHa9zl8x+A1O/iPkOJFOObf2VVv37zr/2xz+ybe/i4PyI0+0nvbFa/9U3AwlNf2JOHv+9A7831/wMAS3DlxX+xDNoAAAAASUVORK5CYII=');\n" + 
				"    background-repeat: no-repeat;\n" + 
				"}\n" + 
				".logo span {\n" + 
				"    line-height: 63px;\n" + 
				"    vertical-align: middle;\n" + 
				"    color: #5e5e5e;\n" + 
				"    padding-left: 60px;\n" + 
				"    font-size: 20px;\n" + 
				"}\n" + 
				".header hr {\n" + 
				"    margin: 0 -8px 0 -8px;\n" + 
				"}\n" +
				".period input {\n" + 
				"    margin-left: 10px;\n" + 
				"    margin-right: 10px;\n" + 
				"}\n" + 
				".instructions {\n" + 
				"    font-size: 1.2em;\n" + 
				"    font-style: italic;\n" + 
				"}" +
				"</style>");
		// drag & drop support
		html.append("<script>\n" + 
				"function allowDrop(ev) {\n" + 
				"    ev.preventDefault();\n" + 
				"}\n" + 
				"function drag(ev, text) {\n" + 
				"    ev.dataTransfer.setData(\"text\", \"'\"+text+\"'\");\n" + 
				"}\n" + 
				"function drop(ev) {\n" + 
				"    ev.preventDefault();\n" + 
				"    var data = ev.dataTransfer.getData(\"text\");\n" + 
				"    ev.target.value = data;\n" +
				"}\n" + 
				"</script>");
		html.append("<body>");
		return html;
	}
	
	private Response createHTMLPageList(AppContext ctx, NavigationQuery query, NavigationResult result) {
		String title = (query.getParent()!=null && query.getParent().length()>0)?query.getParent():"Root";
		StringBuilder html = createHTMLHeader("List: "+title);
		createHTMLtitle(html, title, null, result.getParent().getUpLink());
		// form
		html.append("<form><table>");
		html.append("<tr><td><input size=50 class='q' type='text' name='q' placeholder='filter the list' value='"+(query.getQ()!=null?query.getQ():"")+"'></td>"
				+ "<td><input type=\"submit\" value=\"Filter\"></td></tr>");
		html.append("<input type='hidden' name='parent' value='"+(query.getParent()!=null?query.getParent():"")+"'>");
		if (query.getStyle()!=null)
			html.append("<input type='hidden' name='style' value='"+(query.getStyle()!=null?query.getStyle():"")+"'>");
		if (query.getVisibility()!=null)
			html.append("<input type='hidden' name='visibility' value='"+(query.getVisibility()!=null?query.getVisibility():"")+"'>");
		if (query.getHiearchy()!=null)
			html.append("<input type='hidden' name='hierarchy' value='"+query.getHiearchy()+"'>");
		html.append("<input type='hidden' name='access_token' value='"+ctx.getToken().getOid()+"'>");
		html.append("</table></form>");
		//
		// parent description
		if (result.getParent()!=null && result.getParent().getDescription()!=null && result.getParent().getDescription().length()>0) {
			html.append("<p><i>"+result.getParent().getDescription()+"</i></p>");
		}
		//
		html.append("<table style='border-collapse:collapse'>");
		for (NavigationItem item : result.getChildren()) {
			html.append("<tr>");
			html.append("<td valign='top'>"+item.getType()+"</td>");
			if (item.getLink()!=null) {
				html.append("<td valign='top'><a href=\""+StringEscapeUtils.escapeHtml4(item.getLink().toString())+"\">"+item.getName()+"</a>");
			} else {
				html.append("<td valign='top'>"+item.getName());
			}
			if (item.getObjectLink()!=null) {
				html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(item.getObjectLink().toString())+"\">info</a>]");
			}
			if (item.getViewLink()!=null) {
				html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(item.getViewLink().toString())+"\">view</a>]");
			}
			if (item.getDescription()!=null && item.getDescription().length()>0) {
				html.append("<br><i>"+(item.getDescription()!=null?item.getDescription():"")+"</i>");
			}
			html.append("</td>");
			if (item.getAttributes()!=null) {
				for (Entry<String, String> entry : item.getAttributes().entrySet()) {
					html.append("<td valign='top'>"+entry.getKey()+"="+entry.getValue()+"</td>");
				}
			}
		}
		html.append("</table>");
		createHTMLAPIpanel(html, "listContent");
		html.append("</body></html>");
		return Response.ok(html.toString(),"text/html").build();
	}
	
	private Response createHTMLPageView(Space space, ViewQuery view, Info info, ViewReply reply) {
		String title = getPageTitle(space);
		StringBuilder html = createHTMLHeader("View: "+title);
		if (getPublicBaseUriBuilder().build().getScheme().equalsIgnoreCase("https")) {
			html.append("<script src=\"https://d3js.org/d3.v3.min.js\" charset=\"utf-8\"></script>\r\n<script src=\"https://vega.github.io/vega/vega.js\" charset=\"utf-8\"></script>\r\n<script src=\"https://vega.github.io/vega-lite/vega-lite.js\" charset=\"utf-8\"></script>\r\n<script src=\"https://vega.github.io/vega-editor/vendor/vega-embed.js\" charset=\"utf-8\"></script>\r\n\r\n");
		} else {
			html.append("<script src=\"http://d3js.org/d3.v3.min.js\" charset=\"utf-8\"></script>\r\n<script src=\"http://vega.github.io/vega/vega.js\" charset=\"utf-8\"></script>\r\n<script src=\"http://vega.github.io/vega-lite/vega-lite.js\" charset=\"utf-8\"></script>\r\n<script src=\"http://vega.github.io/vega-editor/vendor/vega-embed.js\" charset=\"utf-8\"></script>\r\n\r\n");
		}
		html.append("<body>");
		createHTMLtitle(html, title, view.getBBID(), getParentLink(space));
		createHTMLproblems(html, reply.getQuery().getProblems());
		html.append("<div id=\"vis\"></div>\r\n\r\n<script>\r\nvar embedSpec = {\r\n  mode: \"vega-lite\", renderer:\"svg\",  spec:");
		html.append(writeVegalightSpecs(reply.getResult()));
		Encoding channels = reply.getResult().encoding;
		html.append("}\r\nvg.embed(\"#vis\", embedSpec, function(error, result) {\r\n  // Callback receiving the View instance and parsed Vega spec\r\n  // result.view is the View, which resides under the '#vis' element\r\n});\r\n</script>\r\n");
		createHTMLpagination(html, view, info);
		// data-link
		URI dataLink = buildAnalyticsQueryURI(userContext, reply.getQuery(), "RECORDS", "ALL", Style.HTML, null);
		html.append("<p><a href=\""+StringEscapeUtils.escapeHtml4(dataLink.toASCIIString())+"\">view query data</a></p>");
		//
		html.append("<form>");
		createHTMLfilters(html, reply.getQuery());
		html.append("<table>"
				+ "<tr><td>x</td><td><input type=\"text\" size=30 name=\"x\" value=\""+getFieldValue(view.getX())+"\"></td><td>"+(channels.x!=null?"as <b>"+channels.x.field+"</b>":"")+"</td></tr>"
				+ "<tr><td>y</td><td><input type=\"text\" size=30 name=\"y\" value=\""+getFieldValue(view.getY())+"\"></td><td>"+(channels.y!=null?"as <b>"+channels.y.field+"</b>":"")+"</td></tr>"
				+ "<tr><td>color</td><td><input type=\"text\" size=30 name=\"color\" value=\""+getFieldValue(view.getColor())+"\"></td><td>"+(channels.color!=null?"as <b>"+channels.color.field+"</b>":"")+"</td></tr>"
				+ "<tr><td>size</td><td><input type=\"text\" size=30 name=\"size\" value=\""+getFieldValue(view.getSize())+"\"></td><td>"+(channels.size!=null?"as <b>"+channels.size.field+"</b>":"")+"</td></tr>"
				+ "<tr><td>column</td><td><input type=\"text\" size=30 name=\"column\" value=\""+getFieldValue(view.getColumn())+"\"></td><td>"+(channels.column!=null?"as <b>"+channels.column.field+"</b>":"")+"</td></tr>"
				+ "<tr><td>row</td><td><input type=\"text\" size=30 name=\"row\" value=\""+getFieldValue(view.getRow())+"\"></td><td>"+(channels.row!=null?"as <b>"+channels.row.field+"</b>":"")+"</td></tr>");
		// metrics -- display the actual metrics
		html.append("<tr><td valign='top'>metrics</td><td>");
		createHTMLinputArray(html, "text", "metrics", reply.getQuery().getMetrics());
		html.append("</td><td>Use the metrics parameters if you want to view multiple metrics on the same graph. Then you can use the <b>__VALUE</b> expression in channel to reference the metrics' value, and the <b>__METRICS</b> to get the metrics' name as a series.<br>If you need only a single metrics, you can directly define it in a channel, e.g. <code>y=count()</code>.");
		html.append("</td></tr>");
		// limits, maxResults, startIndex
		html.append("<tr><td>limit</td><td>");
		html.append("<input type=\"text\" name=\"limit\" value=\""+getFieldValue(view.getLimit(),-1)+"\">");
		html.append("</td></tr>");
		html.append("<tr><td>maxResults</td><td>");
		html.append("<input type=\"text\" name=\"maxResults\" value=\""+getFieldValue(view.getMaxResults(),-1)+"\">");
		html.append("</td></tr>");
		html.append("<tr><td>startIndex</td><td>");
		html.append("<input type=\"text\" name=\"startIndex\" value=\""+getFieldValue(view.getStartIndex(),0)+"\"></td><td><i>index is zero-based, so use the #count of the last row to view the next page</i>");
		html.append("</td></tr>");
		html.append("</table>"
				+ "<input type=\"hidden\" name=\"style\" value=\"HTML\">"
				+ "<input type=\"hidden\" name=\"access_token\" value=\""+space.getUniverse().getContext().getToken().getOid()+"\">"
				+ "<input type=\"submit\" value=\"Refresh\">"
				+ "</form>");
		createHTMLscope(html, space, reply.getQuery());
		createHTMLAPIpanel(html, "viewAnalysis");
		html.append("</body>\r\n</html>");
		return Response.ok(html.toString(), "text/html; charset=UTF-8").build();
	}
	
	/**
	 * @param result
	 * @return
	 */
	private String writeVegalightSpecs(VegaliteSpecs specs) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			mapper.setSerializationInclusion(Include.NON_NULL);
			return mapper.writeValueAsString(specs);
		} catch (JsonProcessingException e) {
			throw new APIException("failed to write vegalite specs to JSON", e, true);
		}
	}
	
	/**
	 * displays query problems
	 * @param html
	 * @param query
	 */
	private void createHTMLproblems(StringBuilder html, List<Problem> problems) {
		if (problems!=null && problems.size()>0) {
			html.append("<div class='problems' style='border:1px solid red; background-color:lightpink;'>There are some problems with the query:");
			for (Problem problem : problems) {
				html.append("<li>"+problem.getSeverity().toString()+": "+problem.getSubject()+": "+problem.getMessage()+"</li>");
			}
			html.append("</div>");
		}
	}
	
	private SimpleDateFormat htmlDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	private String formatDateForWeb(String jsonFormat) {
		try {
			Date date = ServiceUtils.getInstance().toDate(jsonFormat);
			return htmlDateFormat.format(date);
		} catch (ParseException e) {
			return jsonFormat;
		}
	}
	
	private String getDate(List<String> dates, int pos) {
		if (dates!=null && !dates.isEmpty() && pos<dates.size()) {
			return formatDateForWeb(getFieldValue(dates.get(pos)));
		} else {
			return "";
		}
	}

	/**
	 * create a filter HTML snippet
	 * @param query
	 * @return
	 */
	private void createHTMLfilters(StringBuilder html, AnalyticsQuery query) {
		html.append("<table><tr><td>");
		// period
		html.append("<span class='tooltip'>period: <span class='tooltiptext'>the period defines a dimension or expression of a type date that is used to filter the query or view. You can use the __PERIOD expression as a alias to it.</span></span>");
		html.append("</td><td>");
		html.append("<input type='text' size=30 name='period' value='"+getFieldValue(query.getPeriod())+"'>");
		// timeframe
		html.append("&nbsp;<span class='tooltip'>timeframe <span class='tooltiptext'>the timeframe defines the period range to filter. You can use an array of two dates for lower/upper bounds (inclusive). Or some alias like __ALL, __LAST_DAY, __LAST_7_DAYS, __CURRENT_MONTH, __PREVIOUS_MONTH, __CURRENT_YEAR, __PREVIOOUS_YEAR</span></span>");
		html.append("&nbsp;from:&nbsp;<input type='text' name='timeframe' value='"+getDate(query.getTimeframe(),0)+"'>");
		html.append("&nbsp;to:&nbsp;<input type='text' name='timeframe' value='"+getDate(query.getTimeframe(),1)+"'>");
		// compare
		html.append("&nbsp;<span class='tooltip'>compareTo <span class='tooltiptext'>Activate and define the compare to period. You can use an array of two dates for lower/upper bounds (inclusive). Or some alias like __ALL, __LAST_DAY, __LAST_7_DAYS, __CURRENT_MONTH, __PREVIOUS_MONTH, __CURRENT_YEAR, __PREVIOOUS_YEAR</span></span>");
		html.append("&nbsp;from:&nbsp;<input type='text' name='compareTo' value='"+getDate(query.getCompareTo(),0)+"'>");
		html.append("&nbsp;to:&nbsp;<input type='text' name='compareTo' value='"+getDate(query.getCompareTo(),1)+"'>");
		html.append("</td></tr>");
		// filters
		html.append("<tr><td>");
		html.append("<span class='tooltip'>filters:<span class='tooltiptext'>Define the filters to apply to results. A filter must be a valid conditional expression. If no filter is defined, the subject default config will apply. You can use the * token to extend the subject default configuration.</span></span>&nbsp;");
		html.append("</td><td>");
		if (query.getFilters()!=null && query.getFilters().size()>0) {
			for (String filter : query.getFilters()) {
				html.append("<input type='text' size=50 name='filters' value='"+getFieldValue(filter)+"'>&nbsp;");
			}
		}
		html.append("<input type='text' size=50 name='filters' value='' placeholder='type formula'>");	
		html.append("</td></tr></table>");
	}
	
	private static final String axis_style = "display: inline-block;border:1px solid;border-radius:5px;background-color:LavenderBlush ;margin:1px;";
	private static final String metric_style = "display: inline-block;border:1px solid;border-radius:5px;background-color:Lavender;margin:1px;";
	private static final String func_style = "display: inline-block;border:1px solid;border-radius:5px;background-color:ghostwhite;margin:1px;";
	private static final String other_style = "display: inline-block;border:1px solid;border-radius:5px;background-color:azure;margin:1px;";
	
	private void createHTMLscope(StringBuilder html, Space space, AnalyticsQuery query) {
		html.append("<fieldset><legend>Query scope: <i>this is the list of objects you can combine to build expressions in the query</i></legend>");
		html.append("<table>");
		html.append("<tr><td></td><td>You can Drag & Drop expression into input fields</td></tr>");
		html.append("<tr><td>GroupBy:</td><td>");
		for (Axis axis : space.A(true)) {// only print the visible scope
			try {
				IDomain image = axis.getDefinitionSafe().getImageDomain();
				if (!image.isInstanceOf(IDomain.OBJECT)) {
					DimensionIndex index = axis.getIndex();
					html.append("<span draggable='true' style='"+axis_style+"'");
					ExpressionAST expr = axis.getDefinitionSafe();
					html.append("title='"+getExpressionValueType(expr).toString()+": ");
					if (axis.getDescription()!=null) {
						html.append(axis.getDescription());
					}
					if (index.getErrorMessage()!=null) {
						html.append("\nError:"+index.getErrorMessage());
					}
					html.append("'");
					html.append(" ondragstart='drag(event,\""+index.getDimensionName()+"\")'>");
					if (index.getErrorMessage()==null) {
						html.append("&nbsp;"+index.getDimensionName()+"&nbsp;");
					} else {
						html.append("&nbsp;<del>"+index.getDimensionName()+"</del>&nbsp;");
					}
					html.append("</span>");
				}
			} catch (Exception e) {
				// ignore
			}
		}
		html.append("</td></tr>");
		html.append("<tr><td>Metrics:</td><td>");
		for (Measure m : space.M()) {
			if (m.getMetric()!=null && !m.getMetric().isDynamic()) {
				html.append("<span draggable='true'  style='"+metric_style+"'");
				ExpressionAST expr = m.getDefinitionSafe();
				html.append("title='"+getExpressionValueType(expr).toString()+": ");
				if (m.getDescription()!=null) {
					html.append(m.getDescription());
				}
				html.append("'");
				html.append(" ondragstart='drag(event,\""+m.getName()+"\")'");
				html.append(">&nbsp;"+m.getName()+"&nbsp;</span>");
			}
		}
		html.append("</td></tr></table>");
		URI scopeLink = getPublicBaseUriBuilder().path("/analytics/{reference}/scope").queryParam("style", Style.HTML).queryParam("access_token", userContext.getToken().getOid()).build(query.getBBID());
		html.append("<a href=\""+StringEscapeUtils.escapeHtml4(scopeLink.toASCIIString())+"\">View the scope</a>");
		html.append("</fieldset>");
	}
	
	/**
	 * @param space
	 * @param suggestions
	 * @param values 
	 * @param types 
	 * @param expression 
	 * @return
	 */
	private Response createHTMLPageScope(Space space, ExpressionSuggestion suggestions, String BBID, String value, ObjectType[] types, ValueType[] values) {
		String title = getPageTitle(space);
		StringBuilder html = createHTMLHeader("Scope: "+title);
		createHTMLtitle(html, title, BBID, getParentLink(space));
		if (value!=null && value.length()>0 && suggestions.getValidateMessage()!=null && suggestions.getValidateMessage().length()>0) {
			createHTMLproblems(html, Collections.singletonList(new Problem(Severity.WARNING, value, suggestions.getValidateMessage())));
		}
		html.append("<form>");
		html.append("<p>Expression:<input type='text' name='value' size=100 value='"+getFieldValue(value)+"' placeholder='type expression to validate it or to filter the suggestion list'></p>");
		html.append("<fieldset><legend>Filter by expression type</legend>");
		html.append("<input type='checkbox' name='types' value='"+ObjectType.DIMENSION+"'>"+ObjectType.DIMENSION);
		html.append("<input type='checkbox' name='types' value='"+ObjectType.COLUMN+"'>"+ObjectType.COLUMN);
		html.append("<input type='checkbox' name='types' value='"+ObjectType.RELATION+"'>"+ObjectType.RELATION);
		html.append("<input type='checkbox' name='types' value='"+ObjectType.METRIC+"'>"+ObjectType.METRIC);
		html.append("<input type='checkbox' name='types' value='"+ObjectType.FUNCTION+"'>"+ObjectType.FUNCTION);
		html.append("</fieldset>");
		html.append("<fieldset><legend>Filter by expression value</legend>");
		html.append("<input type='checkbox' name='values' value='"+ValueType.DATE+"'>"+ValueType.DATE);
		html.append("<input type='checkbox' name='values' value='"+ValueType.STRING+"'>"+ValueType.STRING);
		html.append("<input type='checkbox' name='values' value='"+ValueType.CONDITION+"'>"+ValueType.CONDITION);
		html.append("<input type='checkbox' name='values' value='"+ValueType.NUMERIC+"'>"+ValueType.NUMERIC);
		html.append("<input type='checkbox' name='values' value='"+ValueType.AGGREGATE+"'>"+ValueType.AGGREGATE);
		html.append("</fieldset>");
		html.append("<input type=\"hidden\" name=\"style\" value=\"HTML\">"
		+ "<input type=\"hidden\" name=\"access_token\" value=\""+space.getUniverse().getContext().getToken().getOid()+"\">"
		+ "<input type=\"submit\" value=\"Refresh\">");
		html.append("</form>");
		html.append("<p><i> This is the list of all available expressions and function in this scope. Relation expression can be composed in order to navigate the data model.</i></p>");
		html.append("<table>");
		for (ExpressionSuggestionItem item : suggestions.getSuggestions()) {
			html.append("<tr><td>");
			html.append(item.getObjectType()+"</td><td>");
			html.append(item.getValueType()+"</td><td>");
			String style = other_style;
			if (item.getObjectType()==ObjectType.DIMENSION) style = axis_style;
			if (item.getObjectType()==ObjectType.METRIC) style = metric_style;
			if (item.getObjectType()==ObjectType.FUNCTION) style = func_style;
			html.append("<span style='"+style+"'>&nbsp;"+item.getDisplay()+"&nbsp;</span>");
			if (item.getSuggestion()!=null) {
				URI link = getPublicBaseUriBuilder().path("/analytics/{reference}/scope").queryParam("value", value+item.getSuggestion()).queryParam("style", Style.HTML).queryParam("access_token", userContext.getToken().getOid()).build(BBID);
				html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(link.toASCIIString())+"\">+</a>]");
			}
			if (item.getExpression()!=null && item.getExpression() instanceof AxisExpression) {
				AxisExpression ref = (AxisExpression)item.getExpression();
				Axis axis = ref.getAxis();
				if (axis.getDimensionType()==Type.CATEGORICAL) {
					URI link = getPublicBaseUriBuilder().path("/analytics/{reference}/facets/{facetId}").queryParam("style", Style.HTML).queryParam("access_token", userContext.getToken().getOid()).build(BBID, item.getSuggestion());
					html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(link.toASCIIString())+"\">Indexed</a>]");
				} else if (axis.getDimensionType()==Type.CONTINUOUS) {
					URI link = getPublicBaseUriBuilder().path("/analytics/{reference}/facets/{facetId}").queryParam("style", Style.HTML).queryParam("access_token", userContext.getToken().getOid()).build(BBID, item.getSuggestion());
					html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(link.toASCIIString())+"\">Period</a>]");
				}
			}
			if (item.getDescription()!=null && item.getDescription().length()>0) {
				html.append("<br><i>"+item.getDescription()+"</i>");
			}
			html.append("</td></tr>");
		}
		html.append("</table>");
		createHTMLAPIpanel(html, "scopeAnalysis");
		html.append("</body></html>");
		return Response.ok(html.toString(),"text/html").build();
	}

	private void createHTMLAPIpanel(StringBuilder html, String method) {
		html.append("<fieldset><legend>API reference</legend>");
		// compute the raw URI
		UriBuilder builder = getPublicBaseUriBuilder().path(uriInfo.getPath());
		MultivaluedMap<String, String> parameters = uriInfo.getQueryParameters();
		parameters.remove(ACCESS_TOKEN_PARAM);
		parameters.remove(STYLE_PARAM);
		parameters.remove(ENVELOPE_PARAM);
		for (Entry<String, List<String>> parameter : parameters.entrySet()) {
			for (String value : parameter.getValue()) {
				builder.queryParam(parameter.getKey(), value);
			}
		}
		html.append("<p>Request URL: <i>this URL is not authorized</i></p><div style='display:block;'><pre style='background-color: #fcf6db;border: 1px solid #e5e0c6; width:1024px; max-height: 400px;overflow-y: auto;'>"+StringEscapeUtils.escapeHtml4(builder.build().toString())+"</pre></div>");
		String curlURL = "\""+(StringEscapeUtils.escapeHtml4(builder.build().toString()).replace("'", "'"))+"\"";
		html.append("<p>CURL: <i>the command is authorized with the current token</i></p><div style='display:block;'><pre style='background-color: #fcf6db;border: 1px solid #e5e0c6; width:1024px; max-height: 400px;overflow-y: auto;'>curl -X GET --header 'Accept: application/json' --header 'Authorization: Bearer "+userContext.getToken().getOid()+"' "+curlURL+"</pre></div>");
		createHTMLswaggerLink(html, method);
		html.append("</fieldset>");
		html.append("<div class=\"footer\"><p>Powered by <a href=\"http://openbouquet.io/\">Open Bouquet</a> <i style='color:white;'>the Analytics Rest API</i></p></div>\n");
	}
	
	private void createHTMLswaggerLink(StringBuilder html, String method) {
		String baseUrl = "";
		try {
			baseUrl = "?url="+URLEncoder.encode(getPublicBaseUriBuilder().path("swagger.json").build().toString(),"UTF-8")+"";
		} catch (UnsupportedEncodingException | IllegalArgumentException | UriBuilderException e) {
			// default
		}
		html.append("<p>the OB Analytics API provides more parameters... check in <a target='swagger' href='http://swagger.squidsolutions.com/"+baseUrl+"#!/analytics/"+method+"'>swagger UI</a> for details</p>");
	}
	
	private ValueType getExpressionValueType(ExpressionAST expr) {
		IDomain image = expr.getImageDomain();
		return ExpressionSuggestionHandler.computeValueTypeFromImage(image);
	}
	
	private String getFieldValue(Object var) {
		if (var==null) return ""; else return var.toString().replaceAll("\"", "&quot;").replaceAll("'", "&#x27;");
	}
	
	private String getFieldValue(Object var, Object defaultValue) {
		if (var==null) return defaultValue.toString(); else return var.toString().replaceAll("\"", "&quot;").replaceAll("'", "&#x27;");
	}
	
	/**
	 * @param uriInfo
	 * @param userContext 
	 * @param localScope 
	 * @param BBID 
	 * @param query
	 * @return
	 * @throws ScopeException 
	 */
	protected URI buildExportURI(AppContext userContext, String BBID, AnalyticsQuery query, String filename) throws ScopeException {
		UriBuilder builder = getPublicBaseUriBuilder().
			path("/analytics/{"+BBID_PARAM_NAME+"}/export/{filename}");
		addAnalyticsQueryParams(builder, query, null, null);
		builder.queryParam("access_token", userContext.getToken().getOid());
		return builder.build(BBID, filename);
	}
	
	private URI buildAnalyticsViewURI(AppContext userContext, ViewQuery query, String data, String envelope, Style style, HashMap<String, Object> override) {
		UriBuilder builder = getPublicBaseUriBuilder().
			path("/analytics/{"+BBID_PARAM_NAME+"}/view");
		addAnalyticsQueryParams(builder, query, style, override);
		if (query.getX()!=null) builder.queryParam(VIEW_X_PARAM, query.getX());
		if (query.getY()!=null) builder.queryParam(VIEW_Y_PARAM, query.getY());
		if (query.getColor()!=null) builder.queryParam(VIEW_COLOR_PARAM, query.getColor());
		if (query.getSize()!=null) builder.queryParam(VIEW_SIZE_PARAM, query.getSize());
		if (query.getColumn()!=null) builder.queryParam(VIEW_COLUMN_PARAM, query.getColumn());
		if (query.getRow()!=null) builder.queryParam(VIEW_ROW_PARAM, query.getRow());
		if (data!=null) builder.queryParam(DATA_PARAM, data);
		if (envelope!=null) builder.queryParam(ENVELOPE_PARAM, envelope);
		builder.queryParam("access_token", userContext.getToken().getOid());
		return builder.build(query.getBBID());
	}
	
	private URI buildAnalyticsQueryURI(AppContext userContext, AnalyticsQuery query, String data, String envelope, Style style, HashMap<String, Object> override) {
		UriBuilder builder = getPublicBaseUriBuilder().
			path("/analytics/{"+BBID_PARAM_NAME+"}/query");
		addAnalyticsQueryParams(builder, query, style, override);
		if (data!=null) builder.queryParam(DATA_PARAM, data);
		if (envelope!=null) builder.queryParam(ENVELOPE_PARAM, envelope);
		builder.queryParam("access_token", userContext.getToken().getOid());
		return builder.build(query.getBBID());
	}
	
	private URI buildAnalyticsExportURI(AppContext userContext, AnalyticsQuery query, String filename) {
		UriBuilder builder = getPublicBaseUriBuilder().
			path("/analytics/{"+BBID_PARAM_NAME+"}/export/{filename}");
		addAnalyticsQueryParams(builder, query, null, null);
		builder.queryParam("access_token", userContext.getToken().getOid());
		return builder.build(query.getBBID(), filename);
	}

	/**
	 * @param override 
	 * @throws ScopeException 
	 * 
	 */
	private void addAnalyticsQueryParams(UriBuilder builder, AnalyticsQuery query, Style style, HashMap<String, Object> override) {
		if (query.getGroupBy()!=null) {
			for (String item : query.getGroupBy()) {
				builder.queryParam(GROUP_BY_PARAM, item);
			}
		}
		if (query.getMetrics()!=null) {
			for (String item : query.getMetrics()) {
				builder.queryParam(METRICS_PARAM, item);
			}
		}
		if (query.getFilters()!=null) {
			for (String item : query.getFilters()) {
				builder.queryParam(FILTERS_PARAM, item);
			}
		}
		if (query.getPeriod()!=null) builder.queryParam(PERIOD_PARAM, query.getPeriod());
		if (query.getTimeframe()!=null) {
			for (String item : query.getTimeframe()) {
				builder.queryParam(TIMEFRAME_PARAM, item);
			}
		}
		if (query.getCompareTo()!=null) {
			for (String item : query.getCompareTo()) {
				builder.queryParam(COMPARETO_PARAM, item);
			}
		}
		if (query.getOrderBy()!=null) {
			for (String item : query.getOrderBy()) {
				builder.queryParam(ORDERBY_PARAM, item);
			}
		}
		if (query.getRollups()!=null) builder.queryParam(ROLLUP_PARAM, query.getRollups());
		// limit override
		if (override!=null && override.containsKey(LIMIT_PARAM)) {
			if (override.get(LIMIT_PARAM)!=null) builder.queryParam(LIMIT_PARAM, override.get(LIMIT_PARAM));
		} else if (query.getLimit()!=null) builder.queryParam(LIMIT_PARAM, query.getLimit());
		if (query.getBeyondLimit()!=null) {
			for (String value : query.getBeyondLimit()) {
				builder.queryParam("beyondLimit", value);
			}
		}
		// maxResults override
		if (override!=null && override.containsKey(MAX_RESULTS_PARAM)) {
			if (override.get(MAX_RESULTS_PARAM)!=null) builder.queryParam(MAX_RESULTS_PARAM, override.get(MAX_RESULTS_PARAM));
		} else if (query.getMaxResults()!=null) builder.queryParam(MAX_RESULTS_PARAM, query.getMaxResults());
		// startIndex override
		if (override!=null && override.containsKey(START_INDEX_PARAM)) {
			if (override.get(START_INDEX_PARAM)!=null) builder.queryParam(START_INDEX_PARAM, override.get(START_INDEX_PARAM));
		} else if (query.getStartIndex()!=null) builder.queryParam(START_INDEX_PARAM, query.getStartIndex());
		//
		if (query.getLazy()!=null) builder.queryParam(LAZY_PARAM, query.getLazy());
		if (style!=null) {
			builder.queryParam(STYLE_PARAM, style);
		} else if (query.getStyle()!=null) builder.queryParam(STYLE_PARAM, query.getStyle());
	}
	
	private Data transformToVegaData(AnalyticsQuery query, DataMatrix matrix, String format) {
		IDataMatrixConverter<Object[]> converter = getConverter(format);
		Data data = new Data();
		data.values = converter.convert(query, matrix);
		return data;
	}
	
	private IDataMatrixConverter<Object[]> getConverter(String format) {
		if (format.equalsIgnoreCase("RECORDS")) {
			return new RecordConverter();
		} else if (format.equalsIgnoreCase("TRANSPOSE")) {
			return new TransposeConverter();
		} else {
			throw new InvalidIdAPIException("invalid format="+format, true);
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
			PerfDB.INSTANCE.save(queryLog);
			return datamatrix;
		}
	}
	
	// Execution management
	
	/**
	 * list the execution status for a given analysis. Note: an analysis can spam multiple queries.
	 * @param request
	 * @param key
	 * @param style 
	 * @return
	 */
	public List<QueryWorkerJobStatus> getStatus(
			AppContext userContext,
			String key) {
		// first check if the query is available
		String customerId = userContext.getCustomerId();
		List<QueryWorkerJobStatus> queries = RedisCacheManager.getInstance().getQueryServer().getOngoingQueries(customerId);
		queries.addAll(DomainHierarchyManager.INSTANCE.getOngoingQueries(customerId));
		List<QueryWorkerJobStatus> results = new ArrayList<>();
		for (QueryWorkerJobStatus query : queries) {
			if (query.getJobID().equals(key)) {
				ProjectPK projectPK = query.getProjectPK();
				try {
					Project project = ProjectManager.INSTANCE.getProject(userContext, projectPK);
					// restrict to privileged user
					if (checkACL(userContext, project, query)) {
						results.add(query);
					}
				} catch (ScopeException e) {
					// ignore
				}
			}
		}
		//
		return results;
	}

	/**
	 * @param userContext
	 * @param key
	 * @return
	 */
	public boolean cancelQuery(AppContext userContext, String key) {
		List<QueryWorkerJobStatus> jobs = getStatus(userContext, key);
		boolean result = true;
		for (QueryWorkerJobStatus job : jobs) {
			if (!RedisCacheManager.getInstance().getQueryServer().cancelOngoingQuery(userContext.getCustomerId(), job.getKey())) {
				result = false;
			}
		}
		return result;
	}

	private boolean checkACL(AppContext userContext, Project project, QueryWorkerJobStatus query) {
		if (AccessRightsUtils.getInstance().hasRole(userContext, project, Role.WRITE)) {
			return true;
		}  else if (AccessRightsUtils.getInstance().hasRole(userContext, project, Role.READ)) {
			// or to the query owner
			if (query.getUserID().equals(userContext.getUser().getOid())) {
				return true;
			}
		}
		// else
		return false;
	}

}
