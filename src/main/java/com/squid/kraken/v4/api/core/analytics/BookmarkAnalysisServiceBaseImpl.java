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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPOutputStream;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.cxf.jaxrs.impl.UriBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.squid.core.concurrent.ExecutionManager;
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
import com.squid.kraken.v4.api.core.JobStats;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl.OutputCompression;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl.OutputFormat;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.api.core.PerfDB;
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
import com.squid.kraken.v4.model.AnalyticsQuery;
import com.squid.kraken.v4.model.AnalyticsQueryImpl;
import com.squid.kraken.v4.model.AnalyticsReply;
import com.squid.kraken.v4.model.AnalyticsResult;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.BookmarkConfig;
import com.squid.kraken.v4.model.BookmarkFolderPK;
import com.squid.kraken.v4.model.BookmarkPK;
import com.squid.kraken.v4.model.DataTable;
import com.squid.kraken.v4.model.DataTable.Col;
import com.squid.kraken.v4.model.DataTable.Row;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.model.NavigationQuery.HierarchyMode;
import com.squid.kraken.v4.model.NavigationQuery.Style;
import com.squid.kraken.v4.model.NavigationQuery.Visibility;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Expression;
import com.squid.kraken.v4.model.ExpressionSuggestion;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.FacetExpression;
import com.squid.kraken.v4.model.FacetMember;
import com.squid.kraken.v4.model.FacetMemberInterval;
import com.squid.kraken.v4.model.FacetMemberString;
import com.squid.kraken.v4.model.FacetSelection;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.NavigationItem;
import com.squid.kraken.v4.model.NavigationQuery;
import com.squid.kraken.v4.model.NavigationReply;
import com.squid.kraken.v4.model.NavigationResult;
import com.squid.kraken.v4.model.ObjectType;
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
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.ProjectDAO;
import com.squid.kraken.v4.vegalite.VegaliteConfigurator;
import com.squid.kraken.v4.vegalite.VegaliteSpecs;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.*;

/**
 * @author sergefantino
 *
 */
public class BookmarkAnalysisServiceBaseImpl implements BookmarkAnalysisServiceConstants {

	static final Logger logger = LoggerFactory
			.getLogger(BookmarkAnalysisServiceBaseImpl.class);

	//private UriInfo uriInfo = null;
	
	private URI publicBaseUri = null;
	
	protected BookmarkAnalysisServiceBaseImpl(UriInfo uriInfo) {
		//this.uriInfo = uriInfo;
		this.publicBaseUri = getPublicBaseUri(uriInfo);
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
				if (!check.getHost().equalsIgnoreCase("auth.openbouquet.io")) {
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

	private static final NavigationItem ROOT_FOLDER = new NavigationItem("Root", "list all your available content", null, "/", "FOLDER");
	private static final NavigationItem PROJECTS_FOLDER = new NavigationItem("Projects", "list all your Dictionaries", "/", "/PROJECTS", "FOLDER");
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
		query.setVisibility(visibility!=null?visibility:Visibility.VISIBLE);
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
		if (search!=null) filters = search.toLowerCase().split(",");
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
		if (envelope.equalsIgnoreCase("ALL")) {
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
						attrs.put("dictionary", project.getName());
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
					}
					HashMap<String, String> attrs = new HashMap<>();
					attrs.put("dictionary", project.getName());
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
	
	public ExpressionSuggestion evaluateExpression(
			AppContext userContext, 
			String BBID,
			String expression,
			Integer offset,
			ObjectType[] types,
			ValueType[] values
			) throws ScopeException
	{
		if (expression==null) expression="";
		Space space = getSpace(userContext, BBID);
		//
		DomainExpressionScope scope = new DomainExpressionScope(space.getUniverse(), space.getDomain());
		//
		ExpressionSuggestionHandler handler = new ExpressionSuggestionHandler(
				scope);
		if (offset == null) {
			offset = expression.length()+1;
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
		ExpressionSuggestion suggestions = handler.getSuggestion(expression, offset, typeFilters, valueFilters);
		return suggestions;
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
		try {
			Space space = getSpace(userContext, BBID);
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
						result.setData(converter.convert(matrix));
						reply.setResult(result);
					}
				} catch (NotInCacheException e) {
					if (query.getLazy().equals("noError")) {
						reply.setResult(new AnalyticsResult());
					} else {
						throw e;
					}
				} catch (ExecutionException e) {
					throw new APIException(e);
				} catch (TimeoutException e) {
					throw new ComputingInProgressAPIException("computing in progress while running queryID="+query.getQueryID(), true, timeout*2);
				}
			}
			//
			if (envelope==null) {
				if (query.getStyle()==Style.HUMAN || query.getStyle()==Style.HTML) {
					envelope = "RESULT";
				} else if (query.getStyle()==Style.LEGACY) {
					envelope = "RESULT";
				} else {//MACHINE
					envelope = "ALL";
				}
			}
			if (query.getStyle()==Style.HTML && data.equalsIgnoreCase("SQL")) {
					return createHTMLsql(reply.getResult().toString());
			} else if (query.getStyle()==Style.HTML && data.equalsIgnoreCase("LEGACY")) {
					return createHTMLtable(space, (DataTable)reply.getResult());
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
		} catch (ComputingException | InterruptedException | ScopeException | SQLScopeException | RenderingException e) {
			throw new APIException(e.getMessage(), true);
		}
	}

	/**
	 * @param dataTable 
	 * @return
	 */
	private Response createHTMLtable(Space space, DataTable data) {
		String title = createHTMLtitle(space);
		StringBuilder html = new StringBuilder("<html><title>Query: "+title+"</title><body>");
		html.append("<h1>"+title+"</h1>");
		html.append("<table><tr>");
		for (Col col : data.getCols()) {
			html.append("<th>"+col.getName()+"</th>");
		}
		html.append("</tr>");
		for (Row row : data.getRows()) {
			html.append("<tr>");
			for (Col col : data.getCols()) {
				Object value = row.getV()[col.getPos()];
				if (col.getFormat()!=null && col.getFormat().length()>0) {
					try {
						value = String.format(col.getFormat(), value);
					} catch (IllegalFormatException e) {
						// ignore
					}
				}
				html.append("<td>"+value+"</td>");
			}
			html.append("</tr>");
		}
		html.append("</table>");
		html.append("<p>rows from "+(data.getStartIndex()+1)+" to "+data.getStartIndex()+data.getRows().size()+" out of "+data.getTotalSize()+" records</p>");
		if (data.isFromCache()) {
			html.append("<p>data from cache, last computed "+data.getExecutionDate()+"</p>");
		} else {
			html.append("<p>fresh data just computed at "+data.getExecutionDate()+"</p>");
		}
		html.append("<hr>powered by Open Bouquet");
		html.append("</body></html>");
		return Response.ok(html.toString(),"text/html").build();
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
				outFormat = OutputFormat.JSON;
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
		if (query.getPeriod()!=null && query.getTimeframe()!=null && query.getTimeframe().length>0) {
			ExpressionAST expr = scope.parseExpression(query.getPeriod());
			Facet facet = createFacetInterval(space, expr, query.getTimeframe());
			selection.getFacets().add(facet);
		}
		// handle compareframe
		if (query.getPeriod()!=null && query.getCompareframe()!=null && query.getCompareframe().length>0) {
			ExpressionAST expr = scope.parseExpression(query.getPeriod());
			Facet compareFacet = createFacetInterval(space, expr, query.getCompareframe());
			selection.setCompareTo(Collections.singletonList(compareFacet));
		}
		// handle filters
		if (query.getFilters() != null) {
			Facet segment = SegmentManager.newSegmentFacet(domain);
			for (String filter : query.getFilters()) {
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
	
	private Facet createFacetInterval(Space space, ExpressionAST expr, String[] frame) throws ScopeException {
		Facet facet = new Facet();
		facet.setId(rewriteExpressionToGlobalScope(expr, space));
		String lowerbound = frame[0];
		String upperbound = frame.length==2?frame[1]:lowerbound;
		FacetMemberInterval member = new FacetMemberInterval(lowerbound, upperbound);
		facet.getSelectedItems().add(member);
		return facet;
	}

	private ProjectAnalysisJob createAnalysisJob(Universe universe, AnalyticsQuery analysis, FacetSelection selection, OutputFormat format) throws ScopeException {
		// read the domain reference
		if (analysis.getDomain() == null) {
			throw new ScopeException("incomplete specification, you must specify the data domain expression");
		}
		Domain domain = getDomain(universe, analysis.getDomain());
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
		if ((analysis.getGroupBy() == null || analysis.getGroupBy().isEmpty())
		&& (analysis.getMetrics() == null || analysis.getMetrics().isEmpty())) {
			throw new ScopeException("there is no defined facet, can't run the analysis");
		}
		// now we are going to use the domain Space scope
		// -- note that it won't limit the actual expression scope to the bookmark scope - but let's keep that for latter
		SpaceScope scope = new SpaceScope(universe.S(domain));
		// quick fix to support the old facet mechanism
		ArrayList<String> analysisFacets = new ArrayList<>();
		if (analysis.getGroupBy()!=null) analysisFacets.addAll(analysis.getGroupBy());
		if (analysis.getMetrics()!=null) analysisFacets.addAll(analysis.getMetrics());
		for (String facet : analysisFacets) {
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
				Axis axis = root.getUniverse().asAxis(colExpression);
				if (axis == null) {
					throw new ScopeException("cannot use expression='" + colExpression.prettyPrint() + "'");
				}
				facets.add(new FacetExpression(axis.prettyPrint(), axis.getName()));
				//
				lookup.put(facetCount, legacyFacetCount++);
				facetCount++;
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
							if (expr instanceof Operator) {
								Operator op = (Operator)expr;
								String id = op.getOperatorDefinition().getExtendedID();
								if (id.equals(SortOperatorDefinition.ASC_ID) || id.equals(SortOperatorDefinition.DESC_ID)) {
									expr = op.getArguments().get(0);
								}
							}
						} else {
							direction = order.getDirection()!=null?order.getDirection():Direction.ASC;
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

		// create the actual job
		// - using the AnalysisQuery.getQueryID() as the job OID: this one is unique for a given query
		ProjectAnalysisJobPK pk = new ProjectAnalysisJobPK(universe.getProject().getId(), analysis.getQueryID());
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
		
		// beyond limit
		if (analysis.getBeyondLimit()!=null && analysis.getBeyondLimit().length>0) {
			if (analysis.getBeyondLimit().length==1) {
				Index index = new Index(analysis.getBeyondLimit()[0]);
				analysisJob.setBeyondLimit(Collections.singletonList(index));
			} else {
				ArrayList<Index> indexes = new ArrayList<>();
				for (int i=0;i<analysis.getBeyondLimit().length;i++) {
					Index index = new Index(analysis.getBeyondLimit()[i]);
					indexes.add(index);
				}
				analysisJob.setBeyondLimit(indexes);
			}
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
			for (OrderBy orderBy : query.getOrderBy()) {
				String value = orderBy.getExpression().getValue();
				OrderBy copy = new OrderBy(new Expression(query.getDomain()+".("+value+")"), orderBy.getDirection());
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
	 * @param analysis
	 * @param config
	 * @throws ScopeException
	 * @throws ComputingException
	 * @throws InterruptedException
	 */
	private void mergeBoomarkConfig(Space space, AnalyticsQuery analysis, BookmarkConfig config) throws ScopeException, ComputingException, InterruptedException {
		ReferenceStyle prettyStyle = getReferenceStyle(analysis.getStyle());
		PrettyPrintOptions prettyOptions = new PrettyPrintOptions(prettyStyle, null);
		UniverseScope globalScope = new UniverseScope(space.getUniverse());
		if (analysis.getDomain() == null) {
			analysis.setDomain(space.prettyPrint(prettyOptions));
		}
		if (analysis.getLimit() == null) {
			if (config!=null) {
				analysis.setLimit(config.getLimit());
			}
		}
		// merging groupBy
		boolean groupbyWildcard = isWildcard(analysis.getGroupBy());
		if (analysis.getGroupBy() == null || groupbyWildcard) {
			List<String> groupBy = new ArrayList<String>();
			if (config==null) {
				// it is not a bookmark, then we will provide default select *
				// only if there is nothing selected at all (groupBy & metrics)
				// or user ask for it explicitly is wildcard
				if (groupbyWildcard || analysis.getMetrics() == null) {
					// use a default pivot selection...
					// -- just list the content of the table
					PrettyPrintOptions options = new PrettyPrintOptions(prettyStyle, space.getTop().getImageDomain());
					for (Dimension dimension : space.getDimensions()) {
						Axis axis = space.A(dimension);
						try {
							DimensionIndex index = axis.getIndex();
							IDomain image = axis.getDefinitionSafe().getImageDomain();
							if (index!=null && index.isVisible() && index.getStatus()!=Status.ERROR && !image.isInstanceOf(IDomain.OBJECT)) {
								groupBy.add(axis.prettyPrint(options));
							}
						} catch (ComputingException | InterruptedException e) {
							// ignore this one
						}
					}
				}
			} else if (config.getChosenDimensions() != null) {
				for (String chosenDimension : config.getChosenDimensions()) {
					String f = null;
					if (chosenDimension.startsWith("@")) {
						// need to fix the scope
						ExpressionAST expr = globalScope.parseExpression(chosenDimension);
						f = rewriteExpressionToLocalScope(expr, space);
					} else {
						f = "@'" + chosenDimension + "'";
					}
					groupBy.add(f);
				}
			}
			if (groupbyWildcard) {
				analysis.getGroupBy().remove(0);// remove the first one
				groupBy.addAll(analysis.getGroupBy());// add reminding
			}
			analysis.setGroupBy(groupBy);
		}
		// merging Metrics
		boolean metricWildcard = isWildcard(analysis.getMetrics());
		if (analysis.getMetrics() == null || metricWildcard) {
			List<String> metrics = new ArrayList<>();
			if (config==null) {
				boolean someIntrinsicMetric = false;
				for (Measure measure : space.M()) {
					Metric metric = measure.getMetric();
					if (metric!=null && !metric.isDynamic()) {
						IDomain image = measure.getDefinitionSafe().getImageDomain();
						if (image.isInstanceOf(IDomain.AGGREGATE)) {
							Measure m = space.M(metric);
							metrics.add(rewriteExpressionToLocalScope(new MeasureExpression(m), space));
							someIntrinsicMetric = true;
						}
					}
				}
				if (!someIntrinsicMetric) {
					metrics.add("count()");
				}
			} else if (config.getChosenMetrics() != null) {
				for (String chosenMetric : config.getChosenMetrics()) {
					metrics.add("@'" + chosenMetric + "'");
				}
			}
			if (metricWildcard) {
				analysis.getMetrics().remove(0);// remove the first one
				metrics.addAll(analysis.getMetrics());// add reminding
			}
			analysis.setMetrics(metrics);
		}
		if (analysis.getOrderBy() == null) {
			if (config!=null && config.getOrderBy()!=null) {
				analysis.setOrderBy(new ArrayList<OrderBy>());
				for (OrderBy orderBy : config.getOrderBy()) {
					ExpressionAST expr = globalScope.parseExpression(orderBy.getExpression().getValue());
					OrderBy copy = new OrderBy(new Expression(rewriteExpressionToLocalScope(expr, space)), orderBy.getDirection());
					analysis.getOrderBy().add(copy);
				}
			}
		}
		if (analysis.getRollups() == null) {
			if (config!=null) {
				analysis.setRollups(config.getRollups());
			}
		}
		//
		// handling selection
		//
		FacetSelection selection = config!=null?config.getSelection():new FacetSelection();
		// handling the period
		if (analysis.getPeriod()==null && config!=null && config.getPeriod()!=null && !config.getPeriod().isEmpty()) {
			// look for this domain period
			String domainID = space.getDomain().getOid();
			String period = config.getPeriod().get(domainID);
			if (period!=null) {
				ExpressionAST expr = globalScope.parseExpression(period);
				IDomain image = expr.getImageDomain();
				if (image.isInstanceOf(IDomain.TEMPORAL)) {
					// ok, it's a date
					analysis.setPeriod(rewriteExpressionToLocalScope(expr, space));
				}
			}
		}
		if (analysis.getPeriod()==null) {
			DomainHierarchy hierarchy = DomainHierarchyManager.INSTANCE.getHierarchy(space.getUniverse().getProject().getId(), space.getDomain(), false);
			for (DimensionIndex index : hierarchy.getDimensionIndexes()) {
				if (index.isVisible() && index.getDimension().getType().equals(Type.CONTINUOUS) && index.getAxis().getDefinitionSafe().getImageDomain().isInstanceOf(IDomain.TEMPORAL)) {
					// use it as period
					Axis axis = index.getAxis();
					AxisExpression expr = new AxisExpression(axis);
					analysis.setPeriod(rewriteExpressionToLocalScope(expr, space));
					if (analysis.getTimeframe()==null) {
						if (index.getStatus()==Status.DONE) {
							analysis.setTimeframe(new String[]{"__CURRENT_MONTH"});
						} else {
							analysis.setTimeframe(new String[]{"__ALL"});
						}
					}
					// quit the loop!
					break;
				}
			}
			if (analysis.getPeriod()==null) {
				// nothing selected - double check and auto detect?
				if (analysis.getTimeframe()!=null && analysis.getTimeframe().length>0) {
					throw new APIException("No period defined: you cannot set the timeframe");
				}
				if (analysis.getCompareframe()!=null && analysis.getCompareframe().length>0) {
					throw new APIException("No period defined: you cannot set the timeframe");
				}
			}
		}
		// handling the selection
		boolean filterWildcard = isWildcardFilters(analysis.getFilters());
		List<String> filters = analysis.getFilters()!=null?new ArrayList<>(analysis.getFilters()):new ArrayList<String>();
		if (filterWildcard) {
			filters.remove(0); // remove the *
		}
		if (!selection.getFacets().isEmpty()) {// always iterate over selection at least to capture the period
			boolean keepConfig = filterWildcard || filters.isEmpty();
			String period = null;
			if (analysis.getPeriod()!=null && analysis.getTimeframe()==null) {
				period = analysis.getDomain()+"."+analysis.getPeriod();
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
									analysis.setTimeframe(new String[]{upperBound});
								} else {
									// it's a date
									String lowerBound = ((FacetMemberInterval) timeframe).getLowerBound();
									analysis.setTimeframe(new String[]{lowerBound,upperBound});
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
									filters.add("'"+member.getValue()+"'");
								}
							}
						}
					} else if (keepConfig) {
						ExpressionAST expr = globalScope.parseExpression(facet.getId());
						String  filter = rewriteExpressionToLocalScope(expr, space);
						if (facet.getSelectedItems().size()==1) {
							if (facet.getSelectedItems().get(0) instanceof FacetMemberString) {
								filter += "=";
								FacetMember member = facet.getSelectedItems().get(0);
								filter += "\""+member.toString()+"\"";
								filters.add(filter);
							}
						} else {
							filter += " IN [";
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
							filter += "]";
							if (!first) {
								filters.add(filter);
							}
						}
					}
				}
			}
		}
		analysis.setFilters(filters);
		//
		// check timeframe again
		if (analysis.getPeriod()!=null && (analysis.getTimeframe()==null || analysis.getTimeframe().length==0)) {
			// add a default timeframe
			analysis.setTimeframe(new String[]{"__CURRENT_MONTH"});
		}
	}
	
	private PrettyPrintOptions.ReferenceStyle getReferenceStyle(Style style) {
		switch (style) {
		case HUMAN:
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
	 * rewrite a global expression (scope into the universe) to a local expression scoped to the given Space
	 * @param expr
	 * @param scope
	 * @return
	 * @throws ScopeException
	 */
	private String rewriteExpressionToLocalScope(ExpressionAST expr, Space space) throws ScopeException {
		ReferenceStyle style = ReferenceStyle.LEGACY;
		PrettyPrintOptions options = new PrettyPrintOptions(style, space.getTop().getImageDomain());
		return expr.prettyPrint(options);
		/*
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
		*/
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
			UriInfo uriInfo, 
			final AppContext userContext, 
			String BBID,
			ViewQuery view,
			String data,
			Style style, 
			String envelope,
			AnalyticsQuery query) throws ScopeException, ComputingException, InterruptedException {
		Space space = getSpace(userContext, BBID);
		//
		if (data==null) data="URL";
		boolean preFetch = false;// default to prefetch when data mode is URL
		//
		Bookmark bookmark = space.getBookmark();
		BookmarkConfig config = BookmarkManager.INSTANCE.readConfig(bookmark);
		//
		// handle the limit
		Long explicitLimit = query.getLimit();
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
				// not a bookmark, use default if nothing provided
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
					if (!query.getGroupBy().get(next).equals("__PERIOD") && !inputConfig.getRequired().getGroupBy().contains(query.getGroupBy().get(next))) {
						if (view.getColor()==null) {
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
					int next = 0;
					while (next<dims) {
						if (view.getX()==null) {
							view.setX(query.getGroupBy().get(next++));
						} else if (view.getColor()==null) {
							// use it as the column
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
			query.setBeyondLimit(new int[]{outputConfig.getTimeseriesPosition()});
		}
		// make sure we order by something
		if (query.getOrderBy()==null || query.getOrderBy().size()==0) {
			if (query.getMetrics().size()>0) {
				ExpressionAST ast = outputConfig.parse(query.getMetrics().get(0));
				query.setOrderBy(Collections.singletonList(new OrderBy(outputConfig.prettyPrint(ast), Direction.DESC)));
			} else {
				query.setOrderBy(Collections.singletonList(new OrderBy("count()", Direction.DESC)));
			}
		}
		//
		// create the facet selection
		FacetSelection selection = createFacetSelection(space, query);
		final ProjectAnalysisJob job = createAnalysisJob(space.getUniverse(), query, selection, OutputFormat.JSON);
		//
		// handling data
		if (data.equals("EMBEDED")) {
			DataMatrix table = compute(userContext, job, query.getMaxResults(), query.getStartIndex(), false);
			if (!outputConfig.isHasMetricSeries()) {
				specs.data = transformToVegaData(table, "RECORDS");
			} else {
				specs.data = transformToVegaData(table, "TRANSPOSE");
			}
		} else if (data.equals("URL")) {
			if (preFetch) {
				// run the query
				Callable<DataMatrix> task = new Callable<DataMatrix>() {
					@Override
					public DataMatrix call() throws Exception {
						return compute(userContext, job, null, null, false);
					}
				};
				// execute the task, no need to wait for result
				ExecutionManager.INSTANCE.submit(userContext.getCustomerId(), task);
			}
			specs.data = new Data();
			if (!outputConfig.isHasMetricSeries()) {
				specs.data.url = buildAnalyticsQueryURI(uriInfo, userContext, outputConfig.getScope(), query, "RECORDS", "DATA").toString();
			} else {
				specs.data.url = buildAnalyticsQueryURI(uriInfo, userContext, outputConfig.getScope(), query, "TRANSPOSE", "DATA").toString();
			}
			specs.data.format = new Format();
			specs.data.format.type = FormatType.json;// lowercase only!
		} else {
			throw new APIException("undefined value for data parameter, must be EMBEDED or URL");
		}
		//
		if (outputConfig.isTimeseries()) {
			specs.mark = Mark.line;
		} else {
			specs.mark = Mark.bar;
		}
		//
		ViewReply reply = new ViewReply();
		reply.setQuery(query);
		reply.setResult(specs);
		//
		if (style!=null && style==Style.HTML) {
			return createHTMLView(space, view, reply);
		} else if (envelope==null || envelope.equals("") || envelope.equalsIgnoreCase("RESULT")) {
			return Response.ok(reply.getResult(), MediaType.APPLICATION_JSON_TYPE.toString()).build();
		} else if(envelope.equalsIgnoreCase("ALL")) {
			return Response.ok(reply, MediaType.APPLICATION_JSON_TYPE.toString()).build();
		} else {
			throw new InvalidIdAPIException("invalid parameter envelope="+envelope+", must be ALL, RESULT", true);
		}
	}
	
	private String createHTMLtitle(Space space) {
		if (space.hasBookmark()) {
			return space.getBookmark().getPath()+"/"+space.getBookmark().getName();
		} else {
			return space.getUniverse().getProject().getName()+" > "+space.getDomain().getName();
		}
	}
	
	private Response createHTMLView(Space space, ViewQuery view, ViewReply reply) {
		String title = createHTMLtitle(space);
		String html = "<html><title>View: "+title+"</title>\r\n<script src=\"http://d3js.org/d3.v3.min.js\" charset=\"utf-8\"></script>\r\n<script src=\"http://vega.github.io/vega/vega.js\" charset=\"utf-8\"></script>\r\n<script src=\"http://vega.github.io/vega-lite/vega-lite.js\" charset=\"utf-8\"></script>\r\n<script src=\"http://vega.github.io/vega-editor/vendor/vega-embed.js\" charset=\"utf-8\"></script>\r\n\r\n<body>\r\n<h1>"+title+"</h1>\r\n<div id=\"vis\"></div>\r\n\r\n<script>\r\nvar embedSpec = {\r\n  mode: \"vega-lite\",\r\n  spec:";
		ObjectMapper mapper = new ObjectMapper();
		try {
			html += mapper.writeValueAsString(reply.getResult());
		} catch (JsonProcessingException e) {
			throw new APIException("failed to write vegalite specs to JSON", e, true);
		}
		Encoding channels = reply.getResult().encoding;
		html += "}\r\nvg.embed(\"#vis\", embedSpec, function(error, result) {\r\n  // Callback receiving the View instance and parsed Vega spec\r\n  // result.view is the View, which resides under the '#vis' element\r\n});\r\n</script>\r\n"
				+ "<form>"
				+ "<table>"
				+ "<tr><td>x</td><td>=<input type=\"text\" name=\"x\" value=\""+getFieldValue(view.getX())+"\"></td><td>"+(channels.x!=null?"as <b>"+channels.x.field+"</b>":"")+"</td></tr>"
				+ "<tr><td>y</td><td>=<input type=\"text\" name=\"y\" value=\""+getFieldValue(view.getY())+"\"></td><td>"+(channels.y!=null?"as <b>"+channels.y.field+"</b>":"")+"</td></tr>"
				+ "<tr><td>color</td><td>=<input type=\"text\" name=\"color\" value=\""+getFieldValue(view.getColor())+"\"></td><td>"+(channels.color!=null?"as <b>"+channels.color.field+"</b>":"")+"</td></tr>"
				+ "<tr><td>size</td><td>=<input type=\"text\" name=\"size\" value=\""+getFieldValue(view.getSize())+"\"></td><td>"+(channels.size!=null?"as <b>"+channels.size.field+"</b>":"")+"</td></tr>"
				+ "<tr><td>column</td><td>=<input type=\"text\" name=\"column\" value=\""+getFieldValue(view.getColumn())+"\"></td><td>"+(channels.column!=null?"as <b>"+channels.column.field+"</b>":"")+"</td></tr>"
				+ "<tr><td>row</td><td>=<input type=\"text\" name=\"row\" value=\""+getFieldValue(view.getRow())+"\"></td><td>"+(channels.row!=null?"as <b>"+channels.row.field+"</b>":"")+"</td></tr>"
				+ "</table>"
				+ "<input type=\"hidden\" name=\"style\" value=\"HTML\">"
				+ "<input type=\"hidden\" name=\"access_token\" value=\""+space.getUniverse().getContext().getToken().getOid()+"\">"
				+ "<input type=\"submit\" value=\"Refresh\">"
				+ "</form>"
				+ "<p>the OB Analytics API provides more parameters... check <a target='swagger' href='http://swagger.squidsolutions.com/#!/analytics/viewAnalysis'>swagger UI</a> for details</p>"
				+ "<hr>powered by Open Bouquet & VegaLite"
				+ "</body>\r\n</html>";
		return Response.ok(html, "text/html; charset=UTF-8").build();
	}
	
	private String getFieldValue(String var) {
		if (var==null) return ""; else return var;
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
	protected URI buildExportURI(UriInfo uriInfo, AppContext userContext, SpaceScope localScope, String BBID, AnalyticsQuery query, String filename) throws ScopeException {
		UriBuilder builder = uriInfo.getBaseUriBuilder().
			path("/analytics/{"+BBID_PARAM_NAME+"}/export/{filename}");
		addAnalyticsQueryParams(builder, localScope, query);
		builder.queryParam("access_token", userContext.getToken().getOid());
		return builder.build(BBID, filename);
	}
	
	private URI buildAnalyticsQueryURI(UriInfo uriInfo, AppContext userContext, SpaceScope localScope, AnalyticsQuery query, String data, String envelope) throws ScopeException {
		UriBuilder builder = uriInfo.getBaseUriBuilder().
			path("/analytics/{"+BBID_PARAM_NAME+"}/query");
		addAnalyticsQueryParams(builder, localScope, query);
		builder.queryParam(DATA_PARAM, data).queryParam(ENVELOPE_PARAM, envelope);
		builder.queryParam("access_token", userContext.getToken().getOid());
		return builder.build(query.getBBID());
	}

	/**
	 * @throws ScopeException 
	 * 
	 */
	private void addAnalyticsQueryParams(UriBuilder builder, SpaceScope localScope, AnalyticsQuery query) throws ScopeException {
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
		if (query.getCompareframe()!=null) {
			for (String item : query.getCompareframe()) {
				builder.queryParam(COMPARETO_PARAM, item);
			}
		}
		if (query.getOrderBy()!=null) {
			for (OrderBy item : query.getOrderBy()) {
				ExpressionAST check = localScope.parseExpression(item.getExpression().getValue());
				IDomain image = check.getImageDomain();
				if (!image.isInstanceOf(DomainSort.DOMAIN)) {
					// need to add the sort function
					String expr = item.getDirection()+"("+item.getExpression().getValue()+")";
					builder.queryParam(ORDERBY_PARAM, expr);
				} else {
					builder.queryParam(ORDERBY_PARAM, item.getExpression().getValue());
				}
			}
		}
		if (query.getRollups()!=null) builder.queryParam(ROLLUP_PARAM, query.getRollups());
		if (query.getLimit()!=null) builder.queryParam(LIMIT_PARAM, query.getLimit());
		if (query.getBeyondLimit()!=null) {
			for (int index : query.getBeyondLimit()) {
				builder.queryParam("beyondLimit", index);
			}
		}
		if (query.getMaxResults()!=null) builder.queryParam(MAX_RESULTS_PARAM, query.getMaxResults());
		if (query.getStartIndex()!=null) builder.queryParam(START_INDEX_PARAM, query.getStartIndex());
		if (query.getLazy()!=null) builder.queryParam(LAZY_PARAM, query.getLazy());
		if (query.getStyle()!=null) builder.queryParam(STYLE_PARAM, query.getStyle());
	}
	
	private Data transformToVegaData(DataMatrix matrix, String format) {
		IDataMatrixConverter<Object[]> converter = getConverter(format);
		Data data = new Data();
		data.values = converter.convert(matrix);
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
			DataTable res = datamatrix.toDataTable(ctx, maxResults, startIndex, false, job.getOptionKeys());
			logger.debug("Is result set in REDIS complete? " + res.getFullset());
			return datamatrix;
		}
	}
	
	// Execution management
	
	/**
	 * list the execution status for a given analysis. Note: an analysis can spam multiple queries.
	 * @param request
	 * @param key
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
