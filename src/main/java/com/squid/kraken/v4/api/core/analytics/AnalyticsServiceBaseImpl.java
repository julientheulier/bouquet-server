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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPOutputStream;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.codec.binary.Base64;
import org.apache.cxf.jaxrs.impl.UriBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.DefaultScope;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.poi.ExcelFile;
import com.squid.core.poi.ExcelSettingsBean;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.ComputingInProgressAPIException;
import com.squid.kraken.v4.api.core.EngineUtils;
import com.squid.kraken.v4.api.core.InvalidCredentialsAPIException;
import com.squid.kraken.v4.api.core.InvalidIdAPIException;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl.OutputCompression;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl.OutputFormat;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.api.core.bookmark.BookmarkServiceBaseImpl;
import com.squid.kraken.v4.api.core.customer.StateServiceBaseImpl;
import com.squid.kraken.v4.api.core.project.ProjectServiceBaseImpl;
import com.squid.kraken.v4.api.core.projectanalysisjob.AnalysisJobComputer;
import com.squid.kraken.v4.caching.redis.RedisCacheManager;
import com.squid.kraken.v4.caching.redis.queryworkerserver.QueryWorkerJobStatus;
import com.squid.kraken.v4.core.analysis.engine.bookmark.BookmarkManager;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchy;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchyManager;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.SegmentManager;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingService;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.scope.LexiconScope;
import com.squid.kraken.v4.core.analysis.scope.SpaceScope;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.scope.ExpressionSuggestionHandler;
import com.squid.kraken.v4.core.expression.scope.RelationExpressionScope;
import com.squid.kraken.v4.export.ExportSourceWriter;
import com.squid.kraken.v4.export.ExportSourceWriterCSV;
import com.squid.kraken.v4.export.ExportSourceWriterXLSX;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.AnalyticsQuery;
import com.squid.kraken.v4.model.AnalyticsQueryImpl;
import com.squid.kraken.v4.model.AnalyticsReply;
import com.squid.kraken.v4.model.AnalyticsResult;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.BookmarkConfig;
import com.squid.kraken.v4.model.BookmarkFolderPK;
import com.squid.kraken.v4.model.BookmarkPK;
import com.squid.kraken.v4.model.DataLayout;
import com.squid.kraken.v4.model.DataTable;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.ExpressionSuggestion;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.FacetSelection;
import com.squid.kraken.v4.model.GenericPK;
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
import com.squid.kraken.v4.model.ProjectFacetJob;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.ResultInfo;
import com.squid.kraken.v4.model.State;
import com.squid.kraken.v4.model.StatePK;
import com.squid.kraken.v4.model.ValueType;
import com.squid.kraken.v4.model.ViewQuery;
import com.squid.kraken.v4.model.ViewReply;
import com.squid.kraken.v4.persistence.AppContext;

/**
 * @author sergefantino
 *
 */
public class AnalyticsServiceBaseImpl extends AnalyticsServiceCore implements AnalyticsServiceConstants {

	static final Logger logger = LoggerFactory
			.getLogger(AnalyticsServiceBaseImpl.class);

	private UriInfo uriInfo = null;
	
	private URI publicBaseUri = null;
	private AppContext userContext = null;

	private AnalyticsServiceHTMLGenerator generator;
	
	public AnalyticsServiceBaseImpl(UriInfo uriInfo, AppContext userContext) {
		this.uriInfo = uriInfo;
		if (uriInfo != null) {
			this.publicBaseUri = ServiceUtils.getInstance().guessPublicBaseUri(uriInfo);
		}
		this.userContext = userContext;
		this.generator = new AnalyticsServiceHTMLGenerator(this);
	}
	
	/**
	 * @return the userContext
	 */
	public AppContext getUserContext() {
		return userContext;
	}
	
	public UriInfo getUriInfo() {
		return uriInfo;
	}
	
	public UriBuilder getPublicBaseUriBuilder() {
		return new UriBuilderImpl(publicBaseUri);
	}

	protected static final NavigationItem ROOT_FOLDER = new NavigationItem("Root", "list all your available content, organize by Projects and Bookmarks", null, "/", "FOLDER");
	protected static final NavigationItem PROJECTS_FOLDER = new NavigationItem("Projects", "list all your Projects", "/", "/PROJECTS", "FOLDER");
	protected static final NavigationItem SHARED_FOLDER = new NavigationItem("Shared Bookmarks", "list all public bookmarks", "/", "/SHARED", "FOLDER");
	protected static final NavigationItem MYBOOKMARKS_FOLDER = new NavigationItem("My Bookmarks", "list all your bookmarks", "/", "/MYBOOKMARKS", "FOLDER");
	protected static final NavigationItem SHAREDWITHME_FOLDER = new NavigationItem("Shared With Me", "list bookmarks shared with me", "/", "/SHAREDWITHME", "FOLDER");

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
			if (hierarchyMode!=null){
				listProjects(userContext, query, PROJECTS_FOLDER.getSelfRef(), filters, hierarchyMode, result.getChildren());
			}
			result.getChildren().add(createLinkableFolder(userContext, query, SHARED_FOLDER));
			if (hierarchyMode!=null) {
				listSharedBookmarks(userContext, query, SHARED_FOLDER.getSelfRef(), filters, hierarchyMode, result.getChildren());
			}			
			List<Bookmark> myBookmarks = BookmarkManager.INSTANCE.findBookmarksByParent(userContext, BookmarkManager.INSTANCE.getMyBookmarkPath(userContext));
			if(!myBookmarks.isEmpty()){
				result.getChildren().add(createLinkableFolder(userContext, query, MYBOOKMARKS_FOLDER));
				if (hierarchyMode!=null) {
					listMyBookmarks(userContext, query, MYBOOKMARKS_FOLDER.getSelfRef(), filters, hierarchyMode, result.getChildren());
				}
			}		
		} else {
			// need to list parent's content
			if (parent.startsWith(PROJECTS_FOLDER.getSelfRef())) {
				result.setParent(listProjects(userContext, query, parent, filters, hierarchyMode, result.getChildren()));
			} else if (parent.startsWith(SHAREDWITHME_FOLDER.getSelfRef())) {
				// need to check first to avoid going into SHARED
				result.setParent(listSharedWithMeBookmarks(userContext, query, parent, filters, hierarchyMode, result.getChildren()));
			} else if (parent.startsWith(SHARED_FOLDER.getSelfRef())) {
				result.setParent(listSharedBookmarks(userContext, query, parent, filters, hierarchyMode, result.getChildren()));
			} else if (parent.startsWith(MYBOOKMARKS_FOLDER.getSelfRef())) {
				result.setParent(listMyBookmarks(userContext, query, parent, filters, hierarchyMode, result.getChildren()));
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
			return generator.createHTMLPageList(userContext, query, result);
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
	
	private static final List<String> topLevelOrder = Arrays.asList(new String[]{PROJECTS_FOLDER.getSelfRef(), SHARED_FOLDER.getSelfRef(), MYBOOKMARKS_FOLDER.getSelfRef(), SHAREDWITHME_FOLDER.getSelfRef()});
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
			List<Project> projects = ProjectServiceBaseImpl.getInstance().readAll(userContext);
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
		if (query.getHierarchy()!=null) builder.queryParam("hierarchy", query.getHierarchy());
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
			return ProjectManager.INSTANCE.findProjectByName(userContext, projectRef);
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
	private NavigationItem listMyBookmarks(AppContext userContext, NavigationQuery query, String parent, String[] filters, HierarchyMode hierarchyMode, List<NavigationItem> content) throws ScopeException {
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
		listBookmarks(userContext, query, parent, filters, hierarchyMode, fullPath, content);
		return parentFolder;
	}
	
	private NavigationItem listSharedWithMeBookmarks(AppContext userContext, NavigationQuery query, String parent, String[] filters, HierarchyMode hierarchyMode, List<NavigationItem> content) throws ScopeException {
		// list mybookmark related resources
		NavigationItem parentFolder = null;
		if (parent.equals(SHAREDWITHME_FOLDER.getSelfRef())) {
			// just keep the fullpath
			parentFolder = createLinkableFolder(userContext, query, SHAREDWITHME_FOLDER);
		} else {
			// add the remaining path to fullpath
			String name = parent.substring(parent.lastIndexOf("/"));
			String grandParent = parent.substring(0, parent.lastIndexOf("/"));
			parentFolder = new NavigationItem(name, "", grandParent, parent, NavigationItem.FOLDER_TYPE);
		}
		String filterPath = parent.substring(SHAREDWITHME_FOLDER.getSelfRef().length());
		List<Bookmark> bookmarks = findSharedWithMeBookmarks(userContext, filterPath);
		listBookmarks(userContext, query, parent, filters, hierarchyMode, filterPath, bookmarks, content);
		return parentFolder;
	}
	
	/**
	 * list the bookmark shared with me
	 * @param userContext
	 * @param filterPath: this is the remaining part of the path, excluding the root folder prefix
	 * @return
	 */
	private List<Bookmark> findSharedWithMeBookmarks(AppContext userContext, String filterPath) {
		String internalPath = Bookmark.SEPARATOR + Bookmark.Folder.USER;
		String userPath = internalPath + "/"+userContext.getUser().getOid();
		List<Bookmark> bookmarks = BookmarkManager.INSTANCE.findBookmarksByParent(userContext, internalPath);
		Iterator<Bookmark> iter = bookmarks.iterator();
		while (iter.hasNext()) {
			Bookmark bookmark = iter.next();
			if (bookmark.getPath().startsWith(userPath)) {// excluding my bookmarks
				iter.remove();
			} else {
				String subPath = getSubPath(bookmark.getPath());
				if (!subPath.startsWith(filterPath)) {
					iter.remove();
				}
			}
		}
		return bookmarks;
	}
	
	private String getSubPath(String path) {
		// remove the user OID part (first part)
		if (path.startsWith(Bookmark.SEPARATOR + Bookmark.Folder.USER)) {
			path = path.substring((Bookmark.SEPARATOR + Bookmark.Folder.USER).length());
			int pos = path.indexOf("/",1);
			if (pos>=0) {
				return path.substring(pos);
			} else {
				return "";
			}
		} else if (path.startsWith(Bookmark.SEPARATOR + Bookmark.Folder.SHARED)) {
			return path.substring((Bookmark.SEPARATOR + Bookmark.Folder.SHARED).length());
		} else {
			return path;
		}
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
	private NavigationItem listSharedBookmarks(AppContext userContext, NavigationQuery query, String parent, String[] filters, HierarchyMode hierarchyMode, List<NavigationItem> content) throws ScopeException {
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
		listBookmarks(userContext, query, parent, filters, hierarchyMode, fullPath, content);
		return parentFolder;
	}
	
	private void listBookmarks(AppContext userContext, NavigationQuery query, String parent, String[] filters, HierarchyMode hierarchyMode, String fullPath, List<NavigationItem> content) {
		List<Bookmark> bookmarks = BookmarkManager.INSTANCE.findBookmarksByParent(userContext, fullPath);
		String filterPath = getSubPath(fullPath);
		listBookmarks(userContext, query, parent, filters, hierarchyMode, filterPath, bookmarks, content);
	}

	/**
	 * 
	 * @param userContext
	 * @param query
	 * @param parent
	 * @param filters
	 * @param hierarchyMode
	 * @param filterPath
	 * @param content
	 * @throws ScopeException
	 */
	private void listBookmarks(AppContext userContext, NavigationQuery query, String parent, String[] filters, HierarchyMode hierarchyMode, String filterPath, List<Bookmark> bookmarks, List<NavigationItem> content) {
		// list the content first
		HashSet<String> folders = new HashSet<>();
		for (Bookmark bookmark : bookmarks) {
			try {
				Project project = ProjectManager.INSTANCE.getProject(userContext, bookmark.getId().getParent());
				String path = bookmark.getPath();
				String subPath = getSubPath(path);
				// only handle the exact path
				boolean checkParent = (hierarchyMode==HierarchyMode.FLAT)?subPath.startsWith(filterPath):subPath.equals(filterPath);
				if (checkParent) {
					if (filters==null || filter(bookmark, project, filters)) {
						String actualParent = parent;
						if (hierarchyMode==HierarchyMode.FLAT) {
							actualParent = (parent + subPath.substring(filterPath.length()));
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
					String local = subPath.substring(filterPath.length()+1);// remove first /
					String[] split = local.split("/");
					if (split.length>0) {
						String name = "/"+(hierarchyMode!=null?local:split[0]);
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
			} catch (ScopeException e) {
				// ignore
			} catch (InvalidCredentialsAPIException e) {
				// T3019: ignore bookmark if cannot access the parent project
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

	public Bookmark storeBookmark(AppContext userContext, AnalyticsQuery query, String BBID, String stateId, String name, String parent) {
		try {
			Space space = getSpace(userContext, BBID);
			if (query==null) {
				throw new APIException("undefined query");
			}
			String domainID = "@'"+space.getDomain().getOid()+"'";
			if (query.getDomain()==null || query.getDomain()=="") {
				query.setDomain(domainID);
			}
			else if (!query.getDomain().equals(domainID)) {
				throw new APIException("invalid domain definition for the query, doesn't not match the REFERENCE");
			}
			//
			//
			Bookmark bookmark = space.getBookmark();
			if (bookmark == null) {
				// create a new Bookmark
				bookmark = new Bookmark();
				BookmarkPK bookmarkPK = new BookmarkPK(space.getUniverse().getProject().getId());
				bookmark.setId(bookmarkPK);
				if (name==null || name.equals("")) {
					name = space.getDomain().getName()+" Bookmark";
				}
				bookmark.setName(name);
			} else {
				BookmarkConfig originalConfig = BookmarkManager.INSTANCE.readConfig(bookmark);
				// check the state
				if (stateId!=null && !stateId.equals("")) {
					// read the state
					StatePK pk = new StatePK(userContext.getCustomerId(), stateId);
					State state = StateServiceBaseImpl.getInstance().read(userContext, pk);
					BookmarkConfig stateConfig = BookmarkManager.INSTANCE.readConfig(state);
					if (stateConfig!=null) {
						originalConfig = stateConfig;
					}
				}
				//
				// merge the bookmark config with the query
				mergeBookmarkConfig(space, query, originalConfig);
				if (name != null) {
					bookmark.setName(name);
				}
			}

			BookmarkConfig config = createBookmarkConfig(space, query);
			ObjectMapper mapper = new ObjectMapper();
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			String json = mapper.writeValueAsString(config);
			JsonNode tree = mapper.readTree(json);
			bookmark.setConfig(tree);
			String path = "";
			if (parent==null) parent="";
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
		} catch (IOException | InterruptedException | ComputingException e) {
			throw new APIException("cannot create the bookmark: "+e.getMessage());
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
			mergeBookmarkConfig(space, query, config);
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
			String targetID,
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
		Space target = null;
		if (targetID!=null && !targetID.equals("")) target = getSpace(userContext, targetID);
		//
		DefaultScope scope = null;
		if (target==null) {
			scope = new LexiconScope(space);
		} else {
			// check if the target is valid
			if (!target.getUniverse().equals(space.getUniverse())) {
				throw new APIException("invalid target parameter, the Domain does not belong to the space Universe.");
			}
			scope = new RelationExpressionScope(space.getUniverse(), space.getDomain(), target.getDomain());
		}
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
			return generator.createHTMLPageScope(userContext, space, target, suggestions, BBID, value, types, values);
		} else {
			return Response.ok(suggestions).build();
		}
	}

	public Response runAnalysis(
			final AppContext userContext,
			String BBID,
			String stateId, 
			final AnalyticsQuery query, 
			DataLayout data,
			boolean computeGrowth,
			boolean applyFormatting,
			String envelope,
			Integer timeout
			)
	{
		//
		try {
			if (envelope==null) {
				envelope = computeEnvelope(query);
			}
	
			AnalyticsReply reply = super.runAnalysis(userContext, BBID, stateId, query, data, computeGrowth, applyFormatting, timeout);
			//
			if (query.getStyle()==Style.HTML && data==DataLayout.SQL) {
				return generator.createHTMLsql(reply.getResult().toString());
			} else if (query.getStyle()==Style.HTML) {
				return generator.createHTMLPageTable(userContext, reply.getSpace(), reply, (DataTable)reply.getResult());
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
		} catch (TimeoutException e) {
			if (query.getStyle()==Style.HUMAN || query.getStyle()==Style.HTML) {
				URI link = getPublicBaseUriBuilder().path("/status/{queryID}").queryParam("access_token", userContext.getToken().getOid()).build(query.getQueryID());
				throw new ComputingInProgressAPIException("computing in progress", true, timeout*2, query.getQueryID(), link);
			} else {
				throw new ComputingInProgressAPIException("computing in progress", true, timeout*2, query.getQueryID());
			}
		} catch (Throwable e) {
			if (query.getStyle()==Style.HTML) {
				query.add(new Problem(Severity.ERROR, "query", "unable to run the query, fatal error: " + e.getMessage(), e));
				AnalyticsReply reply = new AnalyticsReply();
				reply.setQuery(query);
				return generator.createHTMLPageTable(userContext, getSpace(userContext, BBID), reply, null);
			} else {
				// T3137
				if (e instanceof ScopeException) {
					throw new AnalyticsAPIException(e, 400, query);
				}
				// Make sure runtime exceptions such as auth exceptions are thrown as is
				if (e instanceof RuntimeException) {
					throw (RuntimeException) e;
				} else {
					throw new RuntimeException(e);
				}
			}
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
			mergeBookmarkConfig(space, query, config);
			// create the facet selection
			FacetSelection selection = createFacetSelection(space, query);
			final ProjectAnalysisJob job = createAnalysisJob(space, query, selection, OutputFormat.JSON);
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
	
	public Response viewAnalysis(
			final AppContext userContext, 
			String BBID,
			ViewQuery view,
			String data,
			Style style, 
			String envelope) throws ScopeException, ComputingException, InterruptedException {
		//
		try {
			ViewReply reply = super.viewAnalysis(userContext, BBID, view, data, true, style.equals(Style.HTML));
			//
			Space space = reply.getSpace();
			view = reply.getView();// this is not necessary, but that makes the connection explicit
			ResultInfo info = reply.getResultInfo();
			//
			if (envelope==null) {
				envelope = computeEnvelope(reply.getQuery());
			}
			//
			if (style!=null && style==Style.HTML) {
				return generator.createHTMLPageView(userContext, space, view, info, reply);
			} else if (style!=null && style==Style.SNIPPET) {
				return generator.createHTMLPageViewSnippet(userContext, space, view, info, reply);
			} else if (envelope==null || envelope.equals("") || envelope.equalsIgnoreCase("RESULT")) {
				return Response.ok(reply.getResult(), MediaType.APPLICATION_JSON_TYPE.toString()).build();
			} else if(envelope.equalsIgnoreCase("ALL")) {
				return Response.ok(reply, MediaType.APPLICATION_JSON_TYPE.toString()).build();
			} else {
				throw new InvalidIdAPIException("invalid parameter envelope="+envelope+", must be ALL, RESULT", true);
			}
		} catch (DatabaseServiceException | ComputingException | InterruptedException | ScopeException e) {
			if (style==Style.HTML) {
				view.add(new Problem(Severity.ERROR, "query", "unable to run the query, fatal error: " + e.getMessage(), e));
				ViewReply reply = new ViewReply();
				reply.setQuery(view);
				Space space =  getSpace(userContext, BBID);
				return generator.createHTMLPageView(userContext, space, view, null, reply);
			} else {
				throw new APIException(e.getMessage(), true);
			}
		}
	}

	/**
	 * @param id
	 */
	public URI buildGenericObjectURI(AppContext userContext, GenericPK id) {
		String path = "/rs";
		if (id instanceof ProjectPK) {
			path += "/projects/" + ((ProjectPK)id).getProjectId();
		}
		if (id instanceof DomainPK) {
			path += "/domains/" + ((DomainPK)id).getDomainId();
		}
		UriBuilder builder = getPublicBaseUriBuilder().path(path);
		builder.queryParam("access_token", userContext.getToken().getOid());
		return builder.build();
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
	
	protected URI buildBookmarkURI(AppContext userContext, String BBID) {
		UriBuilder builder = getPublicBaseUriBuilder().
			path("/analytics/{"+BBID_PARAM_NAME+"}/bookmark");
		//addAnalyticsQueryParams(builder, query, null, null);
		builder.queryParam("access_token", userContext.getToken().getOid());
		return builder.build(BBID);
	}
	
	protected URI buildAnalyticsViewURI(AppContext userContext, ViewQuery query, String data, String envelope, Style style, HashMap<String, Object> override) {
		UriBuilder builder = getPublicBaseUriBuilder().
			path("/analytics/{"+BBID_PARAM_NAME+"}/view");
		addAnalyticsQueryParams(builder, query, style, override);
		if (query.getX()!=null) builder.queryParam(VIEW_X_PARAM, query.getX());
		if (query.getY()!=null) builder.queryParam(VIEW_Y_PARAM, query.getY());
		if (query.getColor()!=null) builder.queryParam(VIEW_COLOR_PARAM, query.getColor());
		if (query.getSize()!=null) builder.queryParam(VIEW_SIZE_PARAM, query.getSize());
		if (query.getColumn()!=null) builder.queryParam(VIEW_COLUMN_PARAM, query.getColumn());
		if (query.getRow()!=null) builder.queryParam(VIEW_ROW_PARAM, query.getRow());
		if (query.getOptions()!=null) builder.queryParam(VIEW_OPTIONS_PARAM, query.getOptions());
		if (data!=null) builder.queryParam(DATA_PARAM, data);
		if (envelope!=null) builder.queryParam(ENVELOPE_PARAM, envelope);
		builder.queryParam("access_token", userContext.getToken().getOid());
		return builder.build(query.getBBID());
	}
	@Override
	protected URI buildAnalyticsQueryURI(AppContext userContext, AnalyticsQuery query, String data, String envelope, Style style, HashMap<String, Object> override) {
		UriBuilder builder = getPublicBaseUriBuilder().
			path("/analytics/{"+BBID_PARAM_NAME+"}/query");
		addAnalyticsQueryParams(builder, query, style, override);
		if (data!=null) builder.queryParam(DATA_PARAM, data);
		if (envelope!=null) builder.queryParam(ENVELOPE_PARAM, envelope);
		builder.queryParam("access_token", userContext.getToken().getOid());
		return builder.build(query.getBBID());
	}
	
	protected URI buildAnalyticsExportURI(AppContext userContext, AnalyticsQuery query, String filename) {
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
				String value = item.replaceAll("%", "%25");
				builder.queryParam(FILTERS_PARAM, value);
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
		// rollup
		if (query.getRollups()!=null && !query.getRollups().isEmpty()) {
			for (String rollup : query.getRollups()) {
				builder.queryParam(ROLLUP_PARAM, rollup);
			}
		}
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