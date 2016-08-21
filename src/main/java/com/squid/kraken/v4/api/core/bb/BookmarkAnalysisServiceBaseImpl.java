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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPOutputStream;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.ComputingInProgressAPIException;
import com.squid.kraken.v4.api.core.EngineUtils;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl.OutputCompression;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl.OutputFormat;
import com.squid.kraken.v4.api.core.bb.BookmarkAnalysisServiceRest.HierarchyMode;
import com.squid.kraken.v4.api.core.bb.NavigationQuery.Style;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.api.core.bookmark.BookmarkServiceBaseImpl;
import com.squid.kraken.v4.api.core.projectanalysisjob.AnalysisJobComputer;
import com.squid.kraken.v4.caching.NotInCacheException;
import com.squid.kraken.v4.core.analysis.engine.bookmark.BookmarkManager;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex.Status;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchy;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchyManager;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.SegmentManager;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingService;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.scope.AxisExpression;
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
import com.squid.kraken.v4.model.AnalysisQuery;
import com.squid.kraken.v4.model.AnalysisQueryImpl;
import com.squid.kraken.v4.model.AnalysisResult;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.BookmarkConfig;
import com.squid.kraken.v4.model.BookmarkPK;
import com.squid.kraken.v4.model.DataTable;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Dimension.Type;
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
import com.squid.kraken.v4.model.ObjectType;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectAnalysisJob;
import com.squid.kraken.v4.model.ProjectAnalysisJob.Direction;
import com.squid.kraken.v4.model.ProjectAnalysisJob.OrderBy;
import com.squid.kraken.v4.model.ProjectAnalysisJob.RollUp;
import com.squid.kraken.v4.model.ProjectAnalysisJobPK;
import com.squid.kraken.v4.model.ProjectFacetJob;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.ValueType;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.BookmarkDAO;
import com.squid.kraken.v4.persistence.dao.ProjectDAO;

/**
 * @author sergefantino
 *
 */
public class BookmarkAnalysisServiceBaseImpl {

	static final Logger logger = LoggerFactory
			.getLogger(BookmarkAnalysisServiceBaseImpl.class);
	
	public static final BookmarkAnalysisServiceBaseImpl INSTANCE = new BookmarkAnalysisServiceBaseImpl();
	
	private BookmarkAnalysisServiceBaseImpl() {
	}
	

	private static final NavigationItem PROJECTS_FOLDER = new NavigationItem("Projects", "list all your Dictionaries", "", "/PROJECTS", "FOLDER");
	private static final NavigationItem SHARED_FOLDER = new NavigationItem("Shared Bookmarks", "list all the bookmarks shared with you", "", "/SHARED", "FOLDER");
	private static final NavigationItem MYBOOKMARKS_FOLDER = new NavigationItem("My Bookmarks", "list all your bookmarks", "", "/MYBOOKMARKS", "FOLDER");

	public static final String PROJECT_TYPE = "PROJECT";
	public static final String FOLDER_TYPE = "FOLDER";
	public static final String BOOKMARK_TYPE = "BOOKMARK";
	public static final String DOMAIN_TYPE = "DOMAIN";

	public NavigationReply listContent(
			AppContext userContext,
			String parent,
			String search,
			HierarchyMode hierarchyMode, 
			Style style
		) throws ScopeException {
		List<NavigationItem> content = new ArrayList<>();
		if (parent !=null && parent.endsWith("/")) {
			parent = parent.substring(0, parent.length()-1);// remove trailing /
		}
		NavigationQuery query = new NavigationQuery();
		query.setParent(parent);
		query.setQ(search);
		query.setHiearchy(hierarchyMode);
		query.setStyle(style!=null?style:Style.HUMAN);
		//
		// tokenize the search string
		String[] filters = null;
		if (search!=null) filters = search.toLowerCase().split(",");
		//
		if (parent==null || parent.length()==0) {
			// this is the root
			content.add(PROJECTS_FOLDER);
			if (hierarchyMode!=null) listProjects(userContext, query, PROJECTS_FOLDER.getSelfRef(), filters, hierarchyMode, content);
			content.add(SHARED_FOLDER);
			if (hierarchyMode!=null) listSharedBoomarks(userContext, query, SHARED_FOLDER.getSelfRef(), filters, hierarchyMode, content);
			content.add(MYBOOKMARKS_FOLDER);
			if (hierarchyMode!=null) listMyBoomarks(userContext, query, MYBOOKMARKS_FOLDER.getSelfRef(), filters, hierarchyMode, content);
		} else {
			// need to list parent's content
			if (parent.startsWith(PROJECTS_FOLDER.getSelfRef())) {
				listProjects(userContext, query, parent, filters, hierarchyMode, content);
			} else if (parent.startsWith(SHARED_FOLDER.getSelfRef())) {
				listSharedBoomarks(userContext, query, parent, filters, hierarchyMode, content);
			} else if (parent.startsWith(MYBOOKMARKS_FOLDER.getSelfRef())) {
				listMyBoomarks(userContext, query, parent, filters, hierarchyMode, content);
			} else {
				// invalid
				throw new ObjectNotFoundAPIException("invalid parent reference", true);
			}
		}
		// sort content
		sortNavigationContent(content);
		return new NavigationReply(query, content);
	}
	
	private static final List<String> topLevelOrder = Arrays.asList(new String[]{PROJECTS_FOLDER.getSelfRef(), SHARED_FOLDER.getSelfRef(), MYBOOKMARKS_FOLDER.getSelfRef()});
	private static final List<String> typeOrder = Arrays.asList(new String[]{FOLDER_TYPE, BOOKMARK_TYPE, DOMAIN_TYPE});
	/**
	 * @param content
	 */
	private void sortNavigationContent(List<NavigationItem> content) {
		Collections.sort(content, new Comparator<NavigationItem>() {
			@Override
			public int compare(NavigationItem o1, NavigationItem o2) {
				if (o1.getParentRef()=="" && o2.getParentRef()=="") {
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
	private void listProjects(AppContext userContext, NavigationQuery query, String parent, String[] filters, HierarchyMode hierarchyMode, List<NavigationItem> content) throws ScopeException {
		// list project related resources
		if (parent==null || parent.equals("") || parent.equals("/") || parent.equals(PROJECTS_FOLDER.getSelfRef())) {
			// return available project
			List<Project> projects = ((ProjectDAO) DAOFactory.getDAOFactory().getDAO(Project.class))
					.findByCustomer(userContext, userContext.getCustomerPk());
			for (Project project : projects) {
				if (filters==null || filter(project, filters)) {
					NavigationItem folder = new NavigationItem(query, project, parent);
					HashMap<String, String> attrs = new HashMap<>();
					attrs.put("jdbc", project.getDbUrl());
					folder.setAttributes(attrs);
					content.add(folder);
				}
			}
		} else if (parent.startsWith(PROJECTS_FOLDER.getSelfRef())) {
			String projectRef = parent.substring(PROJECTS_FOLDER.getSelfRef().length()+1);// remove /PROJECTS/ part
			Project project = findProject(userContext, projectRef);
			List<Domain> domains = ProjectManager.INSTANCE.getDomains(userContext, project.getId());
			for (Domain domain : domains) {
				String name = domain.getName();
				if (filters==null || filter(name, filters)) {
					NavigationItem item = new NavigationItem(query, project, domain, parent);
					HashMap<String, String> attrs = new HashMap<>();
					attrs.put("dictionary", project.getName());
					item.setAttributes(attrs);
					content.add(item);
				}
			}
		} else {
			// what's this parent anyway???
		}
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
				if (project.getName().equals(projectRef)) {
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
	 * @throws ScopeException
	 */
	private void listMyBoomarks(AppContext userContext, NavigationQuery query, String parent, String[] filters, HierarchyMode hierarchyMode, List<NavigationItem> content) throws ScopeException {
		// list mybookmark related resources
		String fullPath = getMyBookmarkPath(userContext);
		if (parent.equals(MYBOOKMARKS_FOLDER.getSelfRef())) {
			// just keep the fullpath
		} else {
			// add the remaining path to fullpath
			fullPath += parent.substring(MYBOOKMARKS_FOLDER.getSelfRef().length());
		}
		listBoomarks(userContext, query, parent, filters, hierarchyMode, fullPath, content);
	}
	
	/**
	 * List the Shared bookmarks and folders
	 * @param userContext
	 * @param query 
	 * @param parent
	 * @param isFlat
	 * @param content
	 * @throws ScopeException
	 */
	private void listSharedBoomarks(AppContext userContext, NavigationQuery query, String parent, String[] filters, HierarchyMode hierarchyMode, List<NavigationItem> content) throws ScopeException {
		// list mybookmark related resources
		String fullPath = Bookmark.SEPARATOR + Bookmark.Folder.SHARED;
		if (!parent.equals(SHARED_FOLDER.getSelfRef())) {
			// add the remaining path to fullpath
			fullPath += parent.substring(SHARED_FOLDER.getSelfRef().length());
		}
		listBoomarks(userContext, query, parent, filters, hierarchyMode, fullPath, content);
	}

	private void listBoomarks(AppContext userContext, NavigationQuery query, String parent, String[] filters, HierarchyMode hierarchyMode, String fullPath, List<NavigationItem> content) throws ScopeException {
		// list the content first
		List<Bookmark> bookmarks = ((BookmarkDAO) DAOFactory.getDAOFactory()
				.getDAO(Bookmark.class)).findByPath(userContext, fullPath);
		HashSet<String> folders = new HashSet<>();
		for (Bookmark bookmark : bookmarks) {
			Project project = ProjectManager.INSTANCE.getProject(userContext, bookmark.getId().getParent());
			String path = bookmark.getPath();
			// only handle the exact path
			if (path.equals(fullPath)) {
				String name = bookmark.getName();
				if (filters==null || filter(bookmark, project, filters)) {
					String id = "@'"+bookmark.getId().getProjectId()+"'.[bookmark:'"+bookmark.getId().getBookmarkId()+"']";
					NavigationItem item = new NavigationItem(query.getStyle().equals("HUMAN")?null:bookmark.getId(), name, bookmark.getDescription(), parent, id, BOOKMARK_TYPE);
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
							BookmarkFolderPK id = new BookmarkFolderPK(bookmark.getId().getParent(), oid);
							NavigationItem item = new NavigationItem(query.getStyle().equals("HUMAN")?null:id, name, "", parent, selfpath, FOLDER_TYPE);
							content.add(item);
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
	 * compute the MyBookmark path for the current user
	 * @param ctx
	 */
	private String getMyBookmarkPath(AppContext ctx) {
		return Bookmark.SEPARATOR + Bookmark.Folder.USER
					+ Bookmark.SEPARATOR + ctx.getUser().getOid();
	}

	/**
	 * @param userContext
	 * @param query
	 * @param parent
	 * @param parent2 
	 * @return
	 */
	public Bookmark createBookmark(AppContext userContext, AnalysisQuery query, String BBID, String name, String parent) {
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
				throw new APIException("invalid domain definition for the query, doesn't not match the BBID");
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
				path = getMyBookmarkPath(userContext)+path;
			} else if (parent.startsWith(SHARED_FOLDER.getSelfRef())) {
				path = parent.substring(SHARED_FOLDER.getSelfRef().length());
				path += Bookmark.SEPARATOR + Bookmark.Folder.SHARED + path;
			} else if (!parent.startsWith("/")) {
				path = getMyBookmarkPath(userContext)+"/"+parent;
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
			throw new ObjectNotFoundAPIException("invalid BBID :" + e.getMessage(), true);
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
			throw new ObjectNotFoundAPIException("invalid BBID: "+e.getMessage(), true);
		}
		// else
		throw new ObjectNotFoundAPIException("invalid BBID", true);
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
			AnalysisQuery query = new AnalysisQueryImpl();
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
	
	public AnalysisResult runAnalysis(
			final AppContext userContext,
			String BBID,
			final AnalysisQuery query, 
			Integer timeout
			)
	{
		try {
			Space space = getSpace(userContext, BBID);
			//
			Bookmark bookmark = space.getBookmark();
			BookmarkConfig config = readConfig(bookmark);
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
			AnalysisResult result = new AnalysisResult();
			if (query.getStyle()==Style.LEGACY) {
				// legacy may use the facetSelection
				result.setSelection(selection);
			}
			result.setQuery(query);
			//
			if (query.getFormat().equals("LEGACY")) {
				try {
					Callable<DataTable> task = new Callable<DataTable>() {
						@Override
						public DataTable call() throws Exception {
							return AnalysisJobComputer.INSTANCE.compute(userContext, job, query.getMaxResults(), query.getStartIndex(), lazyFlag);
						}
					};
					Future<DataTable> futur = ExecutionManager.INSTANCE.submit(userContext.getCustomerId(), task);
					DataTable data = null;
					if (timeout==null) {
						// using the customer execution engine to control load
						data = futur.get();
					} else {
						data = futur.get(timeout>1000?timeout:1000, TimeUnit.MILLISECONDS);
					}
					job.setResults(data);
					result.setData(data);
				} catch (NotInCacheException e) {
					if (query.getLazy().equals("noError")) {
						result.setData(new DataTable());
					} else {
						throw e;
					}
				} catch (ExecutionException e) {
					throw new APIException(e);
				} catch (TimeoutException e) {
					throw new ComputingInProgressAPIException("computing in progress", true, timeout*2);
				}
			}
			if (query.getFormat().equals("SQL")) {
				// bypassing the ComputingService
				AnalysisJobComputer computer = new AnalysisJobComputer();
				String sql = computer.viewSQL(userContext, job);
				result.setSQL(sql);
			}
			//
			return result;
		} catch (ComputingException | InterruptedException | ScopeException | SQLScopeException | RenderingException e) {
			throw new APIException(e.getMessage(), true);
		}
	}

	public Response exportAnalysis(
			final AppContext userContext,
			String BBID,
			final AnalysisQuery query,
			String filename,
			String fileext,
			String compression
			)
	{
		try {
			Space space = getSpace(userContext, BBID);
			//
			Bookmark bookmark = space.getBookmark();
			BookmarkConfig config = readConfig(bookmark);
			//
			// merge the bookmark config with the query
			mergeBoomarkConfig(space, query, config);
			// create the facet selection
			FacetSelection selection = createFacetSelection(space, query);
			final ProjectAnalysisJob job = createAnalysisJob(space.getUniverse(), query, selection, OutputFormat.JSON);
			//
			final OutputFormat outFormat;
			if (query.getFormat() == null) query.setFormat(fileext);
			if (query.getFormat() == null) {
				outFormat = OutputFormat.JSON;
			} else {
				outFormat = OutputFormat.valueOf(query.getFormat().toUpperCase());
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
			if (filename==null || filename.equals("")) {
				filename = "job-" + (space.hasBookmark()?space.getBookmark().getName():space.getRoot().getName());
			}
			String mediaType;
			switch (outFormat) {
			case CSV:
				mediaType = "text/csv";
				filename += "."+(fileext!=null?fileext:"csv");
				break;
			case XLS:
				mediaType = "application/vnd.ms-excel";
				filename += "."+(fileext!=null?fileext:"xls");
				break;
			case XLSX:
				mediaType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
				filename += "."+(fileext!=null?fileext:"xlsx");
				break;
			default:
				mediaType = MediaType.APPLICATION_JSON_TYPE.toString();
				filename += "."+(fileext!=null?fileext:"json");
			}

			switch (outCompression) {
			case GZIP:
				// note : setting "Content-Type:application/octet-stream" should
				// disable interceptor's GZIP compression.
				mediaType = MediaType.APPLICATION_OCTET_STREAM_TYPE.toString();
				filename += "."+(compression!=null?compression:"gz");
				break;
			default:
				// NONE
			}

			response = Response.ok(stream);
			// response.header("Content-Type", mediaType);
			if (filename!=null && ((outFormat != OutputFormat.JSON) || (outCompression != OutputCompression.NONE))) {
				logger.info("returning results as " + mediaType + ", fileName : " + filename);
				response.header("Content-Disposition", "attachment; filename=" + filename);
			}

			return response.type(mediaType + "; charset=UTF-8").build();
		} catch (ComputingException | InterruptedException | ScopeException e) {
			throw new APIException(e.getMessage(), true);
		}
	}
	
	private ExportSourceWriter getWriter(OutputFormat outFormat) {
		if (outFormat==OutputFormat.CSV) {
			return new ExportSourceWriterCSV();
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
	private FacetSelection createFacetSelection(Space space, AnalysisQuery query) throws ScopeException {
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

	private ProjectAnalysisJob createAnalysisJob(Universe universe, AnalysisQuery analysis, FacetSelection selection, OutputFormat format) throws ScopeException {
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
				String name = colExpression.getName();
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

		// create
		ProjectAnalysisJobPK pk = new ProjectAnalysisJobPK(universe.getProject().getId(), null);
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
	
	private BookmarkConfig createBookmarkConfig(Space space, AnalysisQuery query) throws ScopeException {
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
	private void mergeBoomarkConfig(Space space, AnalysisQuery analysis, BookmarkConfig config) throws ScopeException, ComputingException, InterruptedException {
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
		if (analysis.getGroupBy() == null) {
			if (config==null) {
				List<String> groupBy = new ArrayList<String>();
				// use a default pivot selection...
				// -- just list the content of the table
				PrettyPrintOptions options = new PrettyPrintOptions(prettyStyle, space.getTop().getImageDomain());
				for (Dimension dimension : space.getDimensions()) {
					Axis axis = space.A(dimension);
					try {
						DimensionIndex index = axis.getIndex();
						if (index!=null && index.isVisible() && index.getStatus()!=Status.ERROR) {
							groupBy.add(axis.prettyPrint(options));
						}
					} catch (ComputingException | InterruptedException e) {
						// ignore this one
					}
				}
				analysis.setGroupBy(groupBy);
			} else if (config.getChosenDimensions() != null) {
				List<String> groupBy = new ArrayList<String>();
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
				analysis.setGroupBy(groupBy);
			}
		}
		boolean metricWildcard = isWildcard(analysis.getMetrics());
		if (analysis.getMetrics() == null || metricWildcard) {
			List<String> metrics = new ArrayList<>();
			if (config==null) {
				// no metrics
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
		case MACHINE:
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
	
	private BookmarkConfig readConfig(Bookmark bookmark) {
		if (bookmark==null) return null;
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		try {
			BookmarkConfig config = mapper.readValue(bookmark.getConfig(), BookmarkConfig.class);
			return config;
		} catch (Exception e) {
			throw new APIException(e);
		}
	}

}
