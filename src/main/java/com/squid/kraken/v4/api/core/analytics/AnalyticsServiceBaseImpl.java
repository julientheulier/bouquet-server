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
import java.sql.Types;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import com.squid.core.concurrent.ExecutionManager;
import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.domain.DomainNumericConstant;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.analytics.AnalyticDomain;
import com.squid.core.domain.operators.ExtendedType;
import com.squid.core.domain.operators.IntrinsicOperators;
import com.squid.core.domain.operators.OperatorDefinition;
import com.squid.core.domain.sort.DomainSort;
import com.squid.core.domain.sort.DomainSort.SortDirection;
import com.squid.core.domain.sort.SortOperatorDefinition;
import com.squid.core.domain.vector.VectorOperatorDefinition;
import com.squid.core.expression.ConstantValue;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.NumericConstant;
import com.squid.core.expression.Operator;
import com.squid.core.expression.PrettyPrintOptions;
import com.squid.core.expression.PrettyPrintOptions.ReferenceStyle;
import com.squid.core.expression.scope.DefaultScope;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.poi.ExcelFile;
import com.squid.core.poi.ExcelSettingsBean;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.SQLSkin;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.ComputingInProgressAPIException;
import com.squid.kraken.v4.api.core.EngineUtils;
import com.squid.kraken.v4.api.core.InvalidCredentialsAPIException;
import com.squid.kraken.v4.api.core.InvalidIdAPIException;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl.OutputCompression;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl.OutputFormat;
import com.squid.kraken.v4.api.core.JobStats;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.api.core.bookmark.BookmarkServiceBaseImpl;
import com.squid.kraken.v4.api.core.customer.StateServiceBaseImpl;
import com.squid.kraken.v4.api.core.project.ProjectServiceBaseImpl;
import com.squid.kraken.v4.api.core.projectanalysisjob.AnalysisJobComputer;
import com.squid.kraken.v4.caching.NotInCacheException;
import com.squid.kraken.v4.caching.redis.RedisCacheManager;
import com.squid.kraken.v4.caching.redis.queryworkerserver.QueryWorkerJobStatus;
import com.squid.kraken.v4.core.analysis.datamatrix.AxisValues;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.datamatrix.IDataMatrixConverter;
import com.squid.kraken.v4.core.analysis.datamatrix.MeasureValues;
import com.squid.kraken.v4.core.analysis.datamatrix.RecordConverter;
import com.squid.kraken.v4.core.analysis.datamatrix.TableConverter;
import com.squid.kraken.v4.core.analysis.datamatrix.TransposeConverter;
import com.squid.kraken.v4.core.analysis.engine.bookmark.BookmarkManager;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex.Status;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchy;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchyManager;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.FacetBuilder;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.SegmentManager;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingService;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.model.DashboardAnalysis;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.model.DomainSelection;
import com.squid.kraken.v4.core.analysis.model.ExpressionInput;
import com.squid.kraken.v4.core.analysis.model.Intervalle;
import com.squid.kraken.v4.core.analysis.scope.AxisExpression;
import com.squid.kraken.v4.core.analysis.scope.GlobalExpressionScope;
import com.squid.kraken.v4.core.analysis.scope.LexiconScope;
import com.squid.kraken.v4.core.analysis.scope.MeasureExpression;
import com.squid.kraken.v4.core.analysis.scope.SpaceExpression;
import com.squid.kraken.v4.core.analysis.scope.SpaceScope;
import com.squid.kraken.v4.core.analysis.scope.UniverseScope;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.reference.DomainReference;
import com.squid.kraken.v4.core.expression.scope.ExpressionSuggestionHandler;
import com.squid.kraken.v4.core.expression.scope.RelationExpressionScope;
import com.squid.kraken.v4.core.model.domain.DomainDomain;
import com.squid.kraken.v4.export.ExportSourceWriter;
import com.squid.kraken.v4.export.ExportSourceWriterCSV;
import com.squid.kraken.v4.export.ExportSourceWriterXLSX;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.AnalyticsQuery;
import com.squid.kraken.v4.model.AnalyticsQueryImpl;
import com.squid.kraken.v4.model.AnalyticsReply;
import com.squid.kraken.v4.model.AnalyticsResult;
import com.squid.kraken.v4.model.AnalyticsSelection;
import com.squid.kraken.v4.model.AnalyticsSelectionImpl;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.BookmarkConfig;
import com.squid.kraken.v4.model.BookmarkFolderPK;
import com.squid.kraken.v4.model.BookmarkPK;
import com.squid.kraken.v4.model.DataHeader;
import com.squid.kraken.v4.model.DataHeader.Column;
import com.squid.kraken.v4.model.DataLayout;
import com.squid.kraken.v4.model.DataTable;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.Expression;
import com.squid.kraken.v4.model.ExpressionSuggestion;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.FacetExpression;
import com.squid.kraken.v4.model.FacetMember;
import com.squid.kraken.v4.model.FacetMemberInterval;
import com.squid.kraken.v4.model.FacetMemberString;
import com.squid.kraken.v4.model.FacetSelection;
import com.squid.kraken.v4.model.GenericPK;
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
import com.squid.kraken.v4.model.ProjectAnalysisJob.Position;
import com.squid.kraken.v4.model.ProjectAnalysisJob.RollUp;
import com.squid.kraken.v4.model.ProjectAnalysisJobPK;
import com.squid.kraken.v4.model.ProjectFacetJob;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.ResultInfo;
import com.squid.kraken.v4.model.State;
import com.squid.kraken.v4.model.StatePK;
import com.squid.kraken.v4.model.ValueType;
import com.squid.kraken.v4.model.ViewQuery;
import com.squid.kraken.v4.model.ViewReply;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.vegalite.VegaliteConfigurator;
import com.squid.kraken.v4.vegalite.VegaliteSpecs;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Data;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.DataType;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Format;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.FormatType;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Mark;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Operation;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Order;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Sort;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Stacked;

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
			if (query.getStyle()==Style.HTML) {
				// change data format to legacy
				data=DataLayout.LEGACY;
			} else if (data==null) {
				data=DataLayout.TABLE;
			}
			//
			AnalyticsReply reply = super.runAnalysis(userContext, BBID, stateId, query, data, computeGrowth, applyFormatting, timeout);
			//
			if (query.getStyle()==Style.HTML && data==DataLayout.SQL) {
				return generator.createHTMLsql(reply.getResult().toString());
			} else if (query.getStyle()==Style.HTML && data==DataLayout.LEGACY) {
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
		// set the project ID
		config.setProject(space.getUniverse().getProject().getOid());
		// config use the Domain OID
		config.setDomain(space.getDomain().getOid());
		//config.setSelection();
		config.setLimit(query.getLimit());
		//
		if (query.getPeriod()!=null && !query.getPeriod().equals("")) {
			config.setPeriod(null);
		}
		//
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
				ExpressionAST expr = scope.parseExpression(facet);
				choosenMetrics.add(rewriteChoosenMetric(expr));
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
				expr = unwrapOrderByExpression(expr);
				OrderBy copy = new OrderBy(new Expression(rewriteExpressionToGlobalScope(expr, space)), direction);
				config.getOrderBy().add(copy);
			}
		}
		//
		if (query.getRollups() != null) {
			config.setRollups(parseRollups(query.getRollups()));
		}
		// add the selection
		FacetSelection selection = createFacetSelection(space, query);
		config.setSelection(selection);
		// period and compareTo
		if (query.getPeriod()!=null && !query.getPeriod().equals("")) {
			// add the period into the hashMap....
			HashMap<String, String> map = new HashMap<>();
			ExpressionAST expr = scope.parseExpression(query.getPeriod());
			map.put(config.getDomain(), rewriteExpressionToGlobalScope(expr, space));
			config.setPeriod(map);
		}
		//
		config.setCurrentAnalysis(BookmarkConfig.TABLE_ANALYSIS);
		return config;
	}
	
	/**
	 * if the orderBy expression is DESC(x) or ASC(x), just unwrap and return x
	 * else do nothing
	 * @param orderBy
	 * @return
	 */
	private ExpressionAST unwrapOrderByExpression(ExpressionAST expr) {
		if (expr.getImageDomain().isInstanceOf(DomainSort.DOMAIN) && expr instanceof Operator) {
			// remove the first operator
			Operator op = (Operator)expr;
			if (op.getArguments().size()==1 
					&& (op.getOperatorDefinition().getExtendedID().equals(SortOperatorDefinition.ASC_ID)
					|| op.getOperatorDefinition().getExtendedID().equals(SortOperatorDefinition.DESC_ID))) 
			{
				return op.getArguments().get(0);
			}
		}	
		// else do nothing
		return expr;
	}
	
	private String rewriteChoosenMetric(ExpressionAST expr) {
		if (expr instanceof MeasureExpression) {
			MeasureExpression measure = (MeasureExpression)expr;
			if (measure.getMeasure().getMetric()!=null) {
				return measure.getMeasure().getMetric().getOid();
			}
		}
		// else
		return expr.prettyPrint(PrettyPrintOptions.ROBOT_GLOBAL);
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
	private void mergeBookmarkConfig(Space space, AnalyticsQuery query, BookmarkConfig config) throws ScopeException, ComputingException, InterruptedException {
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
						query.setTimeframe(new ArrayList<>());
						if (index.getStatus()==Status.DONE) {
							query.getTimeframe().add("__CURRENT_MONTH");
						} else {
							query.getTimeframe().add("__ALL");
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
				/*
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
				*/
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
				// T3036 - only keep the count()
				boolean someIntrinsicMetric = false;
				/*
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
				*/
				if (!someIntrinsicMetric) {
					metrics.add("count() as 'Count'// default metric");
				}
			} else if (config.getChosenMetrics() != null) {
				for (String chosenMetric : config.getChosenMetrics()) {
					// parse to validate and reprint
					try {
						// this is for legacy compatibility...
						ExpressionAST expr = localScope.parseExpression("@'" + chosenMetric + "'");
						metrics.add(expr.prettyPrint(localOptions));
					} catch (ScopeException e) {
						try {
							ExpressionAST expr = globalScope.parseExpression(chosenMetric);
							metrics.add(expr.prettyPrint(localOptions));
						} catch (ScopeException ee) {
							query.add(new Problem(Severity.WARNING, chosenMetric, "failed to parse bookmark metric: " + ee.getMessage(), ee));
						}
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
			if (config!=null && config.getRollups()!=null && !config.getRollups().isEmpty()) {
				query.setRollups(new ArrayList<>());
				for (RollUp rollup : config.getRollups()) {
					query.getRollups().add(rollup.toString());
				}
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
		String period = null;
		if (query.getPeriod()!=null && query.getTimeframe()==null) {
			ExpressionAST expr = localScope.parseExpression(query.getPeriod());
			period = expr.prettyPrint(new PrettyPrintOptions(ReferenceStyle.IDENTIFIER, null));
		}
		
		if (selection != null) {
			if (!selection.getFacets().isEmpty()) {// always iterate over selection at least to capture the period
				boolean keepConfig = filterWildcard || filters.isEmpty();
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
			// check compareTo

			if (!selection.getCompareTo().isEmpty()) {
				for (Facet facet : selection.getCompareTo()) {
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
										query.setCompareTo(Collections.singletonList(upperBound));
									} else {
										// it's a date
										String lowerBound = ((FacetMemberInterval) timeframe).getLowerBound();
										query.setCompareTo(new ArrayList<String>(2));
										query.getCompareTo().add(lowerBound);
										query.getCompareTo().add(upperBound);
									}
								}
							}
						}
					}
				}
			}
			//
			// check timeframe again
			if (query.getPeriod()!=null && (query.getTimeframe()==null || query.getTimeframe().size()==0)) {
				// add a default timeframe
				query.setTimeframe(Collections.singletonList("__CURRENT_MONTH"));
			}
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
	 * @param options 
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
		Space space = null;
		try {
			space = getSpace(userContext, BBID);
			//
			if (data==null) data="URL";
			boolean preFetch = true;// default to prefetch when data mode is URL
			//
			Bookmark bookmark = space.getBookmark();
			BookmarkConfig config = BookmarkManager.INSTANCE.readConfig(bookmark);
			//
			// handle the limit
			Long explicitLimit = view.getLimit();
			AnalyticsQueryImpl query = new AnalyticsQueryImpl(view);
			// merge the bookmark config with the query
			mergeBookmarkConfig(space, query, config);
			//
			// change the query ref to use the domain one
			// - we don't want to have side-effects
			String domainBBID = "@'"+space.getUniverse().getProject().getOid()+"'.@'"+space.getDomain().getOid()+"'";
			query.setBBID(domainBBID);
			//
			Properties options = view.getOptionsAsPropertiesSafe();
			//
			VegaliteConfigurator inputConfig = new VegaliteConfigurator(space, query);
			// first check the provided parameters, because they must override the default
			VegaliteSpecs channels = new VegaliteSpecs();
			channels.encoding.x = inputConfig.createChannelDef("x", view.getX(), options);
			channels.encoding.y = inputConfig.createChannelDef("y", view.getY(), options);
			channels.encoding.color = inputConfig.createChannelDef("color", view.getColor(), options);
			channels.encoding.size = inputConfig.createChannelDef("size", view.getSize(), options);
			channels.encoding.column = inputConfig.createChannelDef("column", view.getColumn(), options);
			channels.encoding.row = inputConfig.createChannelDef("row", view.getRow(), options);
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
			// generate a default dataviz if x and y are not already sets
			if (view.getX()==null || view.getY()==null) {
				// DOMAIN
				if (config==null) {
					// not a bookmark, use default & specs if nothing provided
					// T1935: if explicit groupBy, use it
					boolean metricsDone = false;
					if (view.getGroupBy()!=null && view.getGroupBy().size()>0 && query.getGroupBy()!=null && query.getGroupBy().size()>0) {
						int next = 0;
						// reorder the dims to have the period first
						ArrayList<Integer> reorder = new ArrayList<>();
						for (int i=0;i<dims;i++) {
							String dim = query.getGroupBy().get(i);
							ExpressionAST expr = inputConfig.parse(dim);
							boolean isTemporal = expr.getImageDomain().isInstanceOf(IDomain.TEMPORAL);
							if (isTemporal) {
								reorder.add(0, i);// put it in front
							} else {
								reorder.add(i);
							}
						}
						while (next<dims) {
							if (next==1) {
								// insert the metrics after the first dimension
								metricsDone = handleMetrics(query, inputConfig, view, options, channels, true);
							}
							String dim = query.getGroupBy().get(reorder.get(next++));
							if (!inputConfig.getRequired().getGroupBy().contains(dim)) {
								ExpressionAST expr = inputConfig.parse(dim);
								boolean isTemporal = expr.getImageDomain().isInstanceOf(IDomain.TEMPORAL);
								if ((view.getX()==null || channels.encoding.x.type==DataType.quantitative) && view.getY()==null && !isTemporal) {// use Y only for categories and left a channel for the metrics
									// use it as the y
									view.setY(dim);
									channels.encoding.y = inputConfig.createChannelDef("y", view.getY(), options);
								} else if ((view.getY()==null || channels.encoding.y.type==DataType.quantitative) && view.getX()==null) {
									// use it as the x
									view.setX(dim);
									channels.encoding.x = inputConfig.createChannelDef("x", view.getX(), options);
								} else if (view.getColor()==null) {
									// use it as the color
									view.setColor(dim);
									channels.encoding.color = inputConfig.createChannelDef("color", view.getColor(), options);
								} else if (view.getColumn()==null) {
									// use it as the column
									view.setColumn(dim);
									channels.encoding.column = inputConfig.createChannelDef("column", view.getColumn(), options);
								} else if (view.getRow()==null) {
									// use it as the column
									view.setRow(dim);
									channels.encoding.row = inputConfig.createChannelDef("row", view.getRow(), options);
								} else {
									break;// no more channel available
								}
							}
						}
					} else {
						// add the period if nothing is selected
						if (view.getX()==null) {
							view.setX("daily(__PERIOD)");
							channels.encoding.x = inputConfig.createChannelDef("x", view.getX(), options);
						}
					}
					if (!metricsDone) {
						handleMetrics(query, inputConfig, view, options, channels, false);
					}
					// add the period if no group set
					if (view.getX()==null && !inputConfig.isTimeseries() && query.getPeriod()!=null) {
						view.setX("__PERIOD");
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
			specs.encoding.x = outputConfig.createChannelDef("x", view.getX(), options);
			specs.encoding.y = outputConfig.createChannelDef("y", view.getY(), options);
			specs.encoding.color = outputConfig.createChannelDef("color", view.getColor(), options);
			specs.encoding.size = outputConfig.createChannelDef("size", view.getSize(), options);
			specs.encoding.column = outputConfig.createChannelDef("column", view.getColumn(), options);
			specs.encoding.row = outputConfig.createChannelDef("row", view.getRow(), options);
			//
			if (specs.encoding.x!=null && specs.encoding.y!=null) {
				if (specs.encoding.x.type==DataType.nominal && specs.encoding.y.type==DataType.quantitative) {
					// auto sort
					specs.encoding.x.sort = new Sort(specs.encoding.y.field, Operation.max, Order.descending);
				} else if (specs.encoding.y.type==DataType.nominal && specs.encoding.x.type==DataType.quantitative) {
					// auto sort
					specs.encoding.y.sort = new Sort(specs.encoding.x.field, Operation.max, Order.descending);
				}
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
				//
				// set limit of not defined
				if (query.getLimit()==null) {
					query.setLimit((long) 20);
				}
			}
			final int startIndex = query.getStartIndex()!=null?query.getStartIndex():0;
			final int maxResults = query.getMaxResults()!=null?query.getMaxResults():(query.getLimit()!=null?query.getLimit().intValue():100);
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
									&& (op.getOperatorDefinition().getExtendedID().equals(SortOperatorDefinition.ASC_ID)
									|| op.getOperatorDefinition().getExtendedID().equals(SortOperatorDefinition.DESC_ID))) 
							{
								o = op.getArguments().get(0);
							}
						}
						if (o.equals(m)) {
							check = true;
						}
					}
					if (!check) {
						query.getOrderBy().add("desc("+outputConfig.prettyPrint(m)+")");
					}
				}
			}
			//
			// create the facet selection
			FacetSelection selection = createFacetSelection(space, query);
			final ProjectAnalysisJob job = createAnalysisJob(space, query, selection, OutputFormat.JSON);
			//
			// handling data
			ResultInfo info = null;
			if (data.equals("EMBEDED") || data.equals("EMBEDDED")) {
				DataMatrix matrix = compute(userContext, job, query.getMaxResults(), query.getStartIndex(), false);
				if (!outputConfig.isHasMetricSeries()) {
					specs.data = transformToVegaData(query, matrix, DataLayout.RECORDS);
				} else {
					specs.data = transformToVegaData(query, matrix, DataLayout.TRANSPOSE);
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
				{
					HashMap<String, Object> override = new HashMap<>();
					override.put(AnalyticsServiceConstants.COMPARETO_COMPUTE_GROWTH_PARAM, false);
					if (!outputConfig.isHasMetricSeries()) {
						specs.data.url = buildAnalyticsQueryURI(userContext, query, "RECORDS", "DATA", null/*default style*/, override).toString();
					} else {
						specs.data.url = buildAnalyticsQueryURI(userContext, query, "TRANSPOSE", "DATA", null/*default style*/, override).toString();
					}
				}
				specs.data.format = new Format();
				specs.data.format.type = FormatType.json;// lowercase only!
			} else {
				throw new APIException("undefined value for data parameter, must be EMBEDDED or URL");
			}
			// mark
			if (outputConfig.isTimeseries()) {
				specs.mark = Mark.line;
			} else {
				 if (specs.encoding.x!=null && specs.encoding.x.type==DataType.quantitative && specs.encoding.y!=null && specs.encoding.y.type==DataType.quantitative) {
						// use ticks
					 if (specs.encoding.size!=null) {
						 specs.mark = Mark.circle;
					 } else if (specs.encoding.x.bin || specs.encoding.y.bin) {
						 specs.mark = Mark.bar;
					 } else {
						 specs.mark = Mark.point;
					 }
				 } else {
					 specs.mark = Mark.bar;
				 }
			}
			// options
			if (view.hasOptons()) {
			    try {
			    	Properties properties = view.getOptionsAsProperties();
					// mark
					Object omark = properties.get("mark");
					if (omark!=null) {
						try {
							Mark mark = Mark.valueOf(omark.toString());
							specs.mark = mark;
						} catch (IllegalArgumentException e) {
							query.add(new Problem(Severity.WARNING,"options","invalid options parameter 'mark': possible values are: "+toString(Mark.values())));
						}
					}
					// mark-stacked
					Object omarkStacked = properties.get("mark.stacked");
					if (omarkStacked!=null) {
						try {
							Stacked stacked = Stacked.valueOf(omarkStacked.toString());
							specs.config.mark = new VegaliteSpecs.MarkConfig();
							specs.config.mark.stacked = stacked;
							if (stacked==Stacked.none) {
								specs.config.mark.opacity = 0.6;
							}
						} catch (IllegalArgumentException e) {
							query.add(new Problem(Severity.WARNING,"options","invalid options parameter 'mark-stacked': possible values are: "+toString(Stacked.values())));
						}
					}
				} catch (IOException e) {
					query.add(new Problem(Severity.WARNING,"options","invalid options definition: must be: property1:value1;property2:value2;... where properties can be: mark"));
				}
			}
			// size
			if (specs.encoding.row==null && specs.encoding.column==null) {
				specs.config.cell = new VegaliteSpecs.Cell(640,400);
			} else {
				specs.config.cell = new VegaliteSpecs.Cell(320,200);
			}
			//
			ViewReply reply = new ViewReply();
			// update the facet selection with actual values
			FacetSelection actual = computeFacetSelection(space, selection); 
			reply.setSelection(convertToSelection(userContext, query, space, job, actual));
			reply.setQuery(query);
			reply.setResult(specs);
			//
			if (envelope==null) {
				envelope = computeEnvelope(query);
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
				return generator.createHTMLPageView(userContext, space, view, null, reply);
			} else {
				throw new APIException(e.getMessage(), true);
			}
		}
	}
	
	private String toString(Object[] array) {
		String result = null;
		for (Object x : array) {
			if (result==null) {
				result = x.toString();
			} else {
				result += ", "+x.toString();
			}
		}
		return result;
	}
	
	private boolean handleMetrics(AnalyticsQueryImpl query, VegaliteConfigurator inputConfig, ViewQuery view, Properties options, VegaliteSpecs channels, boolean hasMoreDimensions) throws ScopeException {
		// add the metrics after the first dimension has been set
		if (query.getMetrics()==null || query.getMetrics().size()==0) {
			if (!inputConfig.isHasMetric()) {
				// use count() for now
				if (view.getX()==null) {
					view.setX("count()");
					channels.encoding.x = inputConfig.createChannelDef("x", view.getX(), options);
				} else {
					view.setY("count()");
					channels.encoding.y = inputConfig.createChannelDef("y", view.getY(), options);
				}
			}
		} else {
			// display some metrics
			// - single metric
			if (query.getMetrics().size()==1) {
				if (!inputConfig.isHasMetric()) {
					if (query.getCompareTo()!=null && !query.getCompareTo().isEmpty() && !inputConfig.isHasMetricValue() && !inputConfig.isHasMetricValue()) {
						// handle it like a multi-variate
						if (view.getX()==null) {
							view.setX("__VALUE");
							channels.encoding.x = inputConfig.createChannelDef("x", view.getX(), options);
						} else if (view.getY()==null) {
							view.setY("__VALUE");
							channels.encoding.y = inputConfig.createChannelDef("y", view.getY(), options);
						}
						if (view.getY()==null) {
							view.setY("__METRICS");
							channels.encoding.y = inputConfig.createChannelDef("y", view.getY(), options);
						} else if (view.getColor()==null) {
							view.setColor("__METRICS");
							channels.encoding.color = inputConfig.createChannelDef("color", view.getColor(), options);
						} else if (view.getColumn()==null) {
							view.setColumn("__METRICS");
							channels.encoding.column = inputConfig.createChannelDef("column", view.getColumn(), options);
						} else if (view.getRow()==null) {
							view.setRow("__METRICS");
							channels.encoding.row = inputConfig.createChannelDef("row", view.getRow(), options);
						}
					} else {
						if (view.getX()==null) {
							view.setX(query.getMetrics().get(0));
							channels.encoding.x = inputConfig.createChannelDef("x", view.getX(), options);
						} else if (view.getY()==null) {
							view.setY(query.getMetrics().get(0));
							channels.encoding.y = inputConfig.createChannelDef("y", view.getY(), options);
						}
					}
				}
			// - multiple metrics
			} else if (view.getY()==null || view.getColor()==null || view.getColumn()==null || view.getRow()==null) {
				// set __VALUE
				if (!inputConfig.isHasMetricValue()) {
					if (view.getX()==null) {
						view.setX("__VALUE");
						channels.encoding.x = inputConfig.createChannelDef("x", view.getX(), options);
					} else {
						view.setY("__VALUE");
						channels.encoding.y = inputConfig.createChannelDef("y", view.getY(), options);
					}
				}
				// set __METRICS
				if (!inputConfig.isHasMetricSeries()) {
					if (view.getY()==null) {
						view.setY("__METRICS");
						channels.encoding.y = inputConfig.createChannelDef("y", view.getY(), options);
					} else if (view.getColor()==null && !hasMoreDimensions) {
						view.setColor("__METRICS");
						channels.encoding.color = inputConfig.createChannelDef("color", view.getColor(), options);
					} else if (view.getColumn()==null) {
						view.setColumn("__METRICS");
						channels.encoding.column = inputConfig.createChannelDef("column", view.getColumn(), options);
					} else if (view.getRow()==null) {
						view.setRow("__METRICS");
						channels.encoding.row = inputConfig.createChannelDef("row", view.getRow(), options);
					}
				}
			}
		}
		return true;
	}
	
	/**
	 * @param matrix
	 * @return
	 */
	private ResultInfo getAnalyticsResultInfo(Integer pageSize, Integer startIndex, DataMatrix matrix) {
		ResultInfo info = new ResultInfo();
		info.setFromCache(matrix.isFromCache());
		info.setFromSmartCache(matrix.isFromSmartCache());// actually we don't know the origin, see T1851
		info.setExecutionDate(matrix.getExecutionDate().toString());
		info.setStartIndex(startIndex);
		info.setPageSize(pageSize);
		info.setTotalSize(matrix.getRows().size());
		info.setComplete(matrix.isFullset());
		return info;
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
	
	private Data transformToVegaData(AnalyticsQuery query, DataMatrix matrix, DataLayout format) {
		IDataMatrixConverter<Object[]> converter = getConverter(format);
		Data data = new Data();
		data.values = converter.convert(query, matrix);
		return data;
	}
	
	private IDataMatrixConverter<Object[]> getConverter(DataLayout format) {
		if (format==DataLayout.TABLE) {
			return new TableConverter();
		} else if (format==DataLayout.RECORDS) {
			return new RecordConverter();
		} else if (format==DataLayout.TRANSPOSE) {
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
//			PerfDB.INSTANCE.save(queryLog);
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
