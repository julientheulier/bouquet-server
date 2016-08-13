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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeoutException;

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
import com.squid.kraken.v4.api.core.projectanalysisjob.AnalysisJobComputer;
import com.squid.kraken.v4.core.analysis.engine.bookmark.BookmarkManager;
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
import com.squid.kraken.v4.model.DataTable;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Expression;
import com.squid.kraken.v4.model.ExpressionSuggestion;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.FacetExpression;
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
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.BookmarkDAO;
import com.squid.kraken.v4.persistence.dao.ProjectDAO;

/**
 * @author sergefantino
 *
 */
public class BookmarkAnalysisServiceBaseImpl {
	
	public static final BookmarkAnalysisServiceBaseImpl INSTANCE = new BookmarkAnalysisServiceBaseImpl();
	
	private BookmarkAnalysisServiceBaseImpl() {
	}
	

	private static final NavigationItem PROJECTS_FOLDER = new NavigationItem("Projects", "", "/PROJECTS", "FOLDER");
	private static final NavigationItem SHARED_FOLDER = new NavigationItem("Shared Bookmarks", "", "/SHARED", "FOLDER");
	private static final NavigationItem MYBOOKMARKS_FOLDER = new NavigationItem("My Bookmarks", "", "/MYBOOKMARKS", "FOLDER");

	public List<NavigationItem> listContent(
			AppContext userContext,
			String parent,
			boolean isFlat
		) throws ScopeException {
		List<NavigationItem> content = new ArrayList<>();
		if (parent !=null && parent.endsWith("/")) {
			parent = parent.substring(0, parent.length()-1);// remove trailing /
		}
		if (parent==null || parent.length()==0) {
			// this is the root
			content.add(PROJECTS_FOLDER);
			if (isFlat) listProjects(userContext, parent, content);
			content.add(SHARED_FOLDER);
			if (isFlat) listSharedBoomarks(userContext, parent, isFlat, content);
			content.add(MYBOOKMARKS_FOLDER);
			if (isFlat) listMyBoomarks(userContext, parent, isFlat, content);
		} else {
			// need to list parent's content
			if (parent.startsWith(PROJECTS_FOLDER.getSelfRef())) {
				listProjects(userContext, parent, content);
			} else if (parent.startsWith(SHARED_FOLDER.getSelfRef())) {
				listSharedBoomarks(userContext, parent, isFlat, content);
			} else if (parent.startsWith(MYBOOKMARKS_FOLDER.getSelfRef())) {
				listMyBoomarks(userContext, parent, isFlat, content);
			} else {
				// invalid
				throw new ObjectNotFoundAPIException("invalid parent reference", true);
			}
		}
		return content;
	}
	
	/**
	 * list the projects and their content (domains) depending on the parent path.
	 * Note that this method does not support the flat mode because of computing constraints.
	 * @param userContext
	 * @param parent
	 * @param content
	 * @throws ScopeException
	 */
	private void listProjects(AppContext userContext, String parent, List<NavigationItem> content) throws ScopeException {
		// list project related resources
		if (parent.equals(PROJECTS_FOLDER.getSelfRef())) {
			// return available project
			List<Project> projects = ((ProjectDAO) DAOFactory.getDAOFactory().getDAO(Project.class))
					.findByCustomer(userContext, userContext.getCustomerPk());
			for (Project project : projects) {
				String id = PROJECTS_FOLDER.getSelfRef()+"/"+project.getId().getProjectId();
				NavigationItem folder = new NavigationItem(project.getName(), parent, id, "FOLDER");
				content.add(folder);
			}
		} else {
			String projectId = parent.substring(PROJECTS_FOLDER.getSelfRef().length()+1);// remove /PROJECTS/ part
			ProjectPK projectPk = new ProjectPK(userContext.getClientId(), projectId);
			List<Domain> domains = ProjectManager.INSTANCE.getDomains(userContext, projectPk);
			for (Domain domain : domains) {
				String id = "@'"+projectId+"'.@'"+domain.getId().getDomainId()+"'";
				NavigationItem item = new NavigationItem(domain.getName(), parent, id, "DOMAIN");
				content.add(item);
			}
		}
	}
	
	/**
	 * list the MyBookmarks content (bookmarks and folders)
	 * @param userContext
	 * @param parent
	 * @param isFlat
	 * @param content
	 * @throws ScopeException
	 */
	private void listMyBoomarks(AppContext userContext, String parent, boolean isFlat, List<NavigationItem> content) throws ScopeException {
		// list mybookmark related resources
		String fullPath = getMyBookmarkPath(userContext);
		if (parent.equals(MYBOOKMARKS_FOLDER.getSelfRef())) {
			// just keep the fullpath
		} else {
			// add the remaining path to fullpath
			fullPath += parent.substring(MYBOOKMARKS_FOLDER.getSelfRef().length());
		}
		listBoomarks(userContext, parent, isFlat, fullPath, content);
	}
	
	/**
	 * List the Shared bookmarks and folders
	 * @param userContext
	 * @param parent
	 * @param isFlat
	 * @param content
	 * @throws ScopeException
	 */
	private void listSharedBoomarks(AppContext userContext, String parent, boolean isFlat, List<NavigationItem> content) throws ScopeException {
		// list mybookmark related resources
		String fullPath = Bookmark.SEPARATOR + Bookmark.Folder.SHARED;
		if (!parent.equals(SHARED_FOLDER.getSelfRef())) {
			// add the remaining path to fullpath
			fullPath += parent.substring(SHARED_FOLDER.getSelfRef().length());
		}
		listBoomarks(userContext, parent, isFlat, fullPath, content);
	}

	private void listBoomarks(AppContext userContext, String parent, boolean isFlat, String fullPath, List<NavigationItem> content) throws ScopeException {
		// list the content first
		List<Bookmark> bookmarks = ((BookmarkDAO) DAOFactory.getDAOFactory()
				.getDAO(Bookmark.class)).findByPath(userContext, fullPath);
		HashSet<String> folders = new HashSet<>();
		for (Bookmark bookmark : bookmarks) {
			String path = bookmark.getPath();
			// only handle the exact path
			if (path.equals(fullPath)) {
				String id = "@'"+bookmark.getId().getProjectId()+"'.[bookmark:'"+bookmark.getId().getBookmarkId()+"']";
				NavigationItem item = new NavigationItem(bookmark.getName(), parent, id, "BOOKMARK");
				content.add(item);
			} else {
				// it's a sub folder
				path = bookmark.getPath().substring(fullPath.length()+1);// remove first /
				String[] split = path.split("/");
				if (split.length>0) {
					String name = "/"+(isFlat?path:split[0]);
					String id = parent+name;
					if (!folders.contains(id)) {
						NavigationItem item = new NavigationItem(name, parent, id, "FOLDER");
						content.add(item);
						folders.add(id);
					}
				}
			}
		}
	}
	
	/**
	 * compute the MyBookmark path for the current user
	 * @param ctx
	 */
	private String getMyBookmarkPath(AppContext ctx) {
		return Bookmark.SEPARATOR + Bookmark.Folder.USER
					+ Bookmark.SEPARATOR + ctx.getUser().getOid();
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
	
	private Space getSpace(AppContext userContext, String BBID) throws ScopeException {
		GlobalExpressionScope scope = new GlobalExpressionScope(userContext);
		ExpressionAST expr = scope.parseExpression(BBID);
		if (expr instanceof SpaceExpression) {
			SpaceExpression ref = (SpaceExpression)expr;
			Space space = ref.getSpace();
			return space;
		}
		// else
		throw new ScopeException("invalid BBID");
	}
	
	public Facet getFacet(
			AppContext userContext, 
			String BBID,
			String facetId,
			String filter,
			Integer maxResults,
			Integer startIndex,
			Integer timeoutMs
			) throws ComputingException {
		try {
			Space space = getSpace(userContext, BBID);
			Domain domain = space.getDomain();
			//
			ProjectFacetJob job = new ProjectFacetJob();
			job.setDomain(Collections.singletonList(domain.getId()));
			job.setCustomerId(userContext.getCustomerId());
			//
			if (space.hasBookmark()) {
				BookmarkConfig config = BookmarkManager.INSTANCE.readConfig(space.getBookmark());
				FacetSelection selection = config.getSelection();
				job.setSelection(selection);
			} else {
				job.setSelection(new FacetSelection());
			}
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
						facetId, filter, maxResults, startIndex, sel);
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
	
	public ExpressionSuggestion evaluateExpression(
			AppContext userContext, 
			String BBID,
			String expression,
			Integer offset,
			ObjectType[] kindFilters
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
		Collection<ObjectType> kinds = null;
		if (kindFilters!=null) {
			if (kindFilters.length>1) {
				kinds = new HashSet<>(Arrays.asList(kindFilters));
			} else if (kindFilters.length==1) {
				kinds = Collections.singletonList(kindFilters[0]);
			}
		}
		ExpressionSuggestion suggestions = handler.getSuggestion(expression, offset, kinds, null);
		return suggestions;
	}
	
	public AnalysisResult runAnalysis(
			AppContext userContext,
			String BBID,
			AnalysisQuery query,
			Integer maxResults,
			Integer startIndex,
			String lazy
			) throws ComputingException, ScopeException, InterruptedException {
		Space space = getSpace(userContext, BBID);
		//
		Bookmark bookmark = space.getBookmark();
		BookmarkConfig config = readConfig(bookmark);
		//
		// using the default selection
		FacetSelection selection = config!=null?config.getSelection():new FacetSelection();
		//
		initDefaultAnalysis(space, query, config);
		ProjectAnalysisJob job = createAnalysisJob(space.getUniverse(), query, selection, OutputFormat.JSON);
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
	
	private void initDefaultAnalysis(Space space, AnalysisQuery analysis, BookmarkConfig config) throws ScopeException {
		UniverseScope globalScope = new UniverseScope(space.getUniverse());
		if (analysis.getDomain() == null) {
			analysis.setDomain("@'" + config.getDomain() + "'");
		}
		if (analysis.getLimit() == null) {
			if (config!=null) {
				analysis.setLimit(config.getLimit());
			} else {
				analysis.setLimit((long) 1000);
			}
		}
		if (analysis.getGroupBy() == null) {
			if (config==null) {
				List<AnalysisFacet> groupBy = new ArrayList<AnalysisFacet>();
				// use a default pivot selection...
				// -- just list the content of the table
				for (Dimension dimension : space.getDimensions()) {
					AnalysisFacet f = new AnalysisQueryImpl.AnalysisFacetImpl();
					f.setExpression("@'"+dimension.getOid()+"'");
					groupBy.add(f);
				}
				analysis.setGroupBy(groupBy);
			} else if (config.getChosenDimensions() != null) {
				List<AnalysisFacet> groupBy = new ArrayList<AnalysisFacet>();
				for (String chosenDimension : config.getChosenDimensions()) {
					AnalysisFacet f = new AnalysisQueryImpl.AnalysisFacetImpl();
					if (chosenDimension.startsWith("@")) {
						// need to fix the scope
						ExpressionAST expr = globalScope.parseExpression(chosenDimension);
						f.setExpression(prettyPrint(expr, space));
					} else {
						f.setExpression("@'" + chosenDimension + "'");
					}
					groupBy.add(f);
				}
				analysis.setGroupBy(groupBy);
			}
		}
		if (analysis.getMetrics() == null) {
			if (config==null) {
				// no metrics
			} else if (config.getChosenMetrics() != null) {
				List<AnalysisFacet> metrics = new ArrayList<AnalysisFacet>();
				for (String chosenMetric : config.getChosenMetrics()) {
					AnalysisFacet f = new AnalysisQueryImpl.AnalysisFacetImpl();
					f.setExpression("@'" + chosenMetric + "'");
					metrics.add(f);
				}
				analysis.setMetrics(metrics);
			}
		}
		if (analysis.getOrderBy() == null) {
			if (config!=null) {
				for (OrderBy orderBy : config.getOrderBy()) {
					ExpressionAST expr = globalScope.parseExpression(orderBy.getExpression().getValue());
					orderBy.getExpression().setValue(prettyPrint(expr, space));
				}
				analysis.setOrderBy(config.getOrderBy());
			}
		}
		if (analysis.getRollups() == null) {
			if (config!=null) {
				analysis.setRollups(config.getRollups());
			}
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
