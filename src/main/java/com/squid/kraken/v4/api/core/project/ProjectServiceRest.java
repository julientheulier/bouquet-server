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
package com.squid.kraken.v4.api.core.project;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Optional;
import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.database.model.Database;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.BaseServiceRest;
import com.squid.kraken.v4.api.core.annotation.AnnotationServiceRest;
import com.squid.kraken.v4.api.core.bookmark.BookmarkServiceRest;
import com.squid.kraken.v4.api.core.database.DatabaseServiceRest;
import com.squid.kraken.v4.api.core.domain.DomainServiceRest;
import com.squid.kraken.v4.api.core.projectanalysisjob.AnalysisJobServiceRest;
import com.squid.kraken.v4.api.core.projectfacetjob.FacetJobServiceRest;
import com.squid.kraken.v4.api.core.relation.RelationServiceRest;
import com.squid.kraken.v4.api.core.simpleanalysisjob.SimpleAnalysisJobServiceRest;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.database.impl.DatabaseServiceImpl;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.Annotation;
import com.squid.kraken.v4.model.AnnotationList;
import com.squid.kraken.v4.model.AnnotationPK;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainOption;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.ExpressionSuggestion;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.ProjectDAO;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.Authorization;
import com.wordnik.swagger.annotations.AuthorizationScope;

@Api(value = "projects", hidden = true, authorizations = { @Authorization(value = "kraken_auth", type = "oauth2", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
@Produces({ MediaType.APPLICATION_JSON })
public class ProjectServiceRest extends BaseServiceRest {

	private final static String PARAM_NAME = "projectId";

	private ProjectServiceBaseImpl delegate = ProjectServiceBaseImpl
			.getInstance();

	public ProjectServiceRest(AppContext userContext) {
		super(userContext);
	}
	
	@GET
	@Path("")
	@ApiOperation(value = "Gets All Project")
	public List<Project> readProjects() {
		return delegate.readAll(userContext);
	}

	@DELETE
    @Path("{"+PARAM_NAME+"}")
	@ApiOperation(value = "Deletes a Project")
	public boolean delete(@PathParam(PARAM_NAME) String objectId) {
		return delegate.delete(userContext,
				new ProjectPK(userContext.getCustomerId(), objectId));
	}

	/**
	 * Read a Project.
	 * Note : 
	 * <ul>
	 * <li>project DB settings require WRITE rights to be read otherwise there are set to null.<li>
	 * <li>project DB password is write-only.<li>
	 * </ul>
	 */
	@GET
    @Path("{"+PARAM_NAME+"}")
	@ApiOperation(value = "Gets a Project")
	public Project read(@PathParam(PARAM_NAME) String objectId, @QueryParam("deepread") Boolean deepread) {
		ProjectPK projectPK = new ProjectPK(userContext.getCustomerId(),
				objectId);
		return delegate.read(userContext, projectPK);
	}
	
	@GET
    @Path("{"+PARAM_NAME+"}"+"/validate")
	@ApiOperation(value = "Validate the Project connection")
	public boolean validate(@PathParam(PARAM_NAME) String objectId) {
		ProjectPK projectPK = new ProjectPK(userContext.getCustomerId(),
				objectId);
		Optional<Project> project = ((ProjectDAO) DAOFactory.getDAOFactory().getDAO(Project.class)).read(
				userContext, projectPK);
		if (project.isPresent()) {
			try {
				DatabaseServiceImpl.INSTANCE.getDatabase(project.get());
				return true;
			} catch (DatabaseServiceException e) {
				throw new APIException(e.getMessage(), e, false);
			}
		} else {
			throw new APIException("cannot find project with PK = "+projectPK.toString(),false);
		}
	}

	@GET
	@Path("{"+PARAM_NAME+"}"+"/features")
	@ApiOperation(value = "Give the functions supported by the project")
	public List<String> features(@PathParam(PARAM_NAME) String objectId) {
		ProjectPK projectPK = new ProjectPK(userContext.getCustomerId(),
				objectId);
		Optional<Project> project = ((ProjectDAO) DAOFactory.getDAOFactory().getDAO(Project.class)).read(
				userContext, projectPK);
		if (project.isPresent()) {
			try {
				Database db = DatabaseServiceImpl.INSTANCE.getDatabase(project.get());
				return db.getSkin().canRender();
			} catch (DatabaseServiceException e) {
				throw new APIException(e.getMessage(), e, false);
			}
		} else {
			throw new APIException("cannot find project with PK = "+projectPK.toString(),false);
		}
	}

	@POST
	@Path("")
	@ApiOperation(value = "Creates a Project")
	public Project store(@ApiParam(required = true) Project project) {
		ProjectPK id = project.getId();
		if (id == null) {
			id = new ProjectPK(userContext.getCustomerId(), null);
			project.setId(id);
		}
		return delegate.store(userContext, project);
	}
	
	@POST
    @Path("{"+PARAM_NAME+"}")
	@ApiOperation(value = "Creates a Project")
	public Project store(@PathParam(PARAM_NAME) String objectId, @ApiParam(required = true) Project project) {
		ProjectPK id = new ProjectPK(userContext.getCustomerId(), objectId);
		project.setId(id);
		return delegate.store(userContext, project);
	}
	
	@PUT
	@Path("")
	@ApiOperation(value = "Updates a Project")
    public Project update(@ApiParam(required = true) Project project) {
        return delegate.store(userContext, project);
    }
	
	@PUT
    @Path("{"+PARAM_NAME+"}")
	@ApiOperation(value = "Updates a Project")
    public Project update(@PathParam(PARAM_NAME) String objectId, @ApiParam(required = true) Project project) {
		project.setId(new ProjectPK(userContext.getCustomerId(), objectId));
		return delegate.store(userContext, project);
    }

	/**
	 * Refresh a {@link Database} object.
	 * WRITE Role on the Project is required.
	 * @param projectId
	 */
	@Path("{"+PARAM_NAME+"}"+"/refreshDatabase")
	@GET
	@ApiOperation(value = "Refresh database operation")
	public boolean refreshDatabase(@PathParam(PARAM_NAME) String projectId) {
		return delegate.refreshDatabase(userContext, projectId);
	}

	@Path("{"+PARAM_NAME+"}"+"/cache")
	@GET
	public Object readCacheInfo(
			@PathParam(PARAM_NAME) String objectId) {
		return delegate.readCacheInfo(userContext,
				new ProjectPK(userContext.getCustomerId(), objectId));
	}

	@Path("{"+PARAM_NAME+"}"+"/cache")
	@DELETE
	public Object deleteCache(
			@PathParam(PARAM_NAME) String objectId) {
		return delegate.refreshCache(userContext,
				new ProjectPK(userContext.getCustomerId(), objectId));
	}

	@Path("{"+PARAM_NAME+"}"+"/cache/refresh")
	@GET
	public Object refreshCache(
			@PathParam(PARAM_NAME) String objectId) {
		return delegate.refreshCache(userContext,
				new ProjectPK(userContext.getCustomerId(), objectId));
	}

	@Path("{"+PARAM_NAME+"}"+"/access")
	@GET
	@ApiOperation(value = "Gets a Project's access rights")
	public Set<AccessRight> readAccessRights(
			@PathParam(PARAM_NAME) String objectId) {
		return delegate.readAccessRights(userContext,
				new ProjectPK(userContext.getCustomerId(), objectId));
	}

	@Path("{"+PARAM_NAME+"}"+"/access")
	@POST
	@ApiOperation(value = "Sets a Project's access rights")
	public Set<AccessRight> storeAccessRights(
			@PathParam(PARAM_NAME) String objectId,
			@ApiParam(required = true) Set<AccessRight> accessRights) {
		return delegate.storeAccessRights(userContext, new ProjectPK(
				userContext.getCustomerId(), objectId), accessRights);
	}

	@Path("{"+PARAM_NAME+"}"+"/domains")
	@ApiOperation(value = "Get the domains")
	public DomainServiceRest getDomainService() {
		return new DomainServiceRest(userContext);
	}


	@GET
	@Path("{" + PARAM_NAME + "}" + "/move")
	@ApiOperation(value = "Gets an domain and save it to an other domain/db")
	public Response linkDomain(
			@PathParam(PARAM_NAME) String sourceProjectId,
			@ApiParam(value = "domain name for the source") @QueryParam("sourceDomainId") String sourceDomainId,
			@ApiParam(value = "response timeout in milliseconds in case the job is not yet computed. If no timeout set, the method will return according to current job status.") @QueryParam("timeout") Integer timeout,
			@ApiParam(value = "domain name for the destination") @QueryParam("destDomainName") String destDomainName,
			@ApiParam(value = "database for the destination") @QueryParam("destProjectId") String destProjectId) {
		return transferDomain(sourceProjectId, sourceDomainId, timeout,
				destDomainName, destProjectId);
	}


	private Response transferDomain(
			String sourceProjectId,
			String sourceDomainId,
			Integer timeout,
			String destDomainName,
			String destProjectId) {

		if(sourceDomainId == null){
			throw new APIException("cannot move a domain without the source domain ID ",false);
		}

		String newOid = null;

		if( destProjectId == sourceProjectId  && sourceProjectId != null ){ // Same => Simpler SQL.
			// SELECT (ANALYSISJOB_QUERY) INTO NEW_TABLE FROM OLD_TABLE

		} else if( destProjectId == null ) { // Go to Spark

		} else{ // Some other DB.

			// Create a new domain
			final DomainPK id = new DomainPK(
					userContext.getCustomerId(), destProjectId, sourceDomainId);
			// Prefix + Suffix to indicate it is a link and the source.
			final Domain newDomain = new Domain(id, DomainOption.LINK_PREFIX+destDomainName+"_"+sourceDomainId, null);

			DomainOption newDomainOption = new DomainOption();
			newDomainOption.setAlink(true);
			newDomainOption.setLinkSource(sourceDomainId);
			newDomain.setOptions(newDomainOption);

			// Get the project
			ProjectPK projectPk = new ProjectPK(userContext.getCustomerId(), destProjectId);
			Project project = null;
			try {
				project = ProjectManager.INSTANCE.getProject(userContext, projectPk);
				Persistent<? extends GenericPK> parent = project
						.getParentObject(userContext);
				// need write role on parent
				AccessRightsUtils.getInstance().checkRole(userContext, parent, AccessRight.Role.WRITE);

				// add the link iff there is no domain with the same id.
				ArrayList<Domain> newDomainList = new ArrayList<Domain>(project.getDomains());
				if(!newDomainList.contains(newDomain)) {
					newDomainList.add(newDomain);
					project.setDomains(newDomainList);
					delegate.store(userContext, project);
				}
			} catch (ScopeException e) {
				e.printStackTrace();
			}


/*
			Optional<Project> project = ((ProjectDAO) DAOFactory.getDAOFactory().getDAO(Project.class)).read(
					userContext, projectPk);


			if(project.isPresent()) {
				Persistent<? extends GenericPK> parent = project.get()
						.getParentObject(userContext);
				// need write role on parent
				AccessRightsUtils.getInstance().checkRole(userContext, parent, AccessRight.Role.WRITE);

			}*/

			// Insert the analysisJob as an option in the new domain.
			/*final ProjectAnalysisJobPK id = new ProjectAnalysisJobPK(
					userContext.getCustomerId(), destProjectId, jobId);*/


		}
		Response.ResponseBuilder response = Response.ok(newOid);
		// return the new domain to be able to do processing on it.
		return response.build();

	}

	@Path("{"+PARAM_NAME+"}"+"/domains-suggestion")
	@GET
	@ApiOperation(value = "Gets suggestions for Domain")
	public ExpressionSuggestion getDomainSuggestion(
			@PathParam("projectId") String projectId,
			@QueryParam("expression") String expression,
			@QueryParam("offset") int offset) {
		return delegate.getDomainSuggestion(userContext, projectId, expression,
				offset);
	}

	@Path("{"+PARAM_NAME+"}"+"/schemas-suggestion")
	@GET
	@ApiOperation(value = "Gets suggestions for DB Schemas")
	public ExpressionSuggestion getSchemaSuggestion(
			@PathParam("projectId") String projectId) {
		return delegate.getSchemaSuggestion(userContext, projectId);
	}
	
	// analysisjobs
	@Path("{"+PARAM_NAME+"}"+"/analysisjobs")
	@ApiOperation(value = "Gets AnalysisJobs")
	public AnalysisJobServiceRest getAnalysisJobService() {
		return new AnalysisJobServiceRest(userContext);
	}

	// facetjobs
	@Path("{"+PARAM_NAME+"}"+"/facetjobs")
	@ApiOperation(value = "Gets FacetJobs")
	public FacetJobServiceRest getFacetJobService() {
		return new FacetJobServiceRest(userContext);
	}

	// relations
	@Path("{"+PARAM_NAME+"}"+"/relations")
	@ApiOperation(value = "Gets Relations")
	public RelationServiceRest getRelationService() {
		return new RelationServiceRest(userContext);
	}

	@Path("{"+PARAM_NAME+"}"+"/relations-suggestion")
	@GET
	@ApiOperation(value = "Gets suggestions for Relations")
	public ExpressionSuggestion getRelationSuggestion(
			@PathParam("projectId") String projectId,
			@QueryParam("leftDomainId") String leftDomainId,
			@QueryParam("rightDomainId") String rightDomainId,
			@QueryParam("expression") String expression,
			@QueryParam("offset") int offset) {
		return delegate.getRelationSuggestion(userContext, projectId,
				leftDomainId, rightDomainId, expression, offset);
	}

	// annotations

	/**
	 * Go to the Annotation REST service to create/update/delete annotation
	 * 
	 * @return AnnotationServiceRest
	 */
	@Path("{"+PARAM_NAME+"}"+"/annotations/{annotationId}")
	public AnnotationServiceRest getAnnotationService() {
		return new AnnotationServiceRest(userContext);
	}

	/**
	 * Get the list of annotations defined in a project by project id.
	 * 
	 * @param projectId
	 *            project id of the project that user wants to get the
	 *            annotations
	 * @param unread
	 *            if unread = 1, the result contains only unread annotations,
	 *            otherwise, the result contains all the annotations of the
	 *            project
	 * @return annotation list
	 */
	@Path("{"+PARAM_NAME+"}"+"/annotations")
	@GET
	public AnnotationList readAnnotations(
			@PathParam(PARAM_NAME) String projectId,
			@QueryParam("unread") Integer unread) {
		return delegate.readAnnotations(userContext,
				new ProjectPK(userContext.getCustomerId(), projectId), unread);
	}

	/**
	 * Create a new annotation.
	 * 
	 * @param projectId
	 *            project id
	 * @param annotation
	 *            annotation object to be created
	 * @return created object
	 */
	@Path("{"+PARAM_NAME+"}"+"/annotations")
	@POST
	public AnnotationPK addAnnotation(@PathParam(PARAM_NAME) String projectId,
			Annotation annotation) {
		return delegate.addAnnotation(userContext, projectId, annotation);
	}

	// database service
	@Path("{"+PARAM_NAME+"}"+"/database")
	public DatabaseServiceRest getDatabaseService() {
		return new DatabaseServiceRest(userContext);
	}

	@Path("{"+PARAM_NAME+"}"+"/bookmarks")
	@ApiOperation(value = "Get the Bookmarks")
	public BookmarkServiceRest getBookmarkService(
			@Context HttpServletRequest request) {
		return new BookmarkServiceRest(userContext);
	}
	
	// simple analysisjobs
	@Path("{"+PARAM_NAME+"}"+"/analyses")
	@ApiOperation(value = "Simple Analysis Service")
	public SimpleAnalysisJobServiceRest getSimpleAnalysisJobService() {
		return new SimpleAnalysisJobServiceRest(userContext);
	}
	
}
