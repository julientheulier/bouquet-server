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
import java.util.concurrent.ExecutionException;

import org.bson.types.ObjectId;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.squid.core.database.lazy.LazyDatabaseFactory;
import com.squid.core.database.model.Database;
import com.squid.core.database.model.Schema;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.CoreConstants;
import com.squid.kraken.v4.api.core.GenericServiceImpl;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.api.core.annotation.AnnotationServiceBaseImpl;
import com.squid.kraken.v4.api.core.projectuser.ProjectUserServiceBaseImpl;
import com.squid.kraken.v4.caching.awsredis.SimpleDatabaseManager;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.database.impl.DatabaseServiceImpl;
import com.squid.kraken.v4.core.expression.scope.ExpressionSuggestionHandler;
import com.squid.kraken.v4.core.expression.scope.ProjectExpressionScope;
import com.squid.kraken.v4.core.expression.scope.RelationExpressionScope;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Annotation;
import com.squid.kraken.v4.model.AnnotationList;
import com.squid.kraken.v4.model.AnnotationPK;
import com.squid.kraken.v4.model.Customer;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.ExpressionSuggestion;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.ProjectUser;
import com.squid.kraken.v4.model.ProjectUserPK;
import com.squid.kraken.v4.model.UserGroup;
import com.squid.kraken.v4.model.UserGroupPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.MongoDBHelper;
import com.squid.kraken.v4.persistence.dao.AnnotationDAO;
import com.squid.kraken.v4.persistence.dao.ProjectDAO;

public class ProjectServiceBaseImpl extends GenericServiceImpl<Project, ProjectPK> {
	
	private static final Logger logger = LoggerFactory.getLogger(ProjectServiceBaseImpl.class);

    private static ProjectServiceBaseImpl instance;

    public static ProjectServiceBaseImpl getInstance() {
        if (instance == null) {
            instance = new ProjectServiceBaseImpl();
        }
        return instance;
    }

    private ProjectServiceBaseImpl() {
        // made private for singleton access
        super(Project.class);
    }
    
    /**
     * Create a project. The creation includes the following steps:
     * <ul>
     * <li>Create the project</li>
     * <li>Create the admin group called admin_<projectId></li>
     * <li>Grant WRITE access right to admin_<projectId> group</li>
     * <li>Create the guest group called guest_<projectId></li>
     * <li>Grant READ access right to guest_<projectId> group</li>
     * </ul>
     * 
     * @param ctx
     *            application context
     * @param project
     *            project to add
     * @return the new project
     */
    @Override
	public Project store(AppContext ctx, Project newProject) {
    	boolean updateMode;
    	ProjectPK id = newProject.getId();
        if (id.getObjectId() == null) {
            // generate a new oid
            id.setObjectId(ObjectId.get().toString());
            newProject.setId(id);
            updateMode = false;
        } else {
        	// check if this is a creation or an update
        	Optional<Project> read = DAOFactory.getDAOFactory().getDAO(Project.class).read(ctx, id);
        	if (read.isPresent()) {
        		updateMode = true;
        	} else {
        		updateMode = false;
        	}
        }
        
        if (!updateMode) {
        	// apply creation specific rules
	        String projectOid = newProject.getId().getProjectId();
	        // check the access rights
	 		Persistent<? extends GenericPK> parent = newProject
	 				.getParentObject(ctx);
	 		// need write role on parent
	 		AccessRightsUtils.getInstance().checkRole(ctx, parent, Role.WRITE);
	 		// set the access rights
	 		AccessRightsUtils.getInstance().setAccessRights(ctx, newProject, parent);
	        Set<AccessRight> projectAccessRights = newProject.getAccessRights();
	        
	        // create the admin group
	        UserGroup adminGroup = new UserGroup();
	        adminGroup.setId(new UserGroupPK(ctx.getCustomerId(), CoreConstants.PRJ_DEFAULT_GROUP_ADMIN + projectOid));
	        adminGroup.setName("Administrators for project "+newProject.getName());
	        UserGroup newAdminGroup = DAOFactory.getDAOFactory().getDAO(UserGroup.class).create(ctx, adminGroup);
	        
	        // "admin_projectId" group is assigned WRITE right to the new project
	        AccessRight writeRight = new AccessRight();
	        writeRight.setRole(Role.WRITE);
	        writeRight.setGroupId(newAdminGroup.getId().getUserGroupId());
	        projectAccessRights.add(writeRight);
	        
	        // create the guest group
	        UserGroup guestGroup = new UserGroup();
	        guestGroup.setId(new UserGroupPK(ctx.getCustomerId(), CoreConstants.PRJ_DEFAULT_GROUP_GUEST + projectOid));
	        guestGroup.setName("Guests for project "+newProject.getName());
	        UserGroup newGuestGroup = DAOFactory.getDAOFactory().getDAO(UserGroup.class).create(ctx, guestGroup);
	        
	        // "guest_projectId" group is assigned READ right to the new project
	        AccessRight readRight = new AccessRight();
	        readRight.setRole(Role.READ);
	        readRight.setGroupId(newGuestGroup.getId().getUserGroupId());
	        projectAccessRights.add(readRight);
        }
        
        newProject = super.store(ctx, newProject);
        return newProject;
	}

	public List<Project> readAll(AppContext ctx) {
		return ((ProjectDAO) DAOFactory.getDAOFactory().getDAO(Project.class))
				.findByCustomer(ctx, ctx.getCustomerPk());
	}
    
    /**
     * Get Domain suggestions for current Project.
     * 
     * @param ctx
     * @param projectId
     * @param expression
     * @param offset
     *            if null, will use expression's length
     * @return ExpressionSuggestion
     */
    public ExpressionSuggestion getDomainSuggestion(AppContext ctx, String projectId, String expression, Integer offset) {
        //
    	try {
	        ProjectPK projectPK = new ProjectPK(ctx.getCustomerId(), projectId);
	        Project project = ProjectManager.INSTANCE.getProject(ctx, projectPK);
	        //
	        Universe universe = new Universe(ctx, project);
	        ProjectExpressionScope scope = new ProjectExpressionScope(universe);
	        ExpressionSuggestionHandler handler = new ExpressionSuggestionHandler(scope);
	        if (offset == null) {
	            offset = expression.length();
	        }
	        return handler.getSuggestion(expression, offset);
    	} catch (ScopeException e) {
    		throw new ObjectNotFoundAPIException(e.getMessage(), e, false);
    	}
    }
    
    /**
     * Get Schemas available for current Project.
     * 
     * @param ctx
     * @param projectId
     * @return the list of schema names as an ExpressionSuggestion
     */
    @Deprecated
    public ExpressionSuggestion getSchemaSuggestion(AppContext userContext, String projectId) {
		//
		// check user role
		Customer customer = DAOFactory.getDAOFactory().getDAO(Customer.class).readNotNull(userContext, userContext.getCustomerPk());
		AccessRightsUtils.getInstance().checkRole(userContext, customer, Role.WRITE);
 		//
		SimpleDatabaseManager manager = null;
		try {
			ProjectPK projectPk = new ProjectPK(userContext.getClientId(), projectId);
			// check user access - will fail if user cannot access
			ProjectManager.INSTANCE.getProject(userContext, projectPk);
			// now escalate to get the password
			AppContext root = ServiceUtils.getInstance().getRootUserContext(userContext);
			Project rootProject = ProjectManager.INSTANCE.getProject(root, projectPk);
			manager = new SimpleDatabaseManager(rootProject.getDbUrl(), rootProject.getDbUser(), rootProject.getDbPassword());
			LazyDatabaseFactory factory = new LazyDatabaseFactory(manager);
			Database database = factory.createDatabase();
	        ExpressionSuggestion result = new ExpressionSuggestion();
	        List<String> schemaNames = new ArrayList<String>();
	        for (Schema schema : database.getSchemas()) {
	        	if (!schema.isSystem()) {
	        		schemaNames.add(schema.getName());
	        	}
	        }
	        result.setDefinitions(schemaNames);
	        return result;
		} catch (ExecutionException | ScopeException e) {
			throw new APIException(e.getMessage(), e, false);
		} finally {
			if (manager!=null) {
				manager.close();
			}
		}
    }

	public ExpressionSuggestion getRelationSuggestion(AppContext ctx,
			String projectId, String leftId, String rightId, String expression, int offset) {
	    
	    if ((leftId == null) || leftId.isEmpty() || (rightId == null) || rightId.isEmpty()) {
	        ExpressionSuggestion error = new ExpressionSuggestion();
	        List<String> s = new ArrayList<String>();
	        error.setDefinitions(s);
            error.setValidateMessage("Left ID and Right ID must be selected");
            return error;
	    }
        //
        try {
            ProjectPK projectPK = new ProjectPK(ctx.getCustomerId(), projectId);
	        Project project = ProjectManager.INSTANCE.getProject(ctx, projectPK);
            DomainPK leftPK = new DomainPK(projectPK, leftId);
            Domain left = ProjectManager.INSTANCE.getDomain(ctx, leftPK);
            DomainPK rightPK = new DomainPK(projectPK, rightId);
            Domain right = ProjectManager.INSTANCE.getDomain(ctx, rightPK);
            Universe universe = new Universe(ctx, project);
	        RelationExpressionScope scope = new RelationExpressionScope(universe, left, right);
	        ExpressionSuggestionHandler handler = new ExpressionSuggestionHandler(scope);
	        if (offset == 0) {
	            offset = expression.length();
	        }
	        return handler.getSuggestion(expression, offset);
        } catch (ScopeException e) {
        	ExpressionSuggestion error = new ExpressionSuggestion();
        	error.setValidateMessage(e.getLocalizedMessage());
        	return error;
        }
	}

    /**
     * Create an annotation. This method also inserts a record to the ProjectUser table with last annotation read date
     * is NULL.
     * 
     * @param ctx
     *            application context
     * @param projectId
     *            poject id
     * @param annotation
     *            object to add. Note: length of the message must < 2000.
     * @return PK of the new annotations
     */
    public AnnotationPK addAnnotation(AppContext ctx, String projectId, Annotation annotation) {
        // verify the length of the annotation's message
        String message = annotation.getMessage();
        if ((message != null) && (message.length() >= 2000)) {
            throw new IllegalArgumentException("Message of the annotation must not exceed 2000 characters.");
        }

        // save the new annotation in the data base
        AnnotationPK annotationPk = new AnnotationPK(ctx.getCustomerId(), projectId);
        Annotation newAnnotation = new Annotation(annotationPk);
        newAnnotation.setAuthorId(ctx.getUser().getId());

        // creation date must be entered by the application by using the current time
        Long creationTimestamp = System.currentTimeMillis();
        newAnnotation.setCreationTimestamp(creationTimestamp);
        
        // if user does not enter annotationDate, creationDate will be considered as annotationDate
        Long annotationTimestamp = annotation.getAnnotationTimestamp();
        if (annotationTimestamp == null) {
            newAnnotation.setAnnotationTimestamp(creationTimestamp);
        } else {
            newAnnotation.setAnnotationTimestamp(annotationTimestamp);
        }
        
        // set other properties
        newAnnotation.setMessage(annotation.getMessage());
        newAnnotation = AnnotationServiceBaseImpl.getInstance().store(ctx, newAnnotation);

        // return the ID of the new annotation
        return newAnnotation.getId();
    }

    /**
     * Get all the annotation of a project or only unread annotation depending on the input. This method returns also
     * the last annotation read date.
     * 
     * @param ctx
     *            application context
     * @param projectId
     *            project id
     * @param unread
     *            if unread = 1, the result contains only unread annotations, otherwise, the result contains all the
     *            annotations of the project
     * @return AnnotationList object which contains the last annotation read date and the list of annotations
     */
    public AnnotationList readAnnotations(AppContext ctx, ProjectPK projectPk, Integer unread) {
        // prepare variables
        AnnotationList annotationList = new AnnotationList();
        String orderBy = "-annotationTimestamp";
        List<Annotation> annotations = ((AnnotationDAO) factory.getDAO(Annotation.class)).findByProject(ctx,
                projectPk, orderBy);
            
        // get the "last annotation read date"
        ProjectUserServiceBaseImpl pus = ProjectUserServiceBaseImpl.getInstance();
        ProjectUser projectUser;
        Long lastAnnotationReadTimestamp = null;
        try {
            projectUser = pus.read(ctx, new ProjectUserPK(ctx.getCustomerId(), projectPk.getProjectId(), ctx
                    .getUser().getId().getUserId()));
            lastAnnotationReadTimestamp = projectUser.getLastAnnotationReadTimestamp();
            annotationList.setLastAnnotationReadTimestamp(lastAnnotationReadTimestamp);
        } catch (ObjectNotFoundAPIException e) {
            annotationList.setLastAnnotationReadTimestamp(null);
            projectUser = new ProjectUser(new ProjectUserPK(ctx.getCustomerId(), projectPk.getProjectId(), ctx
                    .getUser().getId().getUserId()));
        }

        // get annotations / unread annotations
        boolean returnAll = (unread == null) || (unread != 1);
        if (!returnAll) { 
            // return unread annotations only
            List<Annotation> unreadAnnotations = new ArrayList<Annotation>();
            for (Annotation an : annotations) {
                if (lastAnnotationReadTimestamp != null) {
                    if ((an.getCreationTimestamp() != null) && (an.getCreationTimestamp() > lastAnnotationReadTimestamp)) {
                        unreadAnnotations.add(an);
                    }
                } else {
                    unreadAnnotations.add(an);
                }
            }
            annotationList.setAnnotations(unreadAnnotations);
        } else { 
            // return all annotations
            annotationList.setAnnotations(annotations);
            // set the ProjectUser lastAnnotationReadTimestamp to current miliseconds and update it in the database
            projectUser.setLastAnnotationReadTimestamp(System.currentTimeMillis());
            pus.store(ctx, projectUser);
        }

        return annotationList;
    }
    
    @Override
    public boolean delete(AppContext ctx, ProjectPK projectPk) {
        try {
            Datastore ds = MongoDBHelper.getDatastore();
            Query<ProjectUser> query = ds.createQuery(ProjectUser.class).filter("id.projectId =",
                    projectPk.getProjectId());
            ds.findAndDelete(query);
        } catch (Exception e) {
        	logger.info("Delete error", e);
            return false;
        }

        return super.delete(ctx, projectPk);
    }

    /**
     * refresh the database data, clear caches
     * @param userContext
     * @param projectId
     * @return
     */
	public boolean refreshDatabase(AppContext ctx, String projectId) {
        try {
	        ProjectPK projectPK = new ProjectPK(ctx.getCustomerId(), projectId);
	        Project project = ProjectManager.INSTANCE.getProject(ctx, projectPK);
	        AccessRightsUtils.getInstance().checkRole(ctx, project, Role.WRITE);
	        //
	        try {
	            ProjectManager.INSTANCE.invalidate(project);
	        } catch (InterruptedException e) {
	        	logger.error("error while invalidating the DomainHierarchy for Project "+projectId,e);
	        }
	        //
	        DatabaseServiceImpl.INSTANCE.invalidate(project,true);
	        //
	        return true;
        } catch (ScopeException e) {
        	throw new ObjectNotFoundAPIException(e.getMessage(), e, false);
        }
	}

}
