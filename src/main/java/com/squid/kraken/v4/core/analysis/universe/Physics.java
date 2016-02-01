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
package com.squid.kraken.v4.core.analysis.universe;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.database.model.Database;
import com.squid.core.database.model.Table;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.attribute.AttributeServiceBaseImpl;
import com.squid.kraken.v4.api.core.dimension.DimensionServiceBaseImpl;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.database.impl.DatabaseServiceImpl;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Attribute;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.Relation;
import com.squid.kraken.v4.persistence.AppContext;

/**
 * Physics class implements the low-level interaction with the domain Model
 *
 */
public class Physics {
	
	static final Logger logger = LoggerFactory.getLogger(Physics.class);
	
	//public static final ProjectServiceBaseImpl PROJECT_SERVICE = ProjectServiceBaseImpl.getInstance();
	//public static final DomainServiceBaseImpl DOMAIN_SERVICE = DomainServiceBaseImpl.getInstance();
	public static final DimensionServiceBaseImpl DIMENSION_SERVICE = DimensionServiceBaseImpl.getInstance();
	//public static final MetricServiceBaseImpl METRIC_SERVICE = MetricServiceBaseImpl.getInstance();
	//public static final RelationServiceBaseImpl RELATION_SERVICE = RelationServiceBaseImpl.getInstance();
	
	private AppContext ctx;
	private Project project;
	
	private ProjectPK projectId = null;
	
	public Physics(AppContext ctx, Project project) {
		super();
		this.ctx = ctx;
		this.project = project;
		//
		this.projectId = project.getId();
	}

	public Project getProject() {
		return project;
	}
	
	public AppContext getContext() {
		return ctx;
	}
	
	/**
	 * check if user context has role on object
	 * @param object
	 * @param role
	 * @return
	 */
	public boolean hasRole(Persistent<?> object, Role role) {
		return AccessRightsUtils.getInstance().hasRole(ctx,object,role);
	}
	
	public Domain getDomain(DomainPK objectId) throws ScopeException {
		return ProjectManager.INSTANCE.getDomain(ctx, objectId);
	}
	
	public List<Domain> getDomains() throws ScopeException {
		//return DOMAIN_SERVICE.readAll(ctx, projectId);
		// T70:
		return ProjectManager.INSTANCE.getDomains(ctx, projectId);
	}
	
	public List<Relation> getRelations() throws ScopeException {
		//return RELATION_SERVICE.readAll(ctx, projectId);
		// T71:
		return ProjectManager.INSTANCE.getRelations(ctx,projectId);
	}
	
	/**
	 * this is too dangerous because we can build any kind of path... we must move that logic in the Cartography class that enforce the path cardinality
	 * Also we need to know the complete path from the true source, not the intermediate path.
	 * @param source
	 * @return
	 * @throws ScopeException 
	 */
	@Deprecated
	public Collection<Relation> getRelationsFrom(Domain source) throws ScopeException {
		return ProjectManager.INSTANCE.getRelation(ctx, source.getId());
	}
	
	/**
	 * get the relation from source domain and with name; return null if not found
	 * @param source
	 * @param name
	 * @return
	 * @throws ScopeException 
	 */
	public Relation getRelation(Domain source, String name) throws ScopeException {
		return ProjectManager.INSTANCE.getRelation(ctx, source.getId(), name);
	}
	
	public Domain getLeft(Relation relation) throws ScopeException {
		DomainPK id = relation.getLeftId();
		if (id.getProjectId()==null || id.getProjectId().equals("")) {
			id.setProjectId(relation.getId().getProjectId());
		}
		return ProjectManager.INSTANCE.getDomain(ctx, id);
	}
	
	public Domain getRight(Relation relation) throws ScopeException {
		DomainPK id = relation.getRightId();
		if (id.getProjectId()==null || id.getProjectId().equals("")) {
			id.setProjectId(relation.getId().getProjectId());
		}
		return ProjectManager.INSTANCE.getDomain(ctx, id);
	}

	public List<Dimension> getSubDimensions(Dimension dimension) {
		return DIMENSION_SERVICE.readSubDimensions(ctx, dimension.getId());
	}

	public List<Attribute> getAttributes(Dimension dimension) {
		return AttributeServiceBaseImpl.getInstance().readAll(ctx, dimension.getId());
	}
	
	public Database getDatabase() {
		return DatabaseServiceImpl.INSTANCE.getDatabase(project);
	}
	
	public Table getTable(String identifier) throws ExecutionException, ScopeException {
		return DatabaseServiceImpl.INSTANCE.lookupTable(project, identifier);
	}
	
	public List<Table> getTables() throws ExecutionException {
		return DatabaseServiceImpl.INSTANCE.getTables(project);
	}
	
}
