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
package com.squid.kraken.v4.core.analysis.engine.project;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.elasticsearch.common.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.squid.core.database.model.Table;
import com.squid.core.expression.reference.RelationDirection;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.InvalidCredentialsAPIException;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.caching.redis.RedisCacheManager;
import com.squid.kraken.v4.caching.redis.generationalkeysserver.RedisKey;
import com.squid.kraken.v4.core.analysis.engine.cartography.Cartography;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainContent;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchyManager;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.LzPersistentBaseImpl;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.Relation;
import com.squid.kraken.v4.model.RelationPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.ProjectDAO;

/**
 * Manage Projects dynamic content: list the Domains & relation, both from the model (mongo) and database (T70/T71).
 * The project content also manages the old cartography model to simplify update and refresh (single lock, also a strong dependency between cartography and relations).
 * 
 * @author sergefantino
 *
 */
public class ProjectManager {

	static final Logger logger = LoggerFactory
			.getLogger(ProjectManager.class);

	public static final ProjectManager INSTANCE = new ProjectManager();

    private DAOFactory factory = DAOFactory.getDAOFactory();
    
    private Map<ProjectPK, ProjectDynamicContent> projects = new ConcurrentHashMap<ProjectPK, ProjectDynamicContent>();

	private ConcurrentHashMap<ProjectPK, ReentrantLock> project_locks = new ConcurrentHashMap<>();
	
	public ProjectManager() {
		projects = new ConcurrentHashMap<ProjectPK, ProjectDynamicContent>();
	}
	
	/**
	 * load the project according to the context
	 * @param ctx
	 * @param projectPk
	 * @return
	 * @throws ScopeException
	 */
	public Project getProject(AppContext ctx, ProjectPK projectPk) throws ScopeException {
		Optional<Project> project = ((ProjectDAO) factory.getDAO(Project.class)).read(
				ctx, projectPk);
		if (project.isPresent()) {
			return project.get();
		} else {
			throw new ScopeException("cannot find project with PK = "+projectPk);
		}
	}

	/**
	 * get the list of domains according to the context:
	 * - if user has only READ access to the project, do not list dynamic domains
	 * - if user has only READ access to the project, list only domains with metrics (?)
	 * - if user has WRITE access to the project, show everything
	 * @param ctx
	 * @param projectPk
	 * @return
	 * @throws ScopeException
	 */
	public List<Domain> getDomains(AppContext ctx, ProjectPK projectPk) throws ScopeException {
		ProjectDynamicContent domains = getProjectContent(ctx, projectPk);
		return filterDomains(ctx, projectPk, domains.getDomains());
	}

	/**
	 * retrieve the Domain with the given PK according to the context, or thrown an ObjectNotFoundAPIException
	 * @param ctx
	 * @param domainPk
	 * @return the Domain
	 * @throws ScopeException if cannot find the domain
	 * @throws InvalidCredentialsAPIException
	 *             if user hasn't required role.
	 */
	public Domain getDomain(AppContext ctx, DomainPK domainPk) throws ScopeException {
		if (domainPk.getCustomerId()==null) {
			domainPk.setCustomerId(ctx.getCustomerId());// why ???
		}
		ProjectDynamicContent domains = getProjectContent(ctx, domainPk.getParent());
		Domain domain = domains.get(domainPk);
		if (domain!=null) {
			checkRole(ctx, domain);
			return cloneWithRole(ctx, domain);
		}
		// else
		throw new ScopeException("cannot find Domain with PK="+domainPk);
	}
	
	/**
	 * refresh the domain content and data
	 * @param ctx
	 * @param domainPk
	 * @return
	 * @throws ScopeException
	 */
	public RedisKey refreshDomainData(AppContext ctx, DomainPK domainPk) throws ScopeException {
		Project project = ProjectManager.INSTANCE.getProject(ctx, domainPk.getParent());
		Domain domain = ProjectManager.INSTANCE.getDomain(ctx, domainPk);
		//
		Universe universe = new Universe(ctx, project);
		Space space = universe.S(domain);
		Table table = space.getTable();
		String uuid = universe.getTableUUID(table);
		if (uuid != null) {
			RedisCacheManager.getInstance().refresh(uuid);
			// and refresh the table?
			table.refresh();
		}
		RedisCacheManager.getInstance().refresh(domainPk.toUUID());
		RedisKey key = RedisCacheManager.getInstance().getKey(domainPk.toUUID());
		try {
			DomainHierarchyManager.INSTANCE.invalidate(domainPk);
		} catch (InterruptedException e) {
			// ignore
		}
		return key;
	}
	
	/**
	 * return the domain if possible
	 * @param ctx
	 * @param domainPk
	 * @return
	 */
	public Optional<Domain> getDomainSafe(AppContext ctx, DomainPK domainPk) {
		try {
			Domain domain = getDomain(ctx, domainPk);
			return Optional.of(domain);
		} catch (Exception e) {
			return Optional.absent();
		}
	}

	/**
	 * look for a domain with this name in the project identified by projectPk
	 * @param ctx
	 * @param parent
	 * @param name
	 * @return
	 * @throws ScopeException if the project does not exist
	 */
	public Domain findDomainByName(AppContext ctx, ProjectPK projectPk, String name) throws ScopeException {
		ProjectDynamicContent content = getProjectContent(ctx, projectPk);
		return content.findDomainByName(name);
	}

	/**
	 * get all the relations for the given PK according to the context
	 * @param ctx
	 * @param projectPk
	 * @return
	 * @throws ScopeException
	 */
	public List<Relation> getRelations(AppContext ctx, ProjectPK projectPk) throws ScopeException {
		ProjectDynamicContent domains = getProjectContent(ctx, projectPk);
		List<Relation> rels = domains.getRelations();
		if (rels==null) {
			// handle the race-condition
			return Collections.emptyList();
		} else {
			List<Relation> filter = new ArrayList<Relation>();
			for (Relation rel : rels) {
				if (hasRole(ctx, rel)) {
					filter.add(cloneWithRole(ctx, rel));
				}
			}
			return filter;
		}
	}

	/**
	 * get the relations connected to the given domain.
	 * A relation is connected to the given domain if any of it's ending (left or right) are equal to this domain.
	 * @param ctx
	 * @param domainPK
	 * @return
	 * @throws ScopeException 
	 */
	public List<Relation> getRelation(AppContext ctx, DomainPK domainPK) throws ScopeException {
		ProjectDynamicContent domains = getProjectContent(ctx, domainPK.getParent());
		List<Relation> rels = domains.getRelations();
		if (rels==null) {
			// handle the race-condition
			return Collections.emptyList();
		} else {
			List<Relation> filter = new ArrayList<Relation>();
			for (Relation rel : rels) {
				if (rel.getDirection(domainPK)!=RelationDirection.NO_WAY && hasRole(ctx, rel)) {
					filter.add(cloneWithRole(ctx, rel));
				}
			}
			return filter;
		}
	}

	/**
	 * look for a relation connected to the given domain with either left or right name equals to the given name 
	 * @param ctx
	 * @param domainPK
	 * @param name
	 * @return
	 * @throws ScopeException
	 */
	public Relation getRelation(AppContext ctx, DomainPK domainPK, String name) throws ScopeException {
		ProjectDynamicContent domains = getProjectContent(ctx, domainPK.getParent());
		List<Relation> rels = domains.getRelations();
		if (rels==null) {
			// handle the race-condition
			return null;
		} else {
			for (Relation rel : rels) {
				RelationDirection direction = rel.getDirection(domainPK);
				if (direction==RelationDirection.LEFT_TO_RIGHT && rel.getRightName().compareTo(name)==0) {
					// LEFT TO RIGHT
					checkRole(ctx, rel);
					return cloneWithRole(ctx, rel);
				} else if (direction==RelationDirection.RIGHT_TO_LEFT && rel.getLeftName().compareTo(name)==0) {
					checkRole(ctx, rel);
					return cloneWithRole(ctx, rel);
				} else {
					// ignore
				}
			}
			return null;
		}
	}

	/**
	 * get the relation with the given PK
	 * @param ctx
	 * @param relationPk
	 * @return
	 * @throws ScopeException
	 */
	public Relation getRelation(AppContext ctx, RelationPK relationPk) throws ScopeException {
		if (relationPk.getCustomerId()==null) {
			relationPk.setCustomerId(ctx.getCustomerId());// why ???
		}
		ProjectDynamicContent domains = getProjectContent(ctx, relationPk.getParent());
		Relation rel = domains.get(relationPk);
		if (rel!=null) {
			checkRole(ctx, rel);
			return cloneWithRole(ctx, rel);
		}
		// else
		throw new ObjectNotFoundAPIException("cannot find Relation "+relationPk,true);
	}
	
	/**
	 * try to find the relation with the given PK
	 * @param ctx
	 * @param relationPk
	 * @return the relation if it exists or null if no relation with this ID
	 * @throws ScopeException if the project is not defined
	 */
	public Relation findRelation(AppContext ctx, RelationPK relationPk) throws ScopeException {
		if (relationPk.getCustomerId()==null) {
			relationPk.setCustomerId(ctx.getCustomerId());// why ???
		}
		ProjectDynamicContent domains = getProjectContent(ctx, relationPk.getParent());
		Relation rel = domains.get(relationPk);
		if (rel!=null) {
			checkRole(ctx, rel);
			return cloneWithRole(ctx, rel);
		}
		// else
		return null;
	}
	
	/**
	 * get the cartography for the given project
	 * @param ctx
	 * @param projectPk
	 * @return
	 * @throws ScopeException
	 */
	public Cartography getCartography(AppContext ctx, ProjectPK projectPk) throws ScopeException {
		ProjectDynamicContent content = getProjectContent(ctx, projectPk);
		if (content.getCartography()==null) {
			throw new ScopeException("failed to compute the cartography");
		}
		return content.getCartography();
	}
	
	public DomainContent getDomainContent(Space space) throws ScopeException {
		ProjectDynamicContent content = getProjectContent(space.getUniverse());
		return content.getDomainContent(space);
	}

	/**
	 * get the project content for the universe
	 * @param universe
	 * @return
	 * @throws ScopeException
	 */
	protected ProjectDynamicContent getProjectContent(Universe universe) throws ScopeException {
		return getProjectContent(universe.getContext(), universe.getProject().getId());
	}

	/**
	 * get the content for the given project
	 * @param ctx
	 * @param projectPk
	 * @return
	 * @throws ScopeException
	 */
	protected ProjectDynamicContent getProjectContent(AppContext ctx, ProjectPK projectPk) throws ScopeException {
		ProjectDynamicContent content = projects.get(projectPk);
		// we use a special key to invalidate the project's domain, which in turn depend on the project key
		String genkey = getProjectContentGenkey(projectPk);
		if (content==null || !genkey.equals(content.getGenkey())) {
			ReentrantLock lock = lock(projectPk);
			try {
				content = projects.get(projectPk);
				if (content!=null && !genkey.equals(content.getGenkey())) {
					projects.remove(projectPk);
					content = null;
				}
				if (content==null) {
					return createProjectContent(ctx, projectPk, genkey);
				}
			} finally {
				lock.unlock();
			}
		}
		return content;
	}
	
	protected String getProjectContentGenkey(ProjectPK projectPk) {
		return RedisCacheManager.getInstance().getKey(projectPk.toUUID()+"/domains",projectPk.toUUID()).getStringKey();
	}

	/**
	 * refresh a domain (and the project domain list)
	 * @param domainId
	 */
	public void refreshDomain(DomainPK domainPk){
		ProjectPK projectPk = new ProjectPK(domainPk.getCustomerId(),
				domainPk.getProjectId());

		RedisCacheManager.getInstance().refresh(domainPk.toUUID(), projectPk.toUUID()+"/domains");
	}

	public void refreshDomain(DomainPK domainPk, ArrayList<DomainPK> domains){
		ProjectPK projectPk = new ProjectPK(domainPk.getCustomerId(),
				domainPk.getProjectId());

		ArrayList<String> refresh = new ArrayList<String>();
		refresh.add(domainPk.toUUID());
		refresh.add(projectPk.toUUID()+"/domains");
		// Trying to refresh domains that are a dependencies of project.
		for(DomainPK domainpk: domains){
			refresh.add(domainpk.toUUID());
		}
		RedisCacheManager.getInstance().refresh(refresh);
	}
	/**
	 * refresh project content
	 * @param projectPk
	 */
	public void refreshContent(ProjectPK projectPk) {
        RedisCacheManager.getInstance().refresh(projectPk.toUUID()+"/domains");// T70: refresh the domains
	}
	
	/**
	 * create content for the given project. The genkey is used to invalidate the content. 
	 * @param ctx
	 * @param projectPk
	 * @param key
	 * @return
	 * @throws ScopeException
	 */
	protected ProjectDynamicContent createProjectContent(AppContext ctx, ProjectPK projectPk, String genkey) throws ScopeException {
		// use root context
		AppContext asRoot = ServiceUtils.getInstance().getRootUserContext(ctx);
		Project project = getProject(asRoot, projectPk);
		logger.info("refreshing content for Project '"+project.getName()+"'");
		Universe root = new Universe(asRoot, project);
		ProjectDynamicContent content = new ProjectDynamicContent(genkey);
		HashMap<Table, Domain> coverage = new HashMap<Table, Domain>();
		List<Domain> domains = DynamicManager.INSTANCE.loadDomains(root,coverage);
		content.setDomains(domains);
		// we need to expose the domains before stepping into the relation computation because the parser will use it
		projects.put(projectPk, content);
		try {
			List<Relation> relations = DynamicManager.INSTANCE.loadRelations(root,domains,coverage);
			content.setRelations(relations);
			//
			Cartography cartography = new Cartography();
			cartography.compute(domains, relations);
			content.setCartography(cartography);
			//
			return content;
		} catch (Exception e) {
			// this is not expected, but the content is already available
			// so just make sure to release any lock
			content.release();
			return content;
		}
	}

	private List<Domain> filterDomains(AppContext ctx, ProjectPK projectPk, List<Domain> domains) {
		List<Domain> filteredDomains = new ArrayList<Domain>();
		for (Domain domain : domains) {
			if (hasRole(ctx, domain)) {
				filteredDomains.add(cloneWithRole(ctx, domain));
			}
		}
		return filteredDomains;
	}

	public boolean hasRole(AppContext ctx, Domain object) {
    	Role role;
    	if (object.isDynamic()) {
    		role = Role.WRITE;
    	} else {
    		role = Role.READ;
    	}
		return AccessRightsUtils.getInstance().hasRole(ctx,object,role);
	}
	
	public void checkRole(AppContext ctx, Domain object) {
    	Role role;
    	if (object.isDynamic()) {
    		role = Role.WRITE;
    	} else {
    		role = Role.READ;
    	}
		AccessRightsUtils.getInstance().checkRole(ctx, object, role);
	}

	public boolean hasRole(AppContext ctx, Relation object) {
    	Role role;
    	if (object.isDynamic()) {
    		role = Role.WRITE;
    	} else {
    		role = Role.READ;
    	}
		return AccessRightsUtils.getInstance().hasRole(ctx,object,role);
	}
	
	public void checkRole(AppContext ctx, Relation object) {
    	Role role;
    	if (object.isDynamic()) {
    		role = Role.WRITE;
    	} else {
    		role = Role.READ;
    	}
		AccessRightsUtils.getInstance().checkRole(ctx, object, role);
	}

	private <TYPE extends LzPersistentBaseImpl<? extends GenericPK>> TYPE cloneWithRole(AppContext ctx, TYPE obj) {
		try {
			TYPE copy = (TYPE)obj.clone();
			AccessRightsUtils.getInstance().setRole(ctx, copy);
			return copy;
		} catch (CloneNotSupportedException e) {
			return obj;
		}
	}

	private ReentrantLock lock(ProjectPK projectPk) {
		try {
			return lock(projectPk, 0);
		} catch (InterruptedException e) {
			// we never get there
			throw new RuntimeException("impossible exception",e);
		}
	}
	
	private ReentrantLock lock(ProjectPK projectPk, int timeoutMs) throws InterruptedException {
		ReentrantLock mylock = new ReentrantLock();
		mylock.lock();
		ReentrantLock lock = project_locks.putIfAbsent(projectPk, mylock);
		if (lock==null) {
			lock = mylock;// I already own the lock
		} else {
			if (timeoutMs > 0) {
				lock.tryLock(timeoutMs, TimeUnit.MILLISECONDS);
			} else {
				lock.lock();
			}
		}
		return lock;
	}
	
	public void invalidate(Project project) throws InterruptedException, ScopeException {
		// load the list first
		List<Domain> domains = ProjectManager.INSTANCE.getDomains(ServiceUtils.getInstance().getRootUserContext(project.getId().getCustomerId()), project.getId());
		// ... then refresh the project
        RedisCacheManager.getInstance().refresh(project.getId().toUUID());
        // ... and force domains invalidation
		for(Domain d :domains ){
			if (! d.isDynamic()){
				logger.info("Invalidating domain " + d.getName());
				DomainHierarchyManager.INSTANCE.invalidate(d.getId());		
			}
		}
	}
	
}
