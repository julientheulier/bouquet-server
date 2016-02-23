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
package com.squid.kraken.v4.core.analysis.engine.cache;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.caching.redis.RedisCacheManager;
import com.squid.kraken.v4.core.database.impl.DatabaseServiceImpl;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchy;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchyManager;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.AccessTokenPK;
import com.squid.kraken.v4.model.AttributePK;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.DimensionPK;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.MetricPK;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectAnalysisJob;
import com.squid.kraken.v4.model.ProjectAnalysisJobPK;
import com.squid.kraken.v4.model.ProjectFacetJob;
import com.squid.kraken.v4.model.ProjectFacetJobPK;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.Relation;
import com.squid.kraken.v4.model.RelationPK;
import com.squid.kraken.v4.model.State;
import com.squid.kraken.v4.model.StatePK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DataStoreEvent;
import com.squid.kraken.v4.persistence.DataStoreEventObserver;

/**
 * Observes the Meta-Model and invalidates the impacted caches if needed.
 */
public class MetaModelObserver implements DataStoreEventObserver {

	static final Logger logger = LoggerFactory
			.getLogger(MetaModelObserver.class);

	private static MetaModelObserver instance;

	public static synchronized MetaModelObserver getInstance() {
		if (instance == null) {
			instance = new MetaModelObserver();
		}
		return instance;
	}

	private MetaModelObserver() {
	}
	
	private boolean acceptEvent(DataStoreEvent event) {
		Object origin = event.getOrigin();
		Object sourceEvent = event.getSource();
		if (origin == sourceEvent || origin == null) {
			// shortcut to avoid loosing too much time with stuff we don't mind
		    if (sourceEvent instanceof ProjectFacetJob 
		            || sourceEvent instanceof ProjectFacetJobPK
		            || sourceEvent instanceof ProjectAnalysisJob
		            || sourceEvent instanceof ProjectAnalysisJobPK
		            || sourceEvent instanceof State
		            || sourceEvent instanceof StatePK
		            || sourceEvent instanceof AccessTokenPK) {
		    // just in case...
		    	return false;
		    } else if (sourceEvent instanceof Persistent || sourceEvent instanceof GenericPK) {
		    	return true;
		    }
		}
		// else
		return false;
	}

	@Override
	public void notifyEvent(DataStoreEvent event) {
		//
		if (acceptEvent(event)) {
			DomainPK domainId = null;
			Object sourceEvent = event.getSource();
		    Persistent<?> sourceObject = null;
		    GenericPK sourcePk = null;
		    // krkn-99: if event is external, the source is always a PK
		    boolean local = true;
		    if (sourceEvent instanceof Persistent) {
		    	sourceObject = (Persistent<?>)sourceEvent;
		    	sourcePk = sourceObject.getId();
		    } else {
		    	sourcePk = (GenericPK)sourceEvent;
		    	local = false;
		    }

			// project update
			if ((sourcePk.getClass().equals(ProjectPK.class)) // don't get inherited PK
					&& event.getType() != DataStoreEvent.Type.CREATION) {
				ProjectPK projectPK = ((ProjectPK) sourcePk);
		        // force refresh
		        Project project = peekProject(projectPK, sourceObject);
	        	if (project!=null) {
	        		if (DatabaseServiceImpl.INSTANCE.invalidate(project,false)) {// invalidate the associated database if needed
		        		// T267 -- if the database need update, we have to invalidate the ES storage too
		        		try {
							ProjectManager.INSTANCE.invalidate(project);
						} catch (ScopeException | InterruptedException e) {
							logger.error("failed to invalidate project '"+project.getName()+"' with PK="+projectPK);
						}
		        	} else {
		        		// soft refresh ?
				        RedisCacheManager.getInstance().refresh(projectPK.toUUID());
		        	}
	        	}
			}

			// relation 
			if (sourcePk.getClass().equals(RelationPK.class)) {
				RelationPK id = (RelationPK) sourcePk;
				ArrayList<String> refreshList = new ArrayList<String>();
				refreshList.add(id.toUUID());
				ProjectPK projectPK = new ProjectPK(id.getCustomerId(),
						id.getProjectId());
		        Project project = peekProject(projectPK, sourceObject);
		        if (project!=null) {
					refreshList.add(projectPK.toUUID());
					// invalidate left & right domains
		        	Relation relation = getRelation(id, sourceObject);
					if (relation!=null && relation.getLeftId()!=null) {
						refreshList.add(relation.getLeftId().toUUID());
					}
	                if (relation!=null && relation.getRightId()!=null) {
						refreshList.add(relation.getRightId().toUUID());
	                }
					if (!refreshList.isEmpty()) {
		                RedisCacheManager.getInstance().refresh(refreshList);
					}
		        }
			}

			// domain
			if (sourcePk.getClass().equals(DomainPK.class)) {
				domainId = (DomainPK) sourcePk;
		        Project project = peekProject(domainId.getParent(), sourceObject);
		        if (project!=null) {
		        	ProjectManager.INSTANCE.refreshDomain(domainId);
		        }
			}
			
			// dimension
			if (sourcePk.getClass().equals(DimensionPK.class)) {
				DimensionPK id = (DimensionPK) sourcePk;
				domainId = id.getParent();
                //
				Project project = peekProject(domainId.getParent(), sourceObject);
		        if (project!=null) {
					List<String> refreshList = new ArrayList<String>();
					refreshList.add(domainId.toUUID());
					refreshList.add("H/"+domainId.toUUID());// refresh hierarchy too
					refreshList.add(id.toUUID());
	                //
	                // refresh parent if any
		        	Dimension dim = getDimension(id, sourceObject);
		        	if (dim!=null && dim.getParentId()!=null) {
				        Dimension parent = getDimension(dim.getParentId(), null);
				        if (project!=null && parent!=null) {
					        Domain domain = getDomain(domainId, null);
					        if (domain!=null) {
				        		Universe universe = new Universe(project);
				        		try {
				        			// get the hierarchy Root
					        		Axis axis = universe.S(domain).A(parent);
					        		DimensionIndex index = axis.getIndex();
					        		Dimension root = index.getRoot().getDimension();
					        		refreshList.add(root.getId().toUUID());
				        		} catch (InterruptedException | ComputingException | ScopeException e) {
				        			logger.error("error while updating hierarchy for dimension "+dim.getName()+": "+e.getMessage(),e);
				        		}
					        }
				        }
		        	}
		        	//
		        	// final refresh
	        		RedisCacheManager.getInstance().refresh(refreshList);
		        }
			}

            // dimension.attribute
            if (sourcePk.getClass().equals(AttributePK.class)) {
                AttributePK attr = (AttributePK) sourcePk;
                DimensionPK dim = (DimensionPK)attr.getParent();// this is the actual DimensionPK...
                // update the dimension
                domainId = new DomainPK(dim.getCustomerId(), dim.getProjectId(),
                        dim.getDomainId());
                //
				Project project = peekProject(domainId.getParent(), sourceObject);
				if (project!=null) {
					RedisCacheManager.getInstance().refresh(domainId.toUUID(), dim.toUUID(), attr.toUUID());
				}
            }

            // metric
            if (sourcePk.getClass().equals(MetricPK.class)) {
            	// deletion
            	MetricPK id = (MetricPK) sourcePk;
            	domainId = new DomainPK(id.getCustomerId(), id.getProjectId(),
            			id.getDomainId());
            	Project project = peekProject(domainId.getParent(), sourceObject);
            	if (project!=null) {
            		// no need to update the domain
            		RedisCacheManager.getInstance().refresh(domainId.toUUID(), id.toUUID());
            	}
            }
		}
	}
	
	private Project peekProject(ProjectPK pk, Persistent<?> sourceObject) {
		if (sourceObject!=null && sourceObject instanceof Project) {
			return (Project)sourceObject;
		}
		AppContext ctx = ServiceUtils.getInstance().getRootUserContext(pk.getCustomerId());
		try {
			return ProjectManager.INSTANCE.peekProject(ctx, pk);
		} catch (Exception e) {
			return null;
		}
	}
	
	private Relation getRelation(RelationPK pk, Persistent<?> sourceObject) {
		if (sourceObject!=null && sourceObject instanceof Relation) {
			return (Relation)sourceObject;
		}
		AppContext ctx = ServiceUtils.getInstance().getRootUserContext(pk.getCustomerId());
		try {
			return ProjectManager.INSTANCE.getRelation(ctx, pk);
		} catch (Exception e) {
			return null;
		}
	}
	
	private Domain getDomain(DomainPK pk, Persistent<?> sourceObject) {
		if (sourceObject!=null && sourceObject instanceof Domain) {
			return (Domain)sourceObject;
		}
		AppContext ctx = ServiceUtils.getInstance().getRootUserContext(pk.getCustomerId());
		try {
			return ProjectManager.INSTANCE.getDomain(ctx, pk);
		} catch (Exception e) {
			return null;
		}
	}
	
	private Dimension getDimension(DimensionPK pk, Persistent<?> sourceObject) {
		if (sourceObject!=null && sourceObject instanceof Dimension) {
			return (Dimension)sourceObject;
		}
		DomainPK domainPk = pk.getParent();
		AppContext ctx = ServiceUtils.getInstance().getRootUserContext(pk.getCustomerId());
		try {
			Domain domain = ProjectManager.INSTANCE.getDomain(ctx, domainPk);
			// get the hierarchy without generating any recomputation
			DomainHierarchy hierarchy = DomainHierarchyManager.INSTANCE.getHierarchySilent(domainPk.getParent(), domain);
			if (hierarchy!=null) {
				return hierarchy.getDimension(ctx, pk);
			} else {
				return null;
			}
		} catch (Exception e) {
			return null;
		}
	}

}
