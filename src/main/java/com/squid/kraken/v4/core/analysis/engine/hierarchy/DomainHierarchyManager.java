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
package com.squid.kraken.v4.core.analysis.engine.hierarchy;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.core.analysis.engine.index.DimensionStoreManagerFactory;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.DimensionPK;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AppContext;

/**
 * This is the main entry point to interact with Domain Hierarchies.
 * 
 * @author sergefantino
 *
 */
public class DomainHierarchyManager {

	static final Logger logger = LoggerFactory
			.getLogger(DomainHierarchyManager.class);

	public static final DomainHierarchyManager INSTANCE = new DomainHierarchyManager();
	
	private LockableMap<DomainPK, DomainHierarchy> hierarchies = new LockableMap<DomainPK, DomainHierarchy>();

	private DomainHierarchyManager() {
		//
	}

	
	public void invalidate(DomainPK domainID) throws InterruptedException {
		// cancel any running execution
		DomainHierarchy hierarchy = hierarchies.get(domainID);	
		if (hierarchy != null){ 
			logger.info("Domain invalidation - Invalidating index for domain" + hierarchy.getRoot().getDomain().getName());
			// cancel any running execution		
			invalidate(hierarchy,true);
		}
		// clear the index
		DimensionStoreManagerFactory.INSTANCE.invalidate(domainID);
	}
	
	public void setStatesToStale(DomainPK domainId) {
		
	}
	

	public DomainHierarchy getHierarchy(ProjectPK projectPk, Domain domain)
			throws ComputingException, InterruptedException {
		return getHierarchy(projectPk, domain, 0);
	}
	
	public DomainHierarchy getHierarchySilent(ProjectPK projectPk, Domain domain) throws ComputingException, InterruptedException {
		return hierarchies.get(domain.getId());
	}

    /**
     * find the dimension identified by its PK - or null if cannot find or cannot access
     * @param ctx
     * @param dimensionPK
     * @return
     * @throws ScopeException
     * @throws InterruptedException 
     * @throws ComputingException 
     */
    public Dimension findDimension(AppContext ctx, ProjectPK projectPk, Domain domain, DimensionPK dimensionPK) throws ScopeException, ComputingException, InterruptedException {
    	DomainHierarchy hierarchy = getHierarchy(projectPk, domain, 0);
    	if (hierarchy!=null) {
    		return hierarchy.findDimension(ctx, dimensionPK);
    	} else {
    		return null;
    	}
    }

	public Metric findMetric(AppContext ctx, ProjectPK projectPk,
			Domain domain, String metricId) throws ComputingException, InterruptedException {
		DomainHierarchy hierarchy = getHierarchy(projectPk, domain, 0);
    	if (hierarchy!=null) {
    		return hierarchy.findMetric(ctx, metricId);
    	} else {
    		return null;
    	}
	}

	/**
	 * check if the hierarchy associated with the domain is ready; optionally
	 * wait for the completion (only if it is running)
	 * 
	 * @param universe
	 * @param domain
	 * @param b
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws TimeoutException
	 */
	public boolean isHierarchyDone(Universe universe, Domain domain,
			Integer timeoutMs) throws InterruptedException, TimeoutException,
			ExecutionException {
		DomainHierarchy hierarchy = hierarchies.get(domain.getId());
		if (hierarchy != null) {
			return hierarchy.isDone(timeoutMs);
		} else {
			return false;
		}
	}

	/**
	 * get the hierarchy. <li>transparently create it if required <li>
	 * 
	 * @param universe
	 * @param domain
	 * @param timeoutMs
	 *            timeout delay in milliseconds, or zero to wait indefinitely
	 * @return
	 * @throws ComputingException
	 * @throws InterruptedException
	 */
	protected DomainHierarchy getHierarchy(ProjectPK projectPk, Domain domain,
			int timeoutMs) throws ComputingException, InterruptedException {
		try {
			DomainHierarchy hierarchy = hierarchies.get(domain.getId());
			ReentrantLock lock = null;
			try {
				DomainHierarchy check = null;
				if (hierarchy != null && !hierarchy.isValid()) {
					lock = hierarchies.lock(domain.getId(), timeoutMs);// need to make invalidate & compute atomic
					// double check
					check = hierarchies.get(hierarchy.getRoot().getDomain().getId());
					if (check!=null && (check!=hierarchy || check.isValid())) {
						return check;
					}
					if (check!=null) {
						// remove and cancel
						hierarchy = hierarchies.remove(hierarchy.getRoot().getDomain().getId());// T595: better check if we are removing the good one
						if (hierarchy!=null) {
							logger.info("invalidated hierarchy for domain "+domain.getName()+" with version "+hierarchy.getVersion());
							hierarchy.cancel();// kill me please (but make sure it's me)
							hierarchy = null;
						} else {
							return hierarchy;
						}
					}
				}
				if (hierarchy != null) {
					return hierarchy;
				} else {
					if (lock==null) lock = hierarchies.lock(domain.getId(), timeoutMs);// if it's a new one
					// check race condition
					hierarchy = hierarchies.get(domain.getId());
					if (hierarchy == null) {
						hierarchy = computeHierarchy(projectPk, domain);
					}
					return hierarchy;
				}
			} finally {
				if (lock!=null) lock.unlock();
			}
		} catch (ScopeException e) {
			throw new RuntimeException("invalid request");
		}
	}

	private DomainHierarchy computeHierarchy(ProjectPK projectPk, Domain domain) throws ScopeException, InterruptedException {
		//
		// escalate context as root
    	AppContext rootctx = ServiceUtils.getInstance().getRootUserContext(projectPk.getCustomerId());
    	Project project = ProjectManager.INSTANCE.getProject(rootctx, projectPk);
		Universe root = new Universe(rootctx, project);
		//
		// fill the todo with the created domains
		DomainHierarchyCreator creator = new DomainHierarchyCreator(hierarchies);
		DomainHierarchy hierarchy = creator.createDomainHierarchy(root.S(domain));
		//
		// create the computer before starting the computation so we can cancel them
		final Deque<DomainHierarchyCompute> goals = new LinkedList<DomainHierarchyCompute>();
		for (DomainHierarchy h : creator.getTodo()) {
			DomainHierarchyCompute compute = new DomainHierarchyCompute(h);
			goals.push(compute);
		}
		//
		
		Runnable run = new Runnable() {
			
			@Override
			public void run() {
				while (!goals.isEmpty()) {
					DomainHierarchyCompute compute = goals.pop();
					if (Thread.interrupted()) {
						break;// stop
					}
					try {
						compute.call();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		};
		//
		Thread t = new Thread(run);
		t.start();
		//
		return hierarchy;
	}

	/**
	 * invalidate the given hierarchy if needed or if force
	 * 
	 * @param hierarchy
	 * @return null if the hierarchy has been invalidated or the valid hierarchy otherwise
	 */
	private DomainHierarchy invalidate(DomainHierarchy hierarchy, boolean force) {
		if (hierarchy != null) {
			// locking first
			ReentrantLock lock = hierarchies.lock(hierarchy.getRoot().getDomain().getId());
			try {
				// double check
				if (!force) {
					DomainHierarchy check = hierarchies.get(hierarchy.getRoot().getDomain().getId());
					if (check!=hierarchy || check.isValid()) {
						return check;
					}
				}
				// remove and cancel

				DomainHierarchy check = hierarchies.remove(hierarchy.getRoot().getDomain().getId());// T595: better check if we are removing the good one
				if (check!=null) {
					check.cancel();// kill me please (but make sure it's me)
					return null;
				} else {
					return check;
				}
			} finally {
				// unlock
				lock.unlock();
			}
		} else {
			return null;
		}
	}

}
