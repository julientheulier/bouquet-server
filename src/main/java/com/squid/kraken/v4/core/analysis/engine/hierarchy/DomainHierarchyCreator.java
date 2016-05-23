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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.domain.IDomain;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.index.DimensionStoreException;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;

/**
 * this class is responsible for creating and populating the domain hierarchy before computing
 * @author sergefantino
 *
 */
public class DomainHierarchyCreator {

	static final Logger logger = LoggerFactory
			.getLogger(DomainHierarchyCreator.class);

	private LockableMap<DomainPK, DomainHierarchy> hierarchies;
	
	private List<DomainHierarchy> todo = new ArrayList<DomainHierarchy>();
	
	private List<DimensionIndex> subdomains = new ArrayList<>();
	
	// fix detection of cycles in sub-domains
	private Deque<DomainPK> in_progress = new ArrayDeque<DomainPK>();
	
	// managing parenthood
	private DomainHierarchyParenthood parenthood = new DomainHierarchyParenthood();
	
	/**
	 * allocate the creator
	 */
	public DomainHierarchyCreator(LockableMap<DomainPK, DomainHierarchy> hierarchies) {
		this.hierarchies = hierarchies;
	}
	
	public List<DomainHierarchy> getTodo() {
		return todo;
	}

	/**
	 * setup the DomainHierarchy
	 * @param space
	 * @return
	 * @throws InterruptedException
	 * @throws ScopeException
	 */
	public DomainHierarchy createDomainHierarchy(Space space) throws InterruptedException, ScopeException {
		//
		// create the hierarchy
		DomainHierarchy hierarchy = createDomainHierarchyRec(space);
		//
		// globally register
		hierarchies.put(space.getDomain().getId(), hierarchy);
		//
		// add to the todo list (to compute the indexes)
		todo.add(0, hierarchy);// T753: since now it is tail recursive, we add it at the beginning so that sub-domains get on top
		//
		// handles sub-domains
		populateSubDomains(hierarchy);
		//
		return hierarchy;
	}

	/**
	 * Lazy sub-domains initialization
	 * @param hierarchy 
	 */
	public void populateSubDomains(DomainHierarchy hierarchy) {
		//
		// make a copy of subdomains
		ArrayList<DimensionIndex> copy = new ArrayList<>(subdomains);
		subdomains.clear();
		//
		// this is the recursive part
		for (DimensionIndex root : copy) {
			Axis axis = root.getAxis();
			IDomain type = axis.getDimension().getImageDomain();
			try {
				List<DimensionIndex> results = new ArrayList<>();
				populateSubDomainHierarchy(hierarchy.getRoot(), root, axis, results, type);
				if (!results.isEmpty()){
					hierarchy.addHierarchy(results);
				}
			} catch (ScopeException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private DomainHierarchy createDomainHierarchyRec(Space space) throws InterruptedException, ScopeException {
		//
		logger.info("Populating Domain '"+space.getDomain().getName()+"' hierarchies");
		//
		in_progress.push(space.getDomain().getId());// make sure we remember where we came
		//
		DomainContent content = ProjectManager.INSTANCE.getDomainContent(space);
		//
		parenthood.add(content.getDimensions());
		List<List<DimensionIndex>> hierarchies = populateHierarchy(space, content.getDimensions());
		
		if(hierarchies == null || hierarchies.size() == 0) {
			logger.warn("Empty Hierarchy");
		}
		DomainHierarchy hierarchy = new DomainHierarchy(space, hierarchies);
		//
		in_progress.pop();
		//
		return hierarchy;
	}
	

	/**
	 * populate the hierarchy from a base Space
	 * @param space: the space to build the hierarchy for
	 * @param dims : the available dims
	 * @param todo : (output) the list of DomainHierarchy to compute (including sub-domains if required)
	 * 
	 * @return the DimensionIndexes, grouped by Hierarchy
	 * @throws InterruptedException
	 */
	private List<List<DimensionIndex>> populateHierarchy(Space space,
			List<Dimension> dims) throws InterruptedException 
	{
		List<List<DimensionIndex>> result = new ArrayList<>();
		// krkn-93: use the dynamic dimension list
//		logger.info(" populate hierarchy  dimensions " + dims.toString());
		for (Dimension dimension : dims) {
			logger.info("populateHierarchy " + dimension.toString());
			if (dimension.getParentId()==null) {// only root dimension
				Axis axis = space.A(dimension);
				List<DimensionIndex> hierarchy = populateHierarchy(space, null,
						null, axis);
				if (!hierarchy.isEmpty()) {
					result.add(hierarchy);
				}
			}
		//	logger.info("populateHierarchy " + dimension.toString() + " ok ");
		}
		return result;
	}

	/**
	 * returns the root sub-dimension identified by axis
	 * 
	 * @param space
	 * @param parent
	 * @param root
	 * @param axis
	 * @param todo
	 * @param lock
	 * 
	 * @throws InterruptedException
	 */
	private List<DimensionIndex> populateHierarchy(Space space, Axis parent,
			DimensionIndex root, Axis axis)
			throws InterruptedException {
		ArrayList<DimensionIndex> result = new ArrayList<DimensionIndex>();
		try {
			IDomain type = axis.getDimension().getImageDomain();
			if (type.isInstanceOf(IDomain.OBJECT)) {
				// handling sub-domains with full inclusion
				if (parent == null) {
					populateSubDomainHierarchyLazy(space,root,axis,result, type);
					// add as an index too ?
					if (!dimensionIndexInList(result, axis)) {
						DimensionIndex index = DimensionIndexCreationUtils
								.createIndex(root, axis, type);
						result.add(index);
					}
				} else {
					// not OK
					logger.error("Invalid definition: Object dimension cannot have a parent");
					if (!dimensionIndexInList(result, axis)) {
						result.add(DimensionIndexCreationUtils
								.createInvalidIndex(root, axis,
										"Invalid definition: Object dimension cannot have a parent"));
					}
				}
			} else {
				// standard dimension
				if (!dimensionIndexInList(result, axis)) {
					DimensionIndex index = DimensionIndexCreationUtils
							.createIndex(root, axis, type);
					result.add(index);
					// still try to populate the sub-domains to provide at least
					// the
					// hierarchy layout
					List<DimensionIndex> sub = populateSubDimensions(space,
							axis, index);
					result.addAll(sub);
				}
			}
		} catch (Exception e) {
			// invalid Dimension
			logger.error("error while evaluating the dimension: "
					+ e.getLocalizedMessage());
			if (!dimensionIndexInList(result, axis)) {
				DimensionIndex index = DimensionIndexCreationUtils
						.createInvalidIndex(
								root,
								axis,
								"error while evaluating the dimension: "
										+ e.getLocalizedMessage());
				result.add(index);

				// still try to populate the sub-domains to provide at least the
				// hierarchy layout
				List<DimensionIndex> sub = populateSubDimensions(space, axis,
						index);
				result.addAll(sub);

			}
		}
//		logger.info("populate hierarchy ok") ;
		
		return result;
	}
	
	/**
	 * do not actually populate the subDomains, just create a DimensionIndex for the sub-domain link, and delay the actual creation of the content for latter
	 * @param space
	 * @param parent
	 * @param root
	 * @param axis
	 * @param result
	 * @param type
	 * @throws InterruptedException
	 * @throws ScopeException
	 */
	private void populateSubDomainHierarchyLazy(Space space, DimensionIndex root, Axis axis, ArrayList<DimensionIndex> result, IDomain type) throws InterruptedException, ScopeException {
		// OK -- insert the target dimensions
		Object adapter = type.getAdapter(Domain.class);
		if (adapter != null && adapter instanceof Domain) {
			Domain target = (Domain) adapter;
			logger.info("creating subdomain for domain " + target.toString() + " in " + space.toString());
			Axis source = axis;
			try {
				DimensionIndex self = new DimensionIndex(null, source);
				subdomains.add(self);
			} catch (DimensionStoreException e) {
				logger.error("failed to create subdomain for domain " + target.toString() + " in " + space.toString());
			}
		} else {
			logger.error("Unable to resolve the sub-domain");
			if (!dimensionIndexInList(result, axis)) {
				result.add(DimensionIndexCreationUtils
						.createInvalidIndex(root, axis,
								"Unable to resolve the sub-domain"));
			}
		}
	}
	
	/**
	 * populate the hierarchy with the sub-domain, by creating a proxy for each index from the sub-domain hierarchy
	 * @param space
	 * @param parent
	 * @param root
	 * @param axis
	 * @param todo
	 * @param result
	 * @param type
	 * @throws InterruptedException
	 * @throws ScopeException
	 */
	private void populateSubDomainHierarchy(Space space, DimensionIndex root, Axis axis, List<DimensionIndex> result, IDomain type) throws InterruptedException, ScopeException {
		// OK -- insert the target dimensions
		Object adapter = type.getAdapter(Domain.class);
		if (adapter != null && adapter instanceof Domain) {
			Domain target = (Domain) adapter;
			logger.info("creating subdomain for domain " + target.toString()  );

			try {
				// get the sub-domain hierarchy
				// avoid dead-lock in case of cyclic relationship
				// using wait=false
				if (in_progress.contains(target.getId())) {
				//if (space.getDomains().contains(target)) {
					logger.info("Cyclic relation between "+axis+" and sub-domain "+target);
					if (!dimensionIndexInList(result, axis)) {

						result.add(DimensionIndexCreationUtils
								.createInvalidIndex(root, axis,
										"Cyclic relation between "+axis+" and sub-domain "+target));
					}
				} else {
					DomainHierarchy hierarchy = this.hierarchies
							.get(target.getId());
					if (hierarchy == null || !hierarchy.isValid()) {
						// we need to create the hierarchy for the
						// sub-domain first
						// but we also need to take care of the lock
						ReentrantLock lock = hierarchies.lock(target.getId());
						try {
							DomainHierarchy check = hierarchies.get(target.getId());
							if (check!=null && check!=hierarchy && check.isValid()) {
								hierarchy = check;
							}
							if (hierarchy==null || !hierarchy.isValid()) {
								//ExpressionAST def = axis.getDefinitionSafe();
								//Space chained = space.S(def);
								Space unchained = space.getUniverse().S(target);// loosing the origin here
								hierarchy = createDomainHierarchy(unchained);
							}
						} finally {
							lock.unlock();
						}
					}
					if (hierarchy != null) {
						logger.info("linking axis "+ axis +" to proxy "
								+ hierarchy.toString());
						// create the axis that traverse the
						// main dimension to join the sub-domain
						try {
							result.addAll(DimensionIndexCreationUtils
									.createProxyIndexes(space,
											hierarchy, axis));
						} catch (ScopeException e) {
							// ok, just ignore for now
							logger.info("scope exception on linkin axis");
						}
					} else {
						// detected a cyclic relation
						logger.info("Cyclic relation between "+axis+" and sub-domain "+target);
						if (!dimensionIndexInList(result, axis)) {

							result.add(DimensionIndexCreationUtils
									.createInvalidIndex(root, axis,
											"Cyclic relation between "+axis+" and sub-domain "+target));
						}
					}
				}
			} catch (ComputingException e) {
				logger.error("Unable to compute the sub-domain '"
						+ target.getName() + "'");
				if (!dimensionIndexInList(result, axis)) {

					result.add(DimensionIndexCreationUtils
							.createInvalidIndex(root, axis,
									"Unable to compute the sub-domain '"
											+ target.getName()
											+ "'"));
				}
			}
//			logger.info(" subdomain for domain " + target.toString()  +" created");

		} else {
			logger.error("Unable to resolve the sub-domain");
			if (!dimensionIndexInList(result, axis)) {
				result.add(DimensionIndexCreationUtils
						.createInvalidIndex(root, axis,
								"Unable to resolve the sub-domain"));
			}
		}
	}

	private boolean dimensionIndexInList(List<DimensionIndex> indexes, Axis axis) {
		for (DimensionIndex d : indexes) {
			if (axis.equals(d.getAxis())) {
				return true;
			}
		}
		return false;
	}

	private List<DimensionIndex> populateSubDimensions(Space space, Axis axis,
			DimensionIndex index)
			throws InterruptedException {
		if (axis != null) {
			List<Axis> subhierarchy = parenthood.getHierarchy(axis);
			if (!subhierarchy.isEmpty()) {
				ArrayList<DimensionIndex> result = new ArrayList<DimensionIndex>();
				for (Axis a : subhierarchy) {
					result.addAll(populateHierarchy(space, axis, index, a));
				}
				return result;
			}
		}
		return Collections.emptyList();
	}

}
