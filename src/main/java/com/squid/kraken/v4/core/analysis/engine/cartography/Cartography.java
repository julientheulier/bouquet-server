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
package com.squid.kraken.v4.core.analysis.engine.cartography;

import java.util.ArrayList; 
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.cartography.Path.Type;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.Relation;

/**
 * Compute & maintain the Domain cartography using relation as vertices
 *
 */
public class Cartography {
    
    static final Logger logger = LoggerFactory.getLogger(Cartography.class);
	
	private HashMap<DomainPK, LinkedHashSet<DomainPK>> vicinity;
	private HashMap<DomainPK, LinkedHashSet<DomainPK>> direct_vicinity;
	
	private HashMap<Vector, List<Path>> map;

	public void compute(List<Domain> domains, List<Relation> relations) {
		vicinity = new HashMap<DomainPK, LinkedHashSet<DomainPK>>();
		direct_vicinity = new HashMap<DomainPK, LinkedHashSet<DomainPK>>();
		map = new HashMap<Vector, List<Path>>();
		bootstrap(relations);
		int incr = 0;
		while (!Thread.currentThread().isInterrupted() && increment(domains) && incr<100) {
			// keep going...
			logger.info("cartography: iteration #"+(++incr));
		}
		if (incr>=20) {
			logger.info("cartography: interrupted after "+(incr-1)+" iterations");
		}
	}

	/**
	 * return all existing path from the source to any target
	 * @param source
	 * @return
	 */
	public List<Path> getPaths(DomainPK source) {
		List<Path> paths = new ArrayList<Path>();
		LinkedHashSet<DomainPK> vicinity =  getVicinity(source);
		for (DomainPK target : vicinity) {
			paths.addAll(getPaths(source, target));
		}
		return paths;
	}
	
	/**
	 * check if the domain is a "fact", i.e. there is no incoming path
	 * @param source
	 * @return
	 */
	public boolean isFactDomain(DomainPK source) {
		LinkedHashSet<DomainPK> vicinity =  getVicinity(source);
		for (DomainPK target : vicinity) {
			List<Path> paths = getPaths(source, target);
			for (Path path : paths) {
				switch (path.getType()) {
				case MANY_MANY:
				case ONE_MANY:
				case INFINITE:
					return false;// there is a detailed relation
				default:
					// continue
				}
			}
		}
		return true;
	}
	
	/**
	 * Return all existing path between source and target
	 * @param source
	 * @param target
	 * @return
	 */
	public List<Path> getPaths(DomainPK source, DomainPK target) {
		Vector key = new Vector(source, target);
		List<Path> value = map.get(key);
		if (value==null) {
			value = new ArrayList<Path>();
			map.put(key, value);
		}
		return value;
	}

	public List<Space> getAllPaths(Universe universe, Domain source, Domain target) {
		List<Space> result = new ArrayList<Space>();
		List<Path> paths = getPaths(source.getId(),target.getId());
		for (Path path : paths) {
			try {
				result.add(path.apply(universe.S(source)));
			} catch (Exception e) {
				// catch also InvalidCredentialsAPIException
				// and keep going
			}
		}
		return result;
	}
	
	public Space getShortestPath(Universe universe, Domain source, Domain target) throws ScopeException {
		List<Path> paths = getPaths(source.getId(),target.getId());
		// get the first
		if (paths.isEmpty()) return null;
		return paths.get(0).apply(universe.S(source));
	}

	public List<Space> getSubspaces(Universe universe, Space space) throws ScopeException {
		if (space.getParent()==null) {
			return getSubspaces(universe, space.getDomain());
		} else {
			// build the initial path
			LinkedHashSet<DomainPK> trace = new LinkedHashSet<DomainPK>();
			trace.add(space.getDomain().getId());
			Space parent = space.getParent();
			trace.add(parent.getDomain().getId());
			Path path = new DirectPath(space.getRelation(), parent.getDomain().getId());
			while (parent.getParent()!=null) {
				path = new CompositePath(new DirectPath(parent.getRelation(), parent.getParent().getDomain().getId()), path);
				if (path.getType()==Type.INFINITE) {
					throw new ScopeException("the space "+space.toString()+" is not a valid path (crossxproduct)");
				}
				parent = parent.getParent();
				trace.add(parent.getDomain().getId());
			}
			//
			List<Space> result = new ArrayList<Space>();
			for (DomainPK target : getDirectVicinity(space.getDomain().getId())) {
				if (!trace.contains(target)) {
					List<Path> paths = getPaths(space.getDomain().getId(),target);
					for (Path next : paths) {
						if (next instanceof DirectPath) {
							CompositePath append = new CompositePath(path, next);
							if (append.getType()!=Type.INFINITE) {
								try {
									result.add(next.apply(space));
								} catch (Exception e) {
									// catch also InvalidCredentialsAPIException
									// and keep going
								}
							}
						}
					}
				}
			}
			return result;
		}
	}
	
	public Type computeType(Space space) {
		Domain source = space.getRoot();
		Domain target = space.getDomain();
		List<Path> paths = getPaths(source.getId(), target.getId());
		for (Path path : paths) {
			if (path.equals(space)) {
				return path.getType();
			}
		}
		return Type.INFINITE;// ok, actually we don't know
	}
	
	/**
	 * first populate a direct map: for domain A, list all domains with a direct relation R in between
	 * now for each domain, extend the accessible zone by one step
	 * continue until nothing left to add
	 * 
	 */
	protected void bootstrap(List<Relation> relations) {
		for (Relation relation : relations) {
			DomainPK left = relation.getLeftId();
			DomainPK right = relation.getRightId();
			if (left!=null && right!=null) {
				left.setCustomerId(relation.getCustomerId());
				right.setCustomerId(relation.getCustomerId());
				// add the path from source to target
				getVicinity(left).add(right);
				getDirectVicinity(left).add(right);
				getPaths(left, right).add(new DirectPath(relation, left));
				// add the reverse path from target to source if not left==right
				if (!left.equals(right)) {
					getVicinity(right).add(left);
					getDirectVicinity(right).add(left);
					getPaths(right, left).add(new DirectPath(relation, right));
				}
			}
		}
	}

	protected LinkedHashSet<DomainPK> getVicinity(DomainPK source) {
		LinkedHashSet<DomainPK> result = vicinity.get(source);
		if (result==null) {
			result = new LinkedHashSet<DomainPK>();
			vicinity.put(source, result);
		}
		return result;
	}

	protected LinkedHashSet<DomainPK> getDirectVicinity(DomainPK source) {
		LinkedHashSet<DomainPK> result = direct_vicinity.get(source);
		if (result==null) {
			result = new LinkedHashSet<DomainPK>();
			direct_vicinity.put(source, result);
		}
		return result;
	}

	/**
	 * 
	 * @param domains
	 */
	protected boolean increment(List<Domain> domains) {
		boolean stable = true;
		for (Domain source : domains) {
			LinkedHashSet<DomainPK> vicinity = getVicinity(source.getId());
			List<DomainPK> increment = new ArrayList<DomainPK>();
			for (DomainPK target : vicinity) {
				LinkedHashSet<DomainPK> direct = getDirectVicinity(target);
				for (DomainPK next : direct) {
					if (!next.equals(source.getId()) && !target.equals(source.getId())) {// && !vicinity.contains(next)) {
						// check if is a new reachable domain
						if (!vicinity.contains(next)) {
							increment.add(next);
							stable = false;
						}
						List<Path> source_paths = getPaths(source.getId(),target);
						List<Path> target_paths = getPaths(target,next);
						List<Path> new_paths = getPaths(source.getId(), next);
						for (Path source_path : source_paths) {
							for (Path target_path : target_paths) {
								if (target_path.size()==1 && !source_path.contains(next)) {
									Path new_path = new CompositePath(source_path, target_path);
									if (new_path.getType()!=Path.Type.INFINITE && !new_paths.contains(new_path)) {
										if (new_paths.isEmpty() || new_path.getType().equals(new_paths.get(0).getType())) {
											new_paths.add(new_path);
											stable = false;
										} else {
											// ok, the computed path is not compatible with another path... that's a problem
								//			logger.info("possible inconsistency between paths "+new_paths.get(0)+" and "+new_path+". Keeping the first.");
										}
									}
								}
							}
						}
					}
				}
			}
			vicinity.addAll(increment);
		}
		//
		return !stable;
	}

	private List<Space> getSubspaces(Universe universe, Domain source) {
		List<Space> result = new ArrayList<Space>();
		for (DomainPK target : getDirectVicinity(source.getId())) {
			List<Path> paths = getPaths(source.getId(),target);
			for (Path path : paths) {
				try {
					result.add(path.apply(universe.S(source)));
				} catch (Exception e) {
					// catch also InvalidCredentialsAPIException
					//logger.error("cannot compute the path for "+path.toString()+"+"+source.toString());
				}
			}
		}
		return result;
	}

	public void dump(Universe universe) {
		try {
			for (Domain source : universe.getDomains()) {
				LinkedHashSet<DomainPK> vicinity = getVicinity(source.getId());
				for (DomainPK target : vicinity) {
					List<Path> paths = getPaths(source.getId(),universe.getDomain(target).getId());
					for (Path path : paths) {
						Space space = path.apply(universe.S(source));
						System.out.println(space.toString()+" : "+path.getType());
					}
				}
			}
		} catch (Exception e) {
			//
		}
	}

}
