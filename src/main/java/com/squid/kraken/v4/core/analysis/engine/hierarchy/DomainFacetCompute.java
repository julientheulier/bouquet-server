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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.squid.core.domain.IDomain;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex.Status;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.DimensionOption;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Facet;

/**
 * handle facet computation
 * @author sergefantino
 *
 */
public class DomainFacetCompute extends FacetBuilder {
    
    // default values
    private int offset = 0;
    private int size = 100;
    
	private Universe universe;// this is the link to the user ctx
    
    public DomainFacetCompute(Universe universe) {
    	this.universe = universe;
    }
    
    public DomainFacetCompute(int offset, int size) {
        super();
        this.offset = offset;
        this.size = size;
    }
    
    /**
     * compute the facets for a given dimension
     * @param universe
     * @param domain
     * @param sel
     * @param axis
     * @param filter
     * @param offset
     * @param size
     * @param timeoutMs - if null the call is blocking; if timeoutMs>0 then it will wait for the facet to be complete with timeout; if timeout==0 it won't block
     * @return
     * @throws ComputingException
     * @throws InterruptedException
     * @throws TimeoutException 
     * @throws ExecutionException 
     */
    public Facet computeDimensionFacets(
            Domain domain, 
            DashboardSelection sel,
            Axis axis,
            String filter,
            int offset,
            int size, Integer timeoutMs) throws ComputingException, InterruptedException, ExecutionException, TimeoutException {
        //
        DomainHierarchy hierarchy = universe.getDomainHierarchy(domain);
        DimensionIndex index = hierarchy.getDimensionIndex(axis);
        //
        if (index.getStatus()==Status.STALE) {
            // wait for timeout - if null wait until complete
            hierarchy.isDone(index,timeoutMs);// it should either return true or fail
        }
        //
        return computeDimensionFacets(domain, index, sel, filter, offset, size);
    }
    
	private static final Comparator<Facet> facetsComparator = new Comparator<Facet>() {
		@Override
		public int compare(Facet o1, Facet o2) {
			boolean d1 = o1.getDimension().isDynamic();
			boolean d2 = o2.getDimension().isDynamic();
			if (d1==d2) {
				boolean p1 = o1.isProxy();
				boolean p2 = o2.isProxy();
				if (p1==p2) {
					return o1.getName().compareTo(o2.getName());
				} else if (p1) {
					return 1;
				} else {// p2
					return -1;
				}
			} else if (d1) {
				return 1;
			} else {// d2
				return -1;
			}
		}
	};

    /**
     * compute the facets for a given domain
     * @param universe
     * @param domain
     * @param sel
     * @return
     * @throws ScopeException
     * @throws InterruptedException
     * @throws ComputingException
     */
    public Collection<Facet> computeDomainFacets(
            Domain domain,
            DashboardSelection sel) throws ScopeException, InterruptedException, ComputingException {
        DomainHierarchy hierarchy = universe.getDomainHierarchy(domain);
        List<Facet> facets = new ArrayList<>();
        for (DimensionIndex index : hierarchy.getDimensionIndexes()) {
        	IDomain image = index.getAxis().getDefinitionSafe().getImageDomain();
            if (
            	!hierarchy.isSegment(index) // exclude segments
            	&&  
            	checkHasRole(universe,index.getDimension()) // check user role
            	&&
            	!image.isInstanceOf(IDomain.OBJECT) // hide the objects for now
            ) 
            {
            	// T70
            	if (index.isVisible())
            	{
            		facets.add(computeDimensionFacets(domain, index, sel, null, offset, size));
            	}
            }
        }
        Collections.sort(facets,facetsComparator);
    	// handle segments
        Facet goalFacet = SegmentManager.createSegmentFacet(universe, hierarchy, domain, sel);
        if (goalFacet!=null && !goalFacet.getItems().isEmpty()) facets.add(goalFacet);
        return facets;
    }
    
    private boolean checkHasRole(Universe universe, Dimension dimension) {
    	// T15 - hide dynamic dimension if not super/admin user
    	if (dimension.isDynamic()) {
    		return universe.hasRole(dimension, Role.WRITE);
    	} else {
    		return universe.hasRole(dimension, Role.READ);
    	}
    }
    
    private Facet computeDimensionFacets(Domain domain, DimensionIndex index,
            DashboardSelection sel,
            String filter, int offset, int size) {
        //
    	try {
    		Status status = index.getStatus();// copy the status now to avoid race-conditions?
    		List<DimensionMember> values = populateDimensionFacets(index, sel, filter, offset, size+1);
	        boolean hasMore = values.size()>size;
	        if (hasMore) {
	            values = values.subList(0, size);
	        }
	        Facet facet = buildFacet(domain, index, values, sel);
	        if (hasMore) {
	            facet.setHasMore(true);
	        }
	        if (status==Status.STALE) {
	            facet.setDone(false);
	        }
	        if (status==Status.ERROR) {
	            facet.setError(true);
	            facet.setErrorMessage(index.getErrorMessage());
	        }
	        return facet;
    	} catch (Exception e) {
    		// if the ES cluster is not available, the error is not catch by the index (the state may resolve by itself)
    		// so it is not updating the index state
    		// and we need to catch it by ourselves
    		Facet error = buildFacet(domain, index, Collections.<DimensionMember>emptyList(), sel);
    		error.setError(true);
            error.setErrorMessage(e.getLocalizedMessage());
    		return error;
    	}
    }
    
    public List<DimensionMember> populateDimensionFacets(
            DimensionIndex index, 
            DashboardSelection sel,
            String filter,
            int offset,
            int size
            ) {
    	// krkn-61: check for hidden flag
    	if (index.getFullOptions()!=null && index.getFullOptions().isHidden()) {
    		// hidden flag may be set, check with context
    		DimensionOption option = DimensionOptionUtils.computeContextOption(index.getDimension(), universe.getContext());
    		if (option!=null && option.isHidden()) {
    			Collection<DimensionMember> members = sel.getMembers(index.getAxis());
    			if (members.isEmpty()) {
    				return Collections.emptyList();
    			} else {
    				return new ArrayList<DimensionMember>(members);
    			}
    		}
    	}
        // create a list of all the selected members in the facet parent hierarchy
        List<DimensionIndex> parents = index.getParents();
        Map<DimensionIndex, List<DimensionMember>> selections = new HashMap<DimensionIndex, List<DimensionMember>>();
        for (DimensionIndex parent : parents) {
            Collection<DimensionMember> f = sel.getMembers(parent.getAxis());
            if (f!=null && !f.isEmpty()) {
                selections.put(parent, new ArrayList<>(f));
            }
        }
        if (selections.isEmpty()) {
            if (index.getDimension().getType()==com.squid.kraken.v4.model.Dimension.Type.CONTINUOUS) {
                List<DimensionMember> all = index.getMembers();
                return all;
            } else {
                if (filter==null || filter=="" ) { // T61: turn off for now
                    return index.getMembers(offset,size);
                } else {
                    return index.getMembers(filter, offset, size);
                } 
            }
        } else {
            // filter by the selection
            if (filter==null || filter=="") { // T61: turn off for now
                return index.getMembersFilterByParents(selections, offset, size);
            } else {
                return index.getMembersFilterByParents(selections, filter, offset, size);
            } 
        }
    }

}
