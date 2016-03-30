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
package com.squid.kraken.v4.core.analysis.model;

import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;

import com.squid.core.domain.IDomain;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.model.Domain;

/**
 * manage the dashboard selection
 * 
 * @author sfantino
 *
 */
public class DashboardSelection {

	private Hashtable<Domain, DomainSelection> selections = new Hashtable<Domain, DomainSelection>();

	// T947: support for compareTo feature
	private DomainSelection compareToSelection = null;
	
	public DashboardSelection() {
	}
	
	/**
	 * make a swallow copy
	 * @param shallowCopy
	 */
	public DashboardSelection(DashboardSelection shallowCopy) {
		for (DomainSelection copy : shallowCopy.get()) {
			DomainSelection selection = new DomainSelection(copy);
			selections.put(selection.getDomain(), selection);
		}
	}

	/**
	 * check if the selection is empty
	 * @return
	 */
	public boolean isEmpty() {
		if (selections.isEmpty()) {
			return true;
		} else {
			for (DomainSelection space : selections.values()) {
				if (!space.isEmpty()) {
					return false;
				}
			}
		}
		// else
		return true;
	}
	
	public Collection<DomainSelection> get() {
		return selections.values();
	}
	
	protected DomainSelection getDomainSelection(Axis axis) {
	    Domain domain = axis.getParent().getRoot();
	    return getDomainSelection(domain);
	}
	
	protected DomainSelection getDomainSelection(Domain domain) {
	    DomainSelection selection = selections.get(domain);
		if (selection==null) {
			selection = new DomainSelection(domain);
			selections.put(domain, selection);
		}
		return selection;
	}
	
	/**
	 * return the Filter associated to the Axis or null if there is no filter set
	 * @param axis
	 * @return
	 */
	public Collection<DimensionMember> getMembers(Axis axis) {
	    DomainSelection selection = getDomainSelection(axis);
		if (selection!=null) {
		    return selection.getMembers(axis);
		} else {
			return Collections.emptyList();
		}
	}
	
	public Collection<ExpressionInput> getConditions(Domain domain) {
	    DomainSelection selection = getDomainSelection(domain);
		if (selection!=null) {
		    return selection.getConditions();
		} else {
			return Collections.emptyList();
		}
	}
	
	public void clear(Axis axis) {
		DomainSelection sel = getDomainSelection(axis);
		if (sel!=null) {
			sel.clear(axis);
		}
	}
	
	/**
	 * add a filter defined by an open expression
	 * KRKN-47
	 * @param space
	 * @param filter
	 * @throws ScopeException 
	 */
	public void add(ExpressionInput condition) throws ScopeException {
	    IDomain source = condition.getExpression().getSourceDomain();
	    Object adapter = source.getAdapter(Space.class);
	    if (adapter!=null && adapter instanceof Space) {
	        Space space = (Space)adapter;
	        DomainSelection sel = getDomainSelection(space.getRoot());
            if (sel!=null) {
                sel.add(condition);
            }
	    } else {
	        // try the domain
	        adapter = source.getAdapter(Domain.class);
	        if (adapter!=null && adapter instanceof Domain) {
	            Domain domain = (Domain)adapter;
	            DomainSelection sel = getDomainSelection(domain);
	            if (sel!=null) {
	                sel.add(condition);
	            }
	        }
	    }
	}

	public void add(Axis axis, DimensionMember member) throws ScopeException {
		if (member!=null) {
		    DomainSelection sel = getDomainSelection(axis);
			if (sel!=null) {
			    sel.add(axis, member);
			}
		}
	}

    public void add(Axis axis, Collection<DimensionMember> members) throws ScopeException {
        if (members!=null) {
            DomainSelection sel = getDomainSelection(axis);
            if (sel!=null) {
                sel.add(axis, members);
            }
        }
    }

	public void add(Axis axis, Intervalle intervalle) throws ScopeException {
		if (intervalle!=null) {
		    DomainSelection sel = getDomainSelection(axis);
			if (sel!=null) {
				DimensionMember member = axis.getMemberByID(intervalle);
				sel.add(axis, member);
			}
		}
	}

	public void addCompareTo(Axis axis, DimensionMember member) throws ScopeException {
		Domain domain = axis.getParent().getRoot();
		if (compareToSelection==null) {
			compareToSelection = new DomainSelection(domain);
			compareToSelection.add(axis, member);
		} else {
			if (!compareToSelection.getDomain().equals(domain)) {
				throw new ScopeException("invalid compare, already defined on domain '"+domain.getName()+"'");
			}
			if (compareToSelection.getMembers(axis).isEmpty()) {
				throw new ScopeException("invalid compare, only one axis is supported");
			}
			compareToSelection.add(axis, member);
		}
	}

	public void addCompareTo(Axis axis, Intervalle intervalle) throws ScopeException {
		if (intervalle!=null) {
            DimensionMember member = axis.getMemberByID(intervalle);
            addCompareTo(axis, member);
		}
	}
	
	public boolean hasCompareToSelection() {
		return compareToSelection!=null;
	}
	
	public DomainSelection getCompareToSelection() {
		return compareToSelection;
	}

	@Deprecated
	// don't use that method
	public boolean add(Axis axis, Object something) throws ScopeException {
		if (something!=null && axis.getDimension().getType()==Type.CATEGORICAL) {
		    try {
    			Collection<DimensionMember> members = axis.find(something);
    			if (members.isEmpty()) {
    				return false;
    			} else {
    				for (DimensionMember member : members) {
    					add(axis,member);
    				}
    				return true;
    			}
		    } catch (InterruptedException e) {
		        //ignore
		    }
		}
		//
		return false;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this==obj) {
			return true;
		} else if (obj instanceof DashboardSelection) {
			DashboardSelection that = (DashboardSelection)obj;
			return this.selections.equals(that.selections);
		}
		// else
		return false;
	}
	
	@Override
	public int hashCode() {
		return this.selections.hashCode();
	}
	
	@Override
	public String toString() {
		StringBuffer result = new StringBuffer();
		boolean first = true;
		for (DomainSelection sel : selections.values()) {
		    if (first) {
		        first=false;
		    } else {
		        result.append("\nand ");
		    }
			result.append(sel.toString()).append("\n");
		}
		return result.toString();
	}
	
}