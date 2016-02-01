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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.Domain;

/**
 * The Dashboard class implements a dashboard object that represents several KPI
 * and supports dimensional operations like pivoting and filtering
 * @author sfantino
 *
 */
public class Dashboard {
	
	private Universe universe;
	private Space domain;// now it will be use to support ticket:2905
	
	private DashboardSelection selection = new DashboardSelection();
	
	//private List<KPI> kpis = new LinkedList<KPI>();
    private List<Measure> measures = new ArrayList<Measure>();
	private List<MeasureGroup> groups = new ArrayList<MeasureGroup>();

	public Dashboard(Universe universe) {
		this.universe = universe;
	}
	
	public Universe getUniverse() {
		return universe;
	}

	public Space getMainDomain() {
		return domain;
	}
	
	public void setMainDomain(Space domain) {
		this.domain = domain;
	}

	public DashboardSelection getSelection() {
		return selection;
	}
	
	/**
	 * set the dashboard selection by using an existing selection;
	 * this dashboard will actually share the new selection.
	 * @param selection
	 * @return
	 */
	public void setSelection(DashboardSelection selection) {
	    this.selection = selection;
	}

	public boolean add(Measure measure) throws ScopeException {
		// check that they are compatible with the DS
		if (!measure.getParent().getUniverse().equals(this.universe)) {
			throw new ScopeException("the KPI does not belongs to the Dashboard Universe");
		}
		// ok
		measures.add(measure);
        //
        // merge within a group
        MeasureGroup merge = null;
        for (MeasureGroup group : groups) {
            if (group.merge(measure)) {
                merge = group;
                break;
            }
        }
        if (merge==null) {
            groups.add(new MeasureGroup(measure));
        }
		//
		return true;
	}

	public void add(Collection<Measure> measures) throws ScopeException {
		for (Measure measure : measures) {
			add(measure);
		}
	}
	
	public List<Measure> getKpis() {
		return measures;
	}
	
	public List<MeasureGroup> getGroups() {
		return groups;
	}

	/**
	 * compute the list of eligible domains for adding KPI
	 * @return
	 * @throws ScopeException 
	 * @throws ComputingException 
	 */
	public Collection<Domain> getKPIDomains() throws ScopeException, ComputingException {
		if (groups.isEmpty()) {
			// return all the domains
			return universe.getDomains();
		} else {
			// compute the list of domain on already defined KPIs
			ArrayList<Domain> roots = new ArrayList<Domain>();
			// for each KPI, compute the set of available domains
			Set<Domain> intersection = null;
			for (MeasureGroup group : groups) {
				roots.add(group.getMaster().getParent().getRoot());
				Set<Domain> domains = computeDomains(group.getMaster());
				if (intersection==null) {
					intersection = domains; 
				} else {
					intersection.retainAll(domains);
				}
			}
			//
			if (intersection==null) {
				return roots;
			} else {
				intersection.removeAll(roots);
				roots.addAll(intersection);
				return roots;
			}
		}
	}

	/**
	 * compute the list of all available domains for search & pivot
	 * @return
	 * @throws ScopeException 
	 * @throws ComputingException 
	 */
	public Collection<Domain> getAllDomains() throws ScopeException, ComputingException {
		// for each KPI, compute the set of available domains
		Set<Domain> intersection = null;
		for (MeasureGroup group : groups) {
			Set<Domain> domains = computeDomains(group.getMaster());
			if (intersection==null) {
				intersection = domains; 
			} else {
				intersection.retainAll(domains);
			}
		}
		return intersection;
	}

	private Set<Domain> computeDomains(Measure measure) throws ScopeException, ComputingException {
		HashSet<Domain> domains = new HashSet<Domain>();
		Domain root = measure.getParent().getRoot();
		domains.add(root);
		ArrayList<Space> stack = new ArrayList<Space>(universe.S(root).S());
		while (!stack.isEmpty()) {
			ArrayList<Space> queue = new ArrayList<Space>(); 
			for (Space space : stack) {
				Domain domain = space.getDomain();
				if (!domains.contains(domain)) {
					domains.add(domain);
					queue.addAll(space.S());
				}
			}
			stack = queue;
		}
		return domains;
	}
	
	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("Select");
		if (!measures.isEmpty()) {
			for (Measure measure : measures) {
				buffer.append(" '").append(measure.toString()).append("'");
			}
		} else if (domain!=null) {
			buffer.append(" ").append(domain.toString());
		}
		if (!selection.isEmpty()) {
			buffer.append("\nWhere ");
			buffer.append(selection.toString());
		}
		return buffer.toString();
	}

}
