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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.squid.core.domain.IDomain;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.model.domain.DomainDomain;
import com.squid.kraken.v4.model.Domain;

/**
 * define a selection for a given Domain. A selection can use:
 * - open expression that must be defined on the same domain
 * - list of DimensionMembers
 * 
 * @author sergefantino
 *
 */
public class DomainSelection {
    
    private Domain root = null;
    
    private List<ExpressionInput> conditions = null;
    private HashMap<Axis, List<DimensionMember>> selections = null;
    
    public DomainSelection(Domain root) {
        this.root = root;
    }
    
    public DomainSelection(DomainSelection copy) {
        this(copy.root);
        if (copy.hasConditions()) {
        	conditions = new ArrayList<>(copy.conditions);
        }
        for (Axis filter : copy.getFilters()) {
        	try {
				add(filter,copy.getMembers(filter));
			} catch (ScopeException e) {
				// ok to ignore
			}
        }
    }
    
    public Domain getDomain() {
    	return root;
    }

	public void clear(Axis axis) {
		if (selections!=null) {
			selections.remove(axis);
		}
	}
    
    public Collection<ExpressionInput> getConditions() {
        if (conditions==null) {
            return Collections.emptyList();
        } else {
            return conditions;
        }
    }
    
    public Collection<Axis> getFilters() {
        if (selections==null) {
            return Collections.emptyList();
        } else {
            return selections.keySet();
        }
    }
    
    public Collection<DimensionMember> getMembers(Axis axis) {
        if (selections==null) {
            return Collections.emptyList();
        } else {
            List<DimensionMember> members = selections.get(axis);
            if (members==null) {
                return Collections.emptyList();
            } else {
                return members;
            }
        }
    }
    
    public boolean isEmpty() {
        return conditions==null && selections==null;
    }
    
    public void add(ExpressionInput condition) throws ScopeException {
        IDomain source = condition.getExpression().getSourceDomain();
        if (source instanceof DomainDomain) {
            Domain domain = (Domain)source.getAdapter(Domain.class);
            if (domain==null || !this.root.equals(domain)) {
                throw new ScopeException("cannot apply this condition on " + this.root.toString());
            }
        }
        //
        if (conditions==null) {
            conditions = new ArrayList<>();
        }
        conditions.add(condition);
    }

    public void add(Axis axis, DimensionMember member) throws ScopeException {
        if (!axis.getParent().getRoot().equals(this.root)) {
            throw new ScopeException("cannot create a filter for " + axis.toString() + " on " + this.root.toString());
        }
        //
        if (selections==null) {
            selections = new HashMap<>();
        }
        List<DimensionMember> members = selections.get(axis);
        if (members==null) {
            members = new ArrayList<>();
            selections.put(axis,  members);
        }
        members.add(member);
    }

    public void add(Axis axis, Collection<DimensionMember> filters) throws ScopeException {
        if (!axis.getParent().getRoot().equals(this.root)) {
            throw new ScopeException("cannot create a filter for " + axis.toString() + " on " + this.root.toString());
        }
        //
        if (selections==null) {
            selections = new HashMap<>();
        }
        List<DimensionMember> members = selections.get(axis);
        if (members==null) {
            members = new ArrayList<>();
            selections.put(axis,  members);
        }
        members.addAll(filters);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DomainSelection [root=" + root.getName());
        if (conditions!=null) builder.append(", conditions=" + conditions);
        if (selections!=null) builder.append(", selections=" + selections);
        builder.append("]");
        return builder.toString();
    }

    public boolean hasConditions() {
        return conditions!=null;
    }
    

}
