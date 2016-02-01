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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.DimensionPK;

/**
 * support for validating the parent-child relationship
 * @author sergefantino
 *
 */
public class DomainHierarchyParenthood {


	public static class DimensionExtended {
		
		private Dimension dimension;
		private Dimension parent;
		private List<Dimension> children;
		
		public DimensionExtended(Dimension dimension) {
			super();
			this.dimension = dimension;
		}
		
		public void remove(Dimension child) {
			children.remove(child);
		}

		public Dimension getDimension() {
			return dimension;
		}
		
		public Dimension getParent() {
			return parent;
		}
		
		public void setParent(Dimension parent) {
			this.parent = parent;
		}
		
		public List<Dimension> getChildren() {
			return children!=null?children:Collections.<Dimension>emptyList();
		}

		public void add(Dimension child) {
			if (children==null) {
				children = new ArrayList<Dimension>();
			}
			children.add(child);
		}
		
	}
	
	private HashMap<DimensionPK, DimensionExtended> extended = new HashMap<DimensionPK, DimensionExtended>();

	
	public DomainHierarchyParenthood() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * add the given list of dimension and sanitize the parenthood relationship
	 * @param dims
	 */
	public void add(List<Dimension> dims) {
		HashMap<DimensionPK, Dimension> lookup = new HashMap<DimensionPK, Dimension>();
		for (Dimension dim : dims) {
			lookup.put(dim.getId(), dim);
		}
		// init parenting
		for (Dimension dim : dims) {
			if (dim.getParentId()!=null) {
				Dimension parent = lookup.get(dim.getParentId());
				if (parent==null) {
					dim.setParentId(null);// clear parent
				} else {
					setParent(dim,parent);
				}
			}
		}
		// check cycles
		for (Dimension dim : dims) {
			if (dim.getParentId()!=null) {
				checkCycles(dim);
			}
		}
	}

	/**
	 * return the known sanitize hierarchy for this axis
	 * @param axis
	 * @return
	 */
    public List<Axis> getHierarchy(Axis axis) {
    	Dimension dimension = axis.getDimension();
        if (dimension!=null && dimension.getId().getDimensionId()!=null) {
            ArrayList<Axis> axes = new ArrayList<Axis>();
            for (Dimension child : getExtended(dimension).getChildren()) {
                axes.add(axis.getParent().A(child));
            }
            return axes;
        } else {
            return Collections.emptyList();
        }
    }

	/**
	 * check for cycles by starting to iterate from the given dimension.
	 * If we find a cycle, it will sanitize the relationship.
	 * @param dim
	 */
	private void checkCycles(Dimension dim) {
		List<DimensionPK> check = new ArrayList<DimensionPK>();
		Dimension child = dim;
		check.add(child.getId());
		Dimension parent = getParent(child);
		while (parent!=null) {
			if (check.contains(parent.getId())) {
				sanitizeParent(child);
				break;// go hell
			} else {
				child = parent;
				check.add(child.getId());
				parent = getParent(child);
			}
		}
	}

	/**
	 * clear this child parent relationship
	 * @param child
	 */
	private void sanitizeParent(Dimension child) {
		DimensionExtended xChild = getExtended(child);
		if (xChild.getParent()!=null) {
			DimensionExtended xParent = getExtended(xChild.getParent());
			xChild.setParent(null);// clear the record
			child.setParentId(null);// clear the actual object
			xParent.remove(child);
		}
	}

	/**
	 * get this child parent or null if none
	 * @param child
	 * @return
	 */
	private Dimension getParent(Dimension child) {
		DimensionExtended xChild = getExtended(child);
		return xChild.getParent();
	}
	
	/**
	 * set this child parent relationship
	 * @param child
	 * @param parent
	 */
	private void setParent(Dimension child, Dimension parent) {
		DimensionExtended xChild = getExtended(child);
		xChild.setParent(parent);
		DimensionExtended xParent = getExtended(parent);
		xParent.add(child);
	}
    
	private DimensionExtended getExtended(Dimension dim) {
		DimensionExtended ext = extended.get(dim.getId());
		if (ext==null) {
			ext = new DimensionExtended(dim);
			extended.put(dim.getId(), ext);
		}
		return ext;
	}

}
