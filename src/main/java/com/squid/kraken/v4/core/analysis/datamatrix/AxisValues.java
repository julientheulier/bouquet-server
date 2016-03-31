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
package com.squid.kraken.v4.core.analysis.datamatrix;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentSkipListSet;

import com.squid.core.sql.render.IOrderByPiece.ORDERING;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.universe.Axis;

/**
 * A list of values (members) for an Axis in the DataMatrix
 * @author sfantino
 *
 */
public class AxisValues {
	
	private Axis axis;
	private ConcurrentSkipListSet<Object> values = new ConcurrentSkipListSet<Object>();
	private boolean isVisible = true;
	private ORDERING ordering;

	public AxisValues(AxisValues copy) {
		this.axis = copy.axis;
		this.isVisible = copy.isVisible;
		this.ordering = copy.ordering;
		// don't set values
	}
	
	public AxisValues(Axis axis) {
		super();
		this.axis = axis;
	}

	public Axis getAxis() {
		return axis;
	}

	public Collection<Object> getValues() {
		return values;
	}
	
	public boolean isVisible() {
		return isVisible;
	}

	public void setOrdering(ORDERING ordering) {
		this.ordering = ordering;
	}
	
	public ORDERING getOrdering() {
		return ordering;
	}

	public void setVisible(boolean isVisible) {
		this.isVisible = isVisible;
	}

	@Override
	public String toString() {
		return "AxisData:{axis="+axis.toString()+" =>["+values.size()+"]"+values.toString()+"}";
	}

	public Collection<DimensionMember> getMembers() throws ComputingException, InterruptedException {
		LinkedList<DimensionMember> members = new LinkedList<DimensionMember>();
		DimensionIndex dimIndex = axis.getIndex();
		for (Object value : values) {
			DimensionMember member = dimIndex.getMemberByID(value);
			if (member!=null) {
				members.add(member);
			}
		}
		return members;
	}
	
}