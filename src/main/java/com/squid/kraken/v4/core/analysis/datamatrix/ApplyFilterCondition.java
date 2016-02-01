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

import java.util.HashSet;

import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;

/**
 * Wrap a filter condition and apply it to a row
 * @author sergefantino
 *
 */
public class ApplyFilterCondition {
	
	public int index;
	public HashSet<Object> items = new HashSet<Object>();
	private boolean nullIsValid;
	
	public ApplyFilterCondition(int index, boolean nullIsValid) {
		this.index = index;
		this.nullIsValid = nullIsValid;
	}
	
	public boolean filter(IndirectionRow row) {
		//DimensionMember m = DataMatrix.this.getDimensionMember(row, index);//row.getAxisValue(DataMatrix.this,index);
		Object m = row.getAxisValue(index);
		return (m==null && this.nullIsValid) || (m!=null && items.contains(row.getAxisValue(index)));
	}

	public void add(DimensionMember filter) {
		items.add(filter.getID());
	}
	
	public boolean isEmpty() {
		return items.isEmpty();
	}
	
}