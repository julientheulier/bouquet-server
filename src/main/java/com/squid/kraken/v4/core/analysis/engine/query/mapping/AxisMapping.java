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
package com.squid.kraken.v4.core.analysis.engine.query.mapping;

import com.squid.core.sql.render.ISelectPiece;
import com.squid.core.sql.render.IOrderByPiece.ORDERING;
import com.squid.kraken.v4.core.analysis.datamatrix.AxisValues;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.universe.Axis;

public class AxisMapping extends SimpleMapping {

	private Axis axis;
	private DimensionIndex index;
	private AxisValues data;

	public AxisMapping(ISelectPiece piece, Axis axis) {
		super(piece);
		this.axis = axis;
		this.data = new AxisValues(axis);
	}

	public Axis getAxis() {
		return axis;
	}
	
	public DimensionIndex getDimensionIndex() throws ComputingException, InterruptedException {
		// the operation may be time-consuming while iterating over a large resultset...
		if (index==null) {
			index = axis.getIndex();
		}
		return index;
	}

	public AxisValues getData() {
		return data;
	}
	
	@Override
	public void setOrdering(ORDERING ordering) {
		// store into axisValue
		this.data.setOrdering(ordering);
	}

}