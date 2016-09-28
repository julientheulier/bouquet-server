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

import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.model.ProjectAnalysisJob.Position;

public class GroupByAxis {
	
	private Axis axis;
	private boolean rollup = false;
    private Position rollupPosition;// if rollup, we can specify where we want the sub-total to appear
	
	public GroupByAxis(Axis axis) {
		super();
		this.axis = axis;
	}

    public GroupByAxis(Axis axis, boolean rollup) {
        super();
        this.axis = axis;
        this.rollup = rollup;
    }

	public Axis getAxis() {
		return axis;
	}

	public boolean isRollup() {
		return rollup;
	}
	
	public void setRollup(boolean rollup) {
        this.rollup = rollup;
    }
	
	public Position getRollupPosition() {
		return rollupPosition;
	}
	
	public void setRollupPosition(Position rollupPosition) {
		this.rollupPosition = rollupPosition;
	}
	
	@Override
	public String toString() {
		return axis.toString()+(rollup?"!":"");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((axis == null) ? 0 : axis.hashCode());
		result = prime * result + (rollup ? 1231 : 1237);
		result = prime * result + ((rollupPosition == null) ? 0 : rollupPosition.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GroupByAxis other = (GroupByAxis) obj;
		if (axis == null) {
			if (other.axis != null)
				return false;
		} else if (!axis.equals(other.axis))
			return false;
		if (rollup != other.rollup)
			return false;
		if (rollupPosition != other.rollupPosition)
			return false;
		return true;
	}
	
	
	
	
}