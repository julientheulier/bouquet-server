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

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.caching.redis.datastruct.RawRow;
import com.squid.kraken.v4.core.analysis.universe.Axis;

/**
 * specialized Merger that can use a Join axis
 * @author sergefantino
 *
 */
public class JoinMerger extends Merger {
	
	private Axis join = null;
	private int joinIndex = -1;
	private AxisValues joinRight = null;
	private boolean hasJoinColumn;
	
	/**
	 * The JoinMerger will merge 2 DataMatrices to compare the output.
	 * If the join Axis is defined and exits in the input DM, it will be use to correctly join them.
	 * @param mergeOrder 
	 * @param join
	 * @throws ScopeException 
	 */
	public JoinMerger(DataMatrix left, DataMatrix right, int[] mergeOrder, Axis join) throws ScopeException {
		super(left, right, mergeOrder);
		this.join = join;
		//
		// check if join is a column
		if (join != null) {
			int i = 0;
			for (AxisValues av : left.getAxes()) {
				if (av.getAxis().equals(join)) {
					hasJoinColumn = true;
					joinIndex = i;
					joinRight = new AxisValues(right.getAxes().get(joinIndex));
					joinRight.setVisible(false);
					break;
				}
				i++;
			}
		}
	}
	
	@Override
	protected int compareValue(int pos, Object leftValue, Object rightValue) {
		if (pos==joinIndex) {
			return compareJoinValue(pos, leftValue, rightValue);
		} else {
			return super.compareValue(pos, leftValue, rightValue);
		}
	}
	
	protected int compareJoinValue(int pos, Object leftValue, Object rightValue) {
		return super.compareValue(pos, leftValue, rightValue);
	}
	
	/**
	 * compute the present value based on the past value. Default is to return null.
	 * @param right (== past) value
	 * @return
	 */
	protected Object translateRightToLeft(Object right) {
		return null;
	}
	
	/**
	 * compute the past value based on the present value. Default is to return null.
	 * @param left (== present) value
	 * @return
	 */
	protected Object translateLeftToRight(Object left) {
		return null;
	}
	
	@Override
	protected void mergeAxes(DataMatrix merge, RawRow leftrow, RawRow rightrow, RawRow merged) {
		if (leftrow == null && rightrow == null) {
			// ignore, should not happen
		} if (leftrow == null) {
			// copy axes
			int rrInd = 0;
			// left & right matrices having the same size
			for (int i = 0; i < left.getAxesSize(); i++) {
				if (hasJoinColumn && i==joinIndex) {
					merged.setData(rrInd, translateRightToLeft(right.getAxisValue(i, rightrow)));
					rrInd++;
					if (joinRight!=null) {// if the join (compare) column is present
						merged.setData(rrInd, right.getAxisValue(i, rightrow));
						rrInd++;
					}
				} else {
					merged.setData(rrInd, right.getAxisValue(i, rightrow));
					rrInd++;
				}
			}
		} else {
			// copy axes
			int rrInd = 0;
			for (int i = 0; i < left.getAxesSize(); i++) {
				merged.setData(rrInd, left.getAxisValue(i, leftrow));
				rrInd++;
				if (joinRight!=null && i==joinIndex) {
					if (rightrow!=null) {
						merged.setData(rrInd, right.getAxisValue(i, rightrow));
					} else {
						merged.setData(rrInd, translateLeftToRight(left.getAxisValue(i, leftrow)));
					}
					rrInd++;
				}
			}
		}
	}
	
	/**
	 * take care of the join column => 2x for present/past values
	 */
	@Override
	protected void createMatrixAxes(DataMatrix merge) {
		// list the new axis
		for (AxisValues ax : left.getAxes()) {
			merge.add(ax);
			// add the compare
			if (joinRight!=null && ax.getAxis().equals(join)) {
				merge.add(joinRight);
			}
		}
	}

}
