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

import java.util.ArrayList;
import java.util.Date;

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.universe.Axis;

/**
 * specialized Merger that can use a Join axis
 * @author sergefantino
 *
 */
public class JoinMerger extends Merger {
	
	private Axis join = null;
	private boolean joinIsAColumn = false;
	private int joinIndex = -1;
	private AxisValues joinRight = null;
	
	/**
	 * The JoinMerger will merge 2 DataMatrices to compare the output.
	 * If the join Axis is defined and exits in the input DM, it will be use to correctly join them.
	 * @param join
	 * @throws ScopeException 
	 */
	public JoinMerger(DataMatrix left, DataMatrix right, Axis join) throws ScopeException {
		super(left, right);
		this.join = join;
		//
		// check if join is a column
		if (join != null) {
			int i = 0;
			for (AxisValues av : left.getAxes()) {
				if (av.getAxis().equals(join)) {
					joinIsAColumn = true;
					joinIndex = i;
					break;
				}
				i++;
			}
			if (joinIsAColumn) {
				joinRight = right.getAxes().get(joinIndex);
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
	protected IndirectionRow createDefaultIndirectionRow() {
		if (joinIsAColumn) {
			// add the join copy for comparison
			return createDefaultIndirectionRow(left.getAxes().size()+1,left.getDataSize()+right.getDataSize());
		} else {
			return createDefaultIndirectionRow(left.getAxes().size(),left.getDataSize()+right.getDataSize());
		}
	}
	
	@Override
	protected IndirectionRow merge(IndirectionRow left, IndirectionRow right, IndirectionRow schema) {
		IndirectionRow merged = new IndirectionRow();
		int nbColumns = schema.getAxesCount()+schema.getDataCount();
		merged.axesIndirection = schema.getAxesIndirection();
		merged.dataIndirection = schema.getDataIndirection();
		merged.rawrow = new Object[nbColumns];

		// we have to reorder
		if (left == null && right == null)
			return merged;
		else if (left == null) {
			// copy axes
			int rrInd = 0;
			for (int i = 0; i < right.getAxesCount(); i++) {
				if (joinIsAColumn && i==joinIndex) {
					merged.rawrow[rrInd] = translateRightToLeft(right.getAxisValue(i));
					rrInd++;
					merged.rawrow[rrInd] = right.getAxisValue(i);
					rrInd++;
				} else {
					merged.rawrow[rrInd] = right.getAxisValue(i);
					rrInd++;
				}
			}
			// go directly to right part
			rrInd = nbColumns - right.getDataCount();
			for (int i = 0; i < right.getDataCount(); i++) {
				merged.rawrow[rrInd] = right.getDataValue(i);
				rrInd++;
			}
		} else {
			// copy axes
			int rrInd = 0;
			for (int i = 0; i < left.getAxesCount(); i++) {
				if (joinIsAColumn && i==joinIndex) {
					merged.rawrow[rrInd] = left.getAxisValue(i);
					rrInd++;
					if (right!=null) {
						merged.rawrow[rrInd] = right.getAxisValue(i);
					} else {
						merged.rawrow[rrInd] = translateLeftToRight(left.getAxisValue(i));
					}
					rrInd++;
				} else {
					merged.rawrow[rrInd] = left.getAxisValue(i);
					rrInd++;
				}
			}
			// copy left part
			for (int i = 0; i < left.getDataCount(); i++) {
				merged.rawrow[rrInd] = left.getDataValue(i);
				rrInd++;
			}
			if (right != null) {
				// copy right part
				for (int i = 0; i < right.getDataCount(); i++) {
					merged.rawrow[rrInd] = right.getDataValue(i);
					rrInd++;
				}
			}
		}
		return merged;
	}
	
	@Override
	protected DataMatrix createMatrix(ArrayList<IndirectionRow> result) {
		DataMatrix merge = new DataMatrix(left.getDatabase(), result);
		merge.setFromCache(left.isFromCache() && right.isFromCache());
		merge.setExecutionDate(new Date(Math.max(left.getExecutionDate().getTime(), right.getExecutionDate().getTime())));
		// list the new axis
		for (AxisValues ax : left.getAxes()) {
			merge.add(ax);
			// add the compare
			if (joinIsAColumn && ax.getAxis().equals(join)) {
				merge.add(joinRight);
			}
		}
		// list the new kpis
		merge.getKPIs().addAll(left.getKPIs());
		merge.getKPIs().addAll(right.getKPIs());
		merge.setFullset(left.isFullset() && right.isFullset());
		return merge;
	}

}
