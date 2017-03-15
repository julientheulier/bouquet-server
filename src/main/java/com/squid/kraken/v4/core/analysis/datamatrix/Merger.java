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

import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.render.IOrderByPiece.ORDERING;
import com.squid.kraken.v4.caching.redis.datastruct.RawRow;

/**
 * Merger class provides simple method to merge 2 DataMatrix based on their axes.
 * The two matrices must share the same axes definition and be sorted the same way.
 */
public class Merger {

	protected DataMatrix left;
	protected DataMatrix right;
	
	protected ORDERING[] ordering;
	private int[] mergeOrder;
	
	public Merger(DataMatrix left, DataMatrix right) throws ScopeException {
		this.left = left;
		this.right = right;
		// check order
		if (left.getAxes().size()!=right.getAxes().size()) {
			throw new ScopeException("matrices axes do not match");
		}
		ordering = new ORDERING[left.getAxes().size()];
		for (int i=0;i<left.getAxes().size();i++) {
			AxisValues axis = left.getAxes().get(i);
			ordering[i] = axis.getOrdering();
		}
	}
	
	public Merger(DataMatrix left, DataMatrix right, int[] mergeOrder) throws ScopeException {
		this(left, right);
		this.mergeOrder = mergeOrder;
	}

	public int compare(RawRow leftrow, RawRow rightrow) {
	 	if (leftrow==rightrow) return 0;
	 	if (mergeOrder==null) {// if no order specified, just use regular
	 		for (int i=0;i<left.getAxesSize();i++) {
	 			if (right.getAxesSize()<=i) {
	 				return 1;// in case o2 is shorter...
	 			} else {
	 				int c = compareValueOrdering(i, left.getAxisValue(i, leftrow), right.getAxisValue(i, rightrow));
	 				if (c!=0) {
	 					return c;
	 				}
	 				// else continue
	 			}
	 		}
	 		// equals !
	 		return 0;
	 	} else {
	 		// compare using the mergeOrder
	 		for (int i=0;i<mergeOrder.length;i++) {
	 			int pos = mergeOrder[i];
	 			if (left.getAxesSize()<=pos) {
	 				return -1;
	 			} else if (right.getAxesSize()<=pos) {
	 				return 1;
	 			} else {
	 				int c = compareValueOrdering(pos, left.getAxisValue(pos, leftrow), right.getAxisValue(pos, rightrow));
	 				if (c!=0) {
	 					return c;
	 				}
	 			}
	 		}
	 		// equals !
	 		return 0;
	 	}
	}
	
	protected int compareValueOrdering(int pos, Object leftValue, Object rightValue) {
		int cc = compareValue(pos, leftValue, rightValue);
		if (ordering[pos]==ORDERING.DESCENT) {// T1890: make sure we are applying exactly the same rules as for the DataMatrix.orderBy
			return -cc;
		} else {
			return cc;
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" }) // there's no way to enforce the comparable.compareTo() types in a polymorphic way (since we don't know the actual type here, can be String, Number, Date...)
	protected int compareValue(int pos, Object leftValue, Object rightValue) {
		if (leftValue == null && rightValue != null)
			return -1;
		if (leftValue != null && rightValue == null)
			return 1;
		if (leftValue == null && rightValue == null)
			return 0;
		if ((leftValue instanceof Comparable) && (rightValue instanceof Comparable)) {
			return ((Comparable) leftValue).compareTo(((Comparable) rightValue));
		} else {
			return leftValue.toString().compareTo(rightValue.toString());
		}
	}

	protected RawRow merge(DataMatrix merge, RawRow leftrow, RawRow rightrow) {
		int nbColumns = merge.getRowSize();
		RawRow merged = new RawRow(nbColumns);

		// we have to reorder
		if (leftrow == null && rightrow == null)
			return merged;
		else {
			mergeAxes(merge, leftrow, rightrow, merged);
			mergeMeasures(merge, leftrow, rightrow, merged);
		}
		return merged;
	}
	
	protected void mergeAxes(DataMatrix merge, RawRow leftrow, RawRow rightrow, RawRow merged) {
		DataMatrix source = (leftrow!=null)?left:right;
		RawRow sourcerow = (leftrow!=null)?leftrow:rightrow;
		// copy axes
		int pos = 0;
		for (int i = 0; i < source.getAxesSize(); i++) {
			merged.data[pos] = source.getAxisValue(i, sourcerow);
			pos++;
		}
	}
	
	protected void mergeMeasures(DataMatrix merge, RawRow leftrow, RawRow rightrow, RawRow merged) {
		int pos = merge.getAxesSize();// start after axes
		if (leftrow != null) {
			// copy left part
			for (int i = 0; i < left.getDataSize(); i++) {
				merged.data[pos] = left.getDataValue(i, leftrow);
				pos++;
			}
		} else {
			pos += left.getDataSize();
		}
		if (rightrow != null) {
			// copy right part
			for (int i = 0; i < right.getDataSize(); i++) {
				merged.data[pos] = right.getDataValue(i, rightrow);
				pos++;
			}
		}

	}

	public DataMatrix merge(boolean sortInput) throws ScopeException {
		// need to work on sorted data
		List<RawRow> this_rows = sortInput?left.sortRows():left.getRows();
		List<RawRow> that_rows = sortInput?right.sortRows():right.getRows();
		if (sortInput) {
			// reset the ordering
			for (int i=0;i<ordering.length;i++) {
				ordering[i] = ORDERING.ASCENT;
			}
		}
		//
		Iterator<RawRow> this_iter = this_rows.iterator();
		Iterator<RawRow> that_iter = that_rows.iterator();
		RawRow this_row = null;
		RawRow that_row = null;
		
		DataMatrix merge = createMatrix();

		while (this_iter.hasNext() || that_iter.hasNext() || this_row!=null || that_row!=null) {
			// read if needed and available
			if (this_row==null && this_iter.hasNext()) {
				this_row = this_iter.next();
			}
			if (that_row==null && that_iter.hasNext()) {
				that_row = that_iter.next();
			}
			// manage remaining
			if ((this_row == null && that_row != null)) {
				RawRow merged = merge(merge, this_row, that_row);
				merge.pushRow(merged);
				that_row = null;
			}
			if ((this_row != null && that_row == null)) {
				RawRow merged = merge(merge, this_row, that_row);
				merge.pushRow(merged);
				this_row=null;		
			}
			// normal case
			if (this_row!=null && that_row!=null) {
				int cc = compare(this_row,that_row);
				if (cc<0) {
					RawRow merged = merge(merge, this_row, null);
					merge.pushRow(merged);
					this_row=null;
				}
				if (cc>0) {
					RawRow merged = merge(merge, null, that_row);
					merge.pushRow(merged);
					that_row=null;
				}
				if (cc==0) {
					RawRow merged = merge(merge, this_row, that_row);
					merge.pushRow(merged);
					that_row=null;this_row=null;
				}
			}
		}
		return merge;
	}

	protected DataMatrix createMatrix() {
		DataMatrix merge = new DataMatrix(left.getDatabase());
		merge.setFromCache(left.isFromCache() && right.isFromCache());
		merge.setFromSmartCache(left.isFromSmartCache() && right.isFromSmartCache());
		merge.setExecutionDate(new Date(Math.max(left.getExecutionDate().getTime(), right.getExecutionDate().getTime())));
		//
		createMatrixAxes(merge);
		createMatrixMeasures(merge);
		merge.setFullset(left.isFullset() && right.isFullset());
		return merge;
	}
	
	protected void createMatrixAxes(DataMatrix merge) {
		// list the new axis
		for (AxisValues ax : left.getAxes()) {
			merge.add(ax);
		}
	}
	
	protected void createMatrixMeasures(DataMatrix merge) {
		// list the new kpis
		merge.getKPIs().addAll(left.getKPIs());
		merge.getKPIs().addAll(right.getKPIs());
	}

}
