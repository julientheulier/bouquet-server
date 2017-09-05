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

import org.joda.time.LocalDate;
import org.joda.time.Period;

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.caching.redis.datastruct.RawRow;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Property.OriginType;

/**
 * specialized version to support date comparison
 * @author sergefantino
 *
 */
public class CompareMerger extends JoinMerger {

	private Period offset;

	private boolean computeGrowth = false;

	public CompareMerger(DataMatrix left, DataMatrix right, int[] mergeOrder, Axis join, Period offset, boolean computeGrowth) throws ScopeException {
		this(left, right, mergeOrder, join, offset);
		this.computeGrowth = computeGrowth;
	}

	public CompareMerger(DataMatrix left, DataMatrix right, int[] mergeOrder, Axis join, Period offset) throws ScopeException {
		super(left, right, mergeOrder, join);
		this.offset = offset;
		// check measures
		if (left.getKPIs().size()!=right.getKPIs().size()) {
			throw new ScopeException("matrices kpis do not match");
		}
	}

	@Override
	protected Object translateRightToLeft(Object right) {
		if (right instanceof Date && offset!=null) {
			LocalDate delta = (new LocalDate(((Date)right).getTime())).plus(offset);
			return new java.sql.Date(delta.toDate().getTime());
		} else {
			return right;
		}
	}

	@Override
	protected Object translateLeftToRight(Object left) {
		if (left instanceof Date && offset!=null) {
			LocalDate delta = (new LocalDate(((Date)left).getTime())).minus(offset);
			return new java.sql.Date(delta.toDate().getTime());
		} else {
			return right;
		}
	}

	@Override
	protected int compareJoinValue(int pos, Object left, Object right) {
		if (right instanceof Date && offset!=null) {
			return ((Date)left).compareTo((new LocalDate(((Date)right).getTime())).plus(offset).toDate());
		} else {
			return super.compareJoinValue(pos, left, right);
		}
	}

	/**
	 * override to interleave present/past values
	 */
	@Override
	protected void mergeMeasures(DataMatrix merge, RawRow leftrow, RawRow rightrow, RawRow merged) {
		/*	int pos = merge.getAxesSize();// start after axes
		for (int i = 0; i < left.getDataSize(); i++) {// left.size==right.size
			if (leftrow!=null) {
				merged.data[pos] = left.getDataValue(i, leftrow);
			}
			pos++;// always advance
			if (rightrow!=null) {
				merged.data[pos] = right.getDataValue(i, rightrow);
			}
			pos++;// always advance
		}*/
		int pos = merge.getAxesSize();// start after axes
		int size = left!=null?left.getDataSize():(right!=null?right.getDataSize():0);
		for (int i = 0; i < size; i++) {
			Object leftValue = left!=null&&leftrow!=null?left.getDataValue(i, leftrow):null;
			Object rightValue = right!=null&&rightrow!=null?right.getDataValue(i, rightrow):null;
			merged.data[pos++] = leftValue;
			merged.data[pos++] = rightValue;
			// compute growth ?
			if (computeGrowth) {
				if (leftValue!=null && rightValue!=null) {
					if (leftValue instanceof Number && rightValue instanceof Number) {
						float leftf = ((Number)leftValue).floatValue();
						float rightf = ((Number)rightValue).floatValue();
						// compute the growth in %
						if (rightf!=0) {
							float growth = (leftf-rightf)*100/rightf;
							//String output = String.format("%+.2f%%", growth);
							merged.data[pos] = growth;
						}
					}
				}
				// always advance
				pos++;
			}
		}



	}

	@Override
	protected void createMatrixMeasures(DataMatrix merge) {
		// interleave KPIs
		for (int i=0; i<left.getKPIs().size(); i++) {
			merge.getKPIs().add(left.getKPIs().get(i));
			merge.getKPIs().add(right.getKPIs().get(i));
			if (computeGrowth) {
				// add the growth definition...
				Measure growth = new Measure(left.getKPIs().get(i).getMeasure());
				growth.setOriginType(OriginType.GROWTH);
				growth.setName(growth.getName() + " [growth%]");
				growth.setFormat("%.2f%%");
				merge.getKPIs().add(new MeasureValues(growth));
			}
		}
	}

}
