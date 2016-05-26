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
import com.squid.kraken.v4.core.analysis.universe.Axis;

/**
 * specialized version to support date comparison
 * @author sergefantino
 *
 */
public class CompareMerger extends JoinMerger {

	private Period offset;

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
	protected void mergeMeasures(IndirectionRow left, IndirectionRow right, IndirectionRow merged) {
		int pos = merged.getAxesCount();// start after axes
		for (int i = 0; i < merged.getDataCount()/2; i++) {
			if (left!=null) {
				merged.rawrow[pos] = left.getDataValue(i);
			}
			pos++;// always advance
			if (right!=null) {
				merged.rawrow[pos] = right.getDataValue(i);
			}
			pos++;// always advance
		}
	}
	
	@Override
	protected void createMatrixMeasures(DataMatrix merge) {
		// interleave KPIs
		for (int i=0; i<left.getKPIs().size(); i++) {
			merge.getKPIs().add(left.getKPIs().get(i));
			merge.getKPIs().add(right.getKPIs().get(i));
		}
	}

}
