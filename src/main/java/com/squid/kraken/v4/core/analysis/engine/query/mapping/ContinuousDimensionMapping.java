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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.squid.core.expression.scope.ScopeException;
import com.squid.core.jdbc.formatter.IJDBCDataFormatter;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.model.IntervalleExpression;
import com.squid.kraken.v4.core.analysis.model.IntervalleObject;
import com.squid.kraken.v4.model.Domain;

public class ContinuousDimensionMapping extends DimensionMapping {

	private MeasureMapping kmin;
	private MeasureMapping kmax;

	public ContinuousDimensionMapping(Domain domain, DimensionIndex index, MeasureMapping kmin, MeasureMapping kmax) {
		super(null, domain, index);
		this.kmin = kmin;
		this.kmax = kmax;
	}
	
	public MeasureMapping getKmin(){
		return this.kmin;
	}
	
	public MeasureMapping getKmax(){
		return this.kmax;
	}
	
	
	@Override
	public void setMetadata(ResultSet result, ResultSetMetaData metadata)
			throws SQLException {
		//
		kmin.setMetadata(result, metadata);
		kmax.setMetadata(result, metadata);
	}
	
	@Override
	public Object readData(IJDBCDataFormatter formatter, ResultSet result)
			throws SQLException {
		//
		Object min = kmin.readData(formatter, result);
		Object max = kmax.readData(formatter, result);
		//
		if (min==null && max==null) {
			return null;
		} else if (min instanceof Comparable && max instanceof Comparable) {
			return new IntervalleObject((Comparable)min, (Comparable)max);
		} else {
			try {
				return new IntervalleExpression(min, max);
			} catch (ScopeException e) {
				throw new SQLException(e);
			}
		}
	}
	
}