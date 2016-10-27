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
import java.util.List;

import com.squid.kraken.v4.caching.redis.datastruct.RawRow;
import com.squid.kraken.v4.model.AnalyticsQuery;

/**
 * return the DataMatrix as a array of records
 * @author sergefantino
 *
 */
public class TableConverter implements IDataMatrixConverter<Object[]> {

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.api.core.bb.IDataMatrixConverter#convert(com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix)
	 */
	@Override
	public Object[] convert(AnalyticsQuery query, DataMatrix matrix) {
		ArrayList<Object> records = new ArrayList<>();
		List<RawRow> rows = matrix.getRows();
		// handling pages
		Integer startIndex = query.getStartIndex();
		if (startIndex == null) {
			startIndex = 0;
		}
		startIndex = Math.max(startIndex, 0);
		Integer maxResults = query.getMaxResults();
		if (maxResults == null) {
			maxResults = rows.size();
		}
		maxResults = Math.max(maxResults, 0);
		int endIndex = Math.min(rows.size(), startIndex + maxResults);
		if (startIndex < endIndex) {
			for (int rowIndex = startIndex; rowIndex < endIndex; rowIndex++) {
				RawRow row = rows.get(rowIndex);
				ArrayList<Object> record = new ArrayList<>(row.size());
				int i = 0;
				for (AxisValues axis : matrix.getAxes()) {
					Object value =  matrix.getAxisValue(i++, row); 
					record.add(value);
				}
				int j = 0;
				for (MeasureValues measure : matrix.getKPIs()) {
					Object value = matrix.getDataValue(j++, row);
					record.add(value);
				}
				records.add(record);
			}
		}
		return records.toArray();
	}

}
