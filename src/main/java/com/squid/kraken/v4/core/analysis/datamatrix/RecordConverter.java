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
import java.util.HashMap;

import com.squid.kraken.v4.caching.redis.datastruct.RawRow;
import com.squid.kraken.v4.core.analysis.universe.Measure;

/**
 * return the DataMatrix as a array of records
 * @author sergefantino
 *
 */
public class RecordConverter implements IDataMatrixConverter<Object[]> {

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.api.core.bb.IDataMatrixConverter#convert(com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix)
	 */
	@Override
	public Object[] convert(DataMatrix matrix) {
		ArrayList<Object> records = new ArrayList<>();
		for (RawRow row : matrix.getRows()) {
			HashMap<String, Object> record = new HashMap<>(row.size());
			int i = 0;
			for (AxisValues axis : matrix.getAxes()) {
				Object value =  matrix.getAxisValue(i++, row); 
				record.put(axis.getAxis().getName(), value);
			}
			int j = 0;
			for (MeasureValues measure : matrix.getKPIs()) {
				Object value = matrix.getDataValue(j++, row);
				record.put(measure.getMeasure().getName(), value);
			}
			records.add(record);
		}
		return records.toArray();
	}

}
