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
package com.squid.kraken.v4.core.analysis.engine.processor;

import java.util.Collection;

import com.squid.kraken.v4.core.analysis.datamatrix.AxisValues;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.datamatrix.MeasureValues;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Property;

/**
 * Hide some fields
 * @author sergefantino
 *
 */
public class DataMatrixTransformHideColumns <T extends Property> implements DataMatrixTransform {
	
	private Collection<T> hideProperties = null;
	
	public DataMatrixTransformHideColumns(Collection<T> hideProperties) {
		this.hideProperties = hideProperties;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.core.analysis.engine.processor.DataMatrixTransform#apply(com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix)
	 */
	@Override
	public DataMatrix apply(DataMatrix input) {
		for (Property property : hideProperties) {
			if (property instanceof Axis) {
				AxisValues column = input.getAxisColumn((Axis)property);
				if (column!=null) {
					column.setVisible(false);
				}
			} else if (property instanceof Measure) {
				MeasureValues column = input.getColumn((Measure)property);
				if (column!=null) {
					column.setVisible(false);
				}
			}
		}
		return input;
	}
}
