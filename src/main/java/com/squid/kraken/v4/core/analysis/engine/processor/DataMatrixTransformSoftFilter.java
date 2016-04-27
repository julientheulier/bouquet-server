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

import java.util.List;

import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.universe.Axis;

/**
 * perform the SoftFilter transformation:
 * - filter the matrix using the given selection
 * - hide the axis
 * @author sergefantino
 *
 */
public class DataMatrixTransformSoftFilter implements DataMatrixTransform {

	private DashboardSelection softFilters;
	private List<Axis> hiddenAxes;

	public DataMatrixTransformSoftFilter(DashboardSelection softFilters, List<Axis> hiddenAxes) {
		this.softFilters = softFilters;
		this.hiddenAxes = hiddenAxes;
	}

	@Override
	public DataMatrix apply(DataMatrix input) {
		// hide axis in case there are coming from generalized query
		for (Axis axis : hiddenAxes) {
			input.getAxisColumn(axis).setVisible(false);
		}
		// apply the soft filters if any left
		if (!softFilters.isEmpty()) {
			input = input.filter(softFilters, false);//ticket:2923 Null values must not be retained.
		}
		return input;
	}
}
