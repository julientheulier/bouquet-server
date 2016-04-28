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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import com.squid.kraken.v4.core.analysis.model.DashboardAnalysis;
import com.squid.kraken.v4.core.analysis.model.GroupByAxis;
import com.squid.kraken.v4.core.analysis.universe.Universe;

/**
 * Prototype: this class allow to define an analysis signature to intelligently reuse cached matrix
 * @author sergefantino
 *
 */
public class CachedAnalysisSignature {
	
	private DashboardAnalysis analysis;
	
	private String axesSignature = null;

	public CachedAnalysisSignature(Universe universe, DashboardAnalysis analysis) {
		super();
		this.analysis = analysis;
		this.axesSignature = computeAxesignature(universe);
	}
	
	/**
	 * @return the axesSignature
	 */
	public String getAxesSignature() {
		return axesSignature;
	}

	/**
	 * @return
	 */
	private String computeAxesignature(Universe universe) {
		ArrayList<GroupByAxis> ordered = new ArrayList<>(analysis.getGrouping());
		Collections.sort(ordered, new Comparator<GroupByAxis>() {
			@Override
			public int compare(GroupByAxis o1, GroupByAxis o2) {
				return o1.getAxis().getId().compareTo(o2.getAxis().getId());
			}
		});
		StringBuilder signature = new StringBuilder(universe.getProject().getOid());
		for (GroupByAxis axis : ordered) {
			signature.append("#").append(axis.getAxis().getId());
		}
		return signature.toString();
	}
	
	

}
