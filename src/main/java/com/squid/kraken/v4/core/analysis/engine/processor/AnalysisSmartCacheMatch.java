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
import java.util.List;

import com.squid.kraken.v4.core.analysis.model.DashboardAnalysis;
import com.squid.kraken.v4.core.analysis.model.MeasureGroup;

/**
 * AnalysisSmartCacheMatch is an object that provide information required to process a Smart Cache Match.
 * If we found a match, we also need to record the transformation to apply to the cached matrix in order to actually answer the requested analysis.
 * @author sergefantino
 *
 */
public class AnalysisSmartCacheMatch {
	
	private AnalysisSmartCacheSignature signature;
	private List<DataMatrixTransform> postProcessing = new ArrayList<>();
	
	/**
	 * create a match for the given original analysis
	 * @param signature is the original analysis that generated the cache content
	 */
	public AnalysisSmartCacheMatch(AnalysisSmartCacheSignature signature) {
		super();
		this.signature = signature;
	}
	
	/**
	 * @return the signature
	 */
	public AnalysisSmartCacheSignature getSignature() {
		return signature;
	}
	
	/**
	 * @return the analysis
	 */
	public DashboardAnalysis getAnalysis() {
		return signature.getAnalysis();
	}
	
	/**
	 * @return the measures
	 */
	public MeasureGroup getMeasures() {
		return signature.getMeasures();
	}

	/**
	 * @return the postProcessing
	 */
	public List<DataMatrixTransform> getPostProcessing() {
		return postProcessing;
	}
	
	public void addPostProcessing(DataMatrixTransform transform) {
		postProcessing.add(transform);
	}
	
}
