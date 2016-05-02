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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.codec.digest.DigestUtils;

import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.model.DomainSelection;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Universe;

/**
 * The SmartCache allow to store AnalysisSignature and retrieve compatible analysis
 * @author sergefantino
 *
 */
public class AnalysisSmartCache {

	public static final AnalysisSmartCache INSTANCE = new AnalysisSmartCache();

	// simple set to control if a qeury is in the smart cache with no need to compute its key
	private Set<String> contains = new HashSet<>();
	
	// the structured lookup sets
	private Map<String, Map<String, Set<AnalysisSignature>>> lookup = new ConcurrentHashMap<>();

	private AnalysisSmartCache() {
		//
	}
	
	/**
	 * Check if an Analysis match the incoming signature
	 * @param model
	 * @return
	 */
	public AnalysisSmartCacheMatch checkMatch(Universe universe, AnalysisSignature signature) {
		// check same axis
		Map<String, Set<AnalysisSignature>> sameAxes = lookup.get(signature.getAxesSignature(universe));
		if (sameAxes!=null) {
			// check same filters
			Set<AnalysisSignature> sameFilters = sameAxes.get(signature.getFiltersSignature(universe));
			if (sameFilters!=null) {
				// iter to check if we found a compatible query
				for (AnalysisSignature match : sameFilters) {
					if (signature.getMeasures().getKPIs().size()<=match.getMeasures().getKPIs().size()) {
						if (equals(signature.getAnalysis().getSelection(), match.getAnalysis().getSelection())) {
							// check the measures
							Set<Measure> o1 = new HashSet<>(signature.getMeasures().getKPIs());
							Set<Measure> o2 = new HashSet<>(match.getMeasures().getKPIs());
							if (o2.containsAll(o1)) {
								AnalysisSmartCacheMatch result = new AnalysisSmartCacheMatch(match);
								if (o2.removeAll(o1) && !o2.isEmpty()) {
									result.addPostProcessing(new DataMatrixTransformHideColumns<Measure>(o2));
								}
								return result;
							}
						}
					}
				}
			}
		}
		//
		return null;
	}
		
	/**
	 * compare 2 selection knowing they have the same signatures
	 * @param signature
	 * @param match
	 * @return
	 */
	private boolean equals(DashboardSelection signature, DashboardSelection match) {
		// check filter values
		for (DomainSelection ds : signature.get()) {
			for (Axis filter : ds.getFilters()) {
				Collection<DimensionMember> original = ds.getMembers(filter);
				Collection<DimensionMember> candidates = match.getMembers(filter);
				if (original.size()==candidates.size()) {
					HashSet<DimensionMember> check = new HashSet<>(original);
					if (!check.containsAll(candidates)) {
						return false;
					}
				} else {
					return false;
				}
			}
		}
		// the same ? yes because we already know they have the same filters !
		return true;
	}
	
	/**
	 * Store the signature
	 * @param signature
	 */
	public void put(Universe universe, AnalysisSignature signature) {
		Map<String, Set<AnalysisSignature>> sameAxes = lookup.get(signature.getAxesSignature(universe));
		if (sameAxes==null) {
			sameAxes = new HashMap<>();
			lookup.put(signature.getAxesSignature(universe), sameAxes);
		}
		Set<AnalysisSignature> sameFilters = sameAxes.get(signature.getFiltersSignature(universe));
		if (sameFilters==null) {
			sameFilters = new HashSet<>();
			sameAxes.put(signature.getFiltersSignature(universe), sameFilters);
		}
		if (!sameFilters.contains(signature)) {
			sameFilters.add(signature);
		}
		//
		contains.add(signature.getHash());
	}

	/**
	 * Check if the given SQL query is in the smart-cache
	 * @param sQL
	 * @return
	 */
	public boolean contains(String SQL) {
		String hash = DigestUtils.sha256Hex(SQL);
		return contains.contains(hash);
	}
	
}
