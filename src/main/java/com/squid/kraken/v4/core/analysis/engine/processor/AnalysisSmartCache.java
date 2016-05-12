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
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.codec.digest.DigestUtils;

import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.model.DashboardAnalysis;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.model.DomainSelection;
import com.squid.kraken.v4.core.analysis.model.GroupByAxis;
import com.squid.kraken.v4.core.analysis.model.Intervalle;
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
				AnalysisSmartCacheMatch match = checkMatch(null, signature, sameFilters);
				if (match!=null) {
					return match;
				}
			}
			// try to generalize the search ?
			Collection<Axis> filters = signature.getAnalysis().getSelection().getFilters();
			if (filters.size()>1) {
				// let try by excluding one filter at a time?
				for (Axis filter : filters) {
					HashSet<Axis> filterMinusOne = new HashSet<>(filters);
					filterMinusOne.remove(filter);
					String sign1 = signature.computeFiltersSignature(universe, new ArrayList<>(filterMinusOne));
					sameFilters = sameAxes.get(sign1);
					if (sameFilters!=null) {
						AnalysisSmartCacheMatch match = checkMatch(filterMinusOne, signature, sameFilters);
						if (match!=null) {
							try {
								DashboardSelection softFilters = new DashboardSelection();
								softFilters.add(filter, signature.getAnalysis().getSelection().getMembers(filter));
								// add the post-processing
								match.addPostProcessing(new DataMatrixTransformSoftFilter(softFilters));
								return match;
							} catch (ScopeException e) {
								// ignore
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
	 * @param filterMinusOne
	 * @param signature
	 * @param sameFilters
	 * @return
	 */
	private AnalysisSmartCacheMatch checkMatch(Set<Axis> restrict, AnalysisSignature signature,
			Set<AnalysisSignature> sameFilters) {
		// iter to check if we found a compatible query
		for (AnalysisSignature candidate : sameFilters) {
			if (signature.getMeasures().getKPIs().size() <= candidate.getMeasures().getKPIs().size()) {
				AnalysisSmartCacheMatch match = new AnalysisSmartCacheMatch(candidate);
				if (equals(restrict, signature, candidate, match)) {
					// check the measures
					Set<Measure> o1 = new HashSet<>(signature.getMeasures().getKPIs());
					Set<Measure> o2 = new HashSet<>(candidate.getMeasures().getKPIs());
					if (o2.containsAll(o1)) {
						// hide not requested metrics
						if (o2.removeAll(o1) && !o2.isEmpty()) {
							match.addPostProcessing(new DataMatrixTransformHideColumns<Measure>(o2));
						}
						// sort
						if (signature.getAnalysis().hasOrderBy()) {
							if (!signature.getAnalysis().getOrders().equals(match.getAnalysis().getOrders())) {
								match.addPostProcessing(new DataMatrixTransformOrderBy(signature.getAnalysis().getOrders()));
							}
						}
						// limit
						if (signature.getAnalysis().hasLimit()) {
							long ending = signature.getAnalysis().getLimit();
							if (signature.getAnalysis().hasOffset()) {
								ending += signature.getAnalysis().getOffset();
							}
							if (ending<match.getSignature().getRowCount()) {
								match.addPostProcessing(new DataMatrixTransformTruncate(signature.getAnalysis().getLimit(), signature.getAnalysis().getOffset()));
							}
						}
						return match;
					}
				}
			}
		}
		// else
		return null;
	}
		
	/**
	 * compare 2 selection knowing they have the same signatures
	 * @param restrict 
	 * @param signature
	 * @param candidate
	 * @param match2 
	 * @return
	 */
	private boolean equals(Set<Axis> restrict, AnalysisSignature signature, AnalysisSignature candidate, AnalysisSmartCacheMatch match) {
		// check filter values
		ArrayList<DataMatrixTransform> postProcessing = new ArrayList<>();
		for (DomainSelection ds : signature.getAnalysis().getSelection().get()) {
			for (Axis filter : ds.getFilters()) {
				if (restrict==null || restrict.contains(filter)) {
					if (!equals(ds, filter, signature, candidate, postProcessing)) {
						return false;
					}
				}
			}
		}
		// the same ? yes because we already know they have the same filters !
		for (DataMatrixTransform transform : postProcessing) {
			match.addPostProcessing(transform);
		}
		return true;
	}
	
	/**
	 * @param ds 
	 * @param filter
	 * @param signature
	 * @param candidate
	 * @param postProcessing
	 * @return
	 */
	private boolean equals(DomainSelection ds, Axis filter, AnalysisSignature signature, AnalysisSignature candidate,
			ArrayList<DataMatrixTransform> postProcessing) {
		Collection<DimensionMember> original = ds.getMembers(filter);
		Collection<DimensionMember> candidates = candidate.getAnalysis().getSelection().getMembers(filter);
		// check for a perfect match
		if (original.size()==candidates.size()) {
			HashSet<DimensionMember> check = new HashSet<>(original);
			if (check.containsAll(candidates)) {
				return true;
			} // check for time-range inclusion
			else if (filter.getDefinitionSafe().getImageDomain().isInstanceOf(IDomain.DATE)
					&& original.size()==1
					&& candidates.size()==1) {
				GroupByAxis groupBy = findGroupingJoin(filter, candidate.getAnalysis());
				if (groupBy!=null) {
					// use it only if the filter is visible
					Object originalValue = original.iterator().next().getID();
					Object candidateValue = candidates.iterator().next().getID();
					if (originalValue instanceof Intervalle
					   && candidateValue instanceof Intervalle) 
					{
						Intervalle originalDate = (Intervalle)originalValue;
						Intervalle candidateDate = (Intervalle)candidateValue;
						Date originalLowerBound = (Date)originalDate.getLowerBound();
						Date originalUpperBound = (Date)originalDate.getUpperBound();
						Date candidateLowerBound = (Date)candidateDate.getLowerBound();
						Date candidateUpperBound = (Date)candidateDate.getUpperBound();
						// check using before or equals
						if ((candidateLowerBound.before(originalLowerBound) || candidateLowerBound.equals(originalLowerBound))
							&& (originalUpperBound.before(candidateUpperBound) || originalUpperBound.equals(candidateUpperBound))) 
						{
							// we need to add some post-processing
							try {
								DashboardSelection softFilters = new DashboardSelection();
								softFilters.add(filter, original);
								// add the post-processing
								postProcessing.add(new DataMatrixTransformSoftFilter(softFilters));
								return true;
							} catch (ScopeException e) {
								return false;
							}
						}
					}
				}
			} else {
				return false;
			}
		}
		// check for simple inclusion
		else if (candidate.getAxes().contains(filter)// filter on visible axis, we can soft-filter
				&& candidates.containsAll(original)) {// yes, it's a subset
			// we need to add some post-processing
			try {
				DashboardSelection softFilters = new DashboardSelection();
				softFilters.add(filter, original);
				// add the post-processing
				postProcessing.add(new DataMatrixTransformSoftFilter(softFilters));
				return true;
			} catch (ScopeException e) {
				return false;
			}
		} else {
			return false;
		}
		return false;// please the compiler
	}

	/**
	 * Store the signature
	 * @param signature
	 * @param dm 
	 */
	public void put(Universe universe, AnalysisSignature signature, DataMatrix dm) {
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
			signature.setRowCount(dm.getRowCount());
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

	/**
	 * copied from AnalysisCompute
	 * @param join
	 * @param from
	 * @return
	 */
	private GroupByAxis findGroupingJoin(Axis join, DashboardAnalysis from) {
		DateExpressionAssociativeTransformationExtractor checker = new DateExpressionAssociativeTransformationExtractor();
		ExpressionAST naked1 = checker.eval(join.getDimension()!=null?join.getReference():join.getDefinitionSafe());
		IDomain d1 = join.getDefinitionSafe().getImageDomain();
		for (GroupByAxis groupBy : from.getGrouping()) {
			IDomain d2 = groupBy.getAxis().getDefinitionSafe().getImageDomain();
			if (d1.isInstanceOf(IDomain.TEMPORAL) && d2.isInstanceOf(IDomain.TEMPORAL)) {
				// if 2 dates, try harder...
				// => the groupBy can be a associative transformation of the filter
				ExpressionAST naked2 = checker.eval(groupBy.getAxis().getDefinitionSafe());
				if (naked1.equals(naked2)) {
					return groupBy;
				}
			} else if (join.equals(groupBy.getAxis())) {
				return groupBy;
			}
		}
		// else
		return null;
	}
	
}
