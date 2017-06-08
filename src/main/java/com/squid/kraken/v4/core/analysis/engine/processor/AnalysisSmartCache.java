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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;
import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
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
 * The SmartCache allow to store AnalysisSignature object and retrieve compatible analysis from the cache to reuse
 * @author sergefantino
 *
 */
public class AnalysisSmartCache {

	public static final AnalysisSmartCache INSTANCE = new AnalysisSmartCache();

	
	static final Logger logger = LoggerFactory.getLogger(AnalysisSmartCache.class);

	// simple set to control if a query is in the smart cache with no need to compute its key
	private Set<String> contains = new HashSet<>();

	// the structured lookup sets
	private Map<String, Map<String, HashSet<String>>> lookup = new ConcurrentHashMap<>();
	
	// the guava cache
	private Cache<String, AnalysisSmartCacheSignature> cache;
	
	private long CACHE_SIZE = 200;
	
	
	private AnalysisSmartCache() {
		
		cache = CacheBuilder.newBuilder()
				    .maximumSize(CACHE_SIZE) // 
				    .removalListener(new GuavaRemovalListener())
				    .build();
	}
	
	private class GuavaRemovalListener implements RemovalListener<String, AnalysisSmartCacheSignature> {

		@Override
		public void onRemoval(RemovalNotification<String, AnalysisSmartCacheSignature> notif) {
			AnalysisSmartCacheSignature signature = notif.getValue();
			String key = notif.getKey();
			logger.info("Removal notification " + key + " from smart cache ; cause "+ notif.getCause());
			Map<String, HashSet<String>> sameAxes = lookup.get(signature.getAxesSignature());
			if (sameAxes != null){
				HashSet<String> sameFilters = sameAxes.get(signature.getFiltersSignature());
				if (sameFilters.contains(key)){
					// check if it has been put back in guava
					if ( cache.getIfPresent(key) == null){
						sameFilters.remove(key);
					}
				}
				
			}
			
		}
		
	}	
	
	
	
	/**
	 * Check if an existing cache entry matches the given analysis signature request
	 * @param universe: this analysis universe
	 * @param request: the analysis signature we are looking to match
	 * @return
	 */
	public AnalysisSmartCacheMatch checkMatch(Universe universe, AnalysisSmartCacheRequest request) {
		// check same axis
		logger.info("Smart Cache: looking for exact match");
		{
			Map<String, HashSet<String>> sameAxes = lookup.get(request.getAxesSignature());
			if (sameAxes!=null) {
				logger.info("Smart Cache: found same signature");
				// check same filters
				HashSet<String> sameFiltersCandidates = sameAxes.get(request.getFiltersSignature());
				if (sameFiltersCandidates!=null) {
					logger.info("Smart Cache: found same filters");
					AnalysisSmartCacheMatch match = checkMatchMany(null, request, sameFiltersCandidates);
					if (match!=null) {
						logger.info("Smart Cache: found a match");
						return match;
					}
				}
			}
		}
		// try to generalize the search ?
		Collection<Axis> filters = request.getAnalysis().getSelection().getFilters();
		if (filters.size()>=1) {
			logger.info("Smart Cache: looking for filtered match");
			// let try by excluding one filter at a time?
			for (Axis filter : filters) {
				logger.info("Smart Cache: looking by adding "+filter.toString());
				// generalize the search by adding the filter as an axis
				AnalysisSmartCacheSignature generalize = new AnalysisSmartCacheSignature(request.getSignature(), filter);
				generalize.setAxesSignature(universe);// ok, the API is a bit awkward
				Map<String, HashSet<String>> sameAxes = lookup.get(generalize.getAxesSignature());
				if (sameAxes!=null) {
					logger.info("Smart Cache: found generalized signature");
					// get candidates with restriction
					HashSet<Axis> filterMinusOne = new HashSet<>(filters);
					filterMinusOne.remove(filter);
					String sign1 = request.computeFiltersSignature(universe, new ArrayList<>(filterMinusOne));
					HashSet<String> sameFiltersCandidates = sameAxes.get(sign1);
					if (sameFiltersCandidates!=null) {
						logger.info("Smart Cache: found same sub-filters");
						AnalysisSmartCacheMatch match = checkMatchMany(filterMinusOne, request, sameFiltersCandidates);
						if (match!=null) {
							try {
								logger.info("Smart Cache: found a match");
								DashboardSelection softFilters = new DashboardSelection();
								softFilters.add(filter, request.getAnalysis().getSelection().getMembers(filter));
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
	 * Check if the analysis with signature can match at least one candidate; if true will return a AnalysisMatch
	 * Hypothesis: all candidates have the same filter signature as the request
	 * @param restrict : if not empty only the filter that belongs to it will be taken into account. This may be used to generalize a match
	 * @param request : the analysis signature we are looking to match
	 * @param sameFiltersCandidates : a set of candidates to match - note that they all have the same filter signature as the candidate
	 * @return
	 */
	private AnalysisSmartCacheMatch checkMatchMany(Set<Axis> restrict, AnalysisSmartCacheRequest request,
			HashSet<String> sameFiltersCandidatesKeys) {
		// iter to check if we found a compatible query
		
		ImmutableMap<String, AnalysisSmartCacheSignature> sameFiltersCandidates = cache.getAllPresent(sameFiltersCandidatesKeys) ;
		
		for (AnalysisSmartCacheSignature candidate  : sameFiltersCandidates.values()) {
			// if not computing more kpis
			if (request.getMeasures().getKPIs().size() <= candidate.getMeasures().getKPIs().size()) {
				// check the filters to see if candidate contains request
				AnalysisSmartCacheMatch match = checkMatchSingle(restrict, request, candidate);
				if (match!=null) {
					// check the measures
					Set<Measure> o1 = new HashSet<>(request.getMeasures().getKPIs());
					Set<Measure> o2 = new HashSet<>(candidate.getMeasures().getKPIs());
					if (o2.containsAll(o1)) {
						// hide not requested metrics
						if (!o2.equals(o1)) {
							if (o2.removeAll(o1) && !o2.isEmpty()) {
								match.addPostProcessing(new DataMatrixTransformHideColumns<Measure>(o2));
							}
						}
						// sort
						if (request.getAnalysis().hasOrderBy()) {
							if (!request.getAnalysis().getOrders().equals(match.getAnalysis().getOrders())) {
								match.addPostProcessing(new DataMatrixTransformOrderBy(request.getAnalysis().getOrders()));
							}
						}
						// limit
						if (request.getAnalysis().hasLimit()) {
							long ending = request.getAnalysis().getLimit();
							if (request.getAnalysis().hasOffset()) {
								ending += request.getAnalysis().getOffset();
							}
							if (ending<match.getSignature().getRowCount()) {
								match.addPostProcessing(new DataMatrixTransformTruncate(request.getAnalysis().getLimit(), request.getAnalysis().getOffset()));
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
	 * Check if the request signature can match the candidate; if true will return a AnalysisMatch
	 * @param restrict : if not empty only the filter that belongs to it will be taken into account. This may be used to generalize a match
	 * @param request : the analysis signature we are looking to match
	 * @param candidate : the candidate for matching
	 * @return a match that tracks required postProcessing or null if it's not a match
	 */
	private AnalysisSmartCacheMatch checkMatchSingle(Set<Axis> restrict, AnalysisSmartCacheRequest request, AnalysisSmartCacheSignature candidate) {
		// check filter values
		AnalysisSmartCacheMatch match = new AnalysisSmartCacheMatch(candidate);
		for (DomainSelection ds : request.getAnalysis().getSelection().get()) {
			for (Axis filter : ds.getFilters()) {
				if (restrict==null || restrict.contains(filter)) {
					if (!checkMatchFilter(ds, filter, request, candidate, match)) {
						return null;
					}
				}
			}
		}
		// the same ? yes because we already know they have the same filters !
		return match;
	}
	
	/**
	 * Check if the analysis with signature can match the candidate on a given filter/axis. Matching may imply performing some postProcessing to restrict the output.
	 * @param selection : the domain selection (we use it to retrieve the filter members)
	 * @param filter : the filter to check against
	 * @param request : the analysis signature we are looking to match
	 * @param candidate : the candidate for matching
	 * @param match : the current match state to add postProcessing if needed
	 * @return
	 */
	private boolean checkMatchFilter(DomainSelection selection, Axis filter, AnalysisSmartCacheRequest request, AnalysisSmartCacheSignature candidate,
			AnalysisSmartCacheMatch match) {
		Collection<DimensionMember> original = selection.getMembers(filter);
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
								match.addPostProcessing(new DataMatrixTransformSoftFilter(softFilters));
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
				match.addPostProcessing(new DataMatrixTransformSoftFilter(softFilters));
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
	 * @param universe
	 * @param signature
	 */
	public boolean remove(AnalysisSmartCacheRequest request) {
		Map<String, HashSet<String>> sameAxes = lookup.get(request.getAxesSignature());
		if (sameAxes!=null) {
			HashSet<String> sameFilters = sameAxes.get(request.getFiltersSignature());
			if (sameFilters!=null) {
				String key = request.getSignature().buildCacheKey();
				if (!sameFilters.contains(key)) {
					boolean check = sameFilters.remove(key);
					return check;
				}
			}
		}
		// else
		return false;
	}
	
	/**
	 * @param signature
	 */
	public boolean remove(AnalysisSmartCacheSignature signature) {
		
		Map<String, HashSet<String>> sameAxes = lookup.get(signature.getAxesSignature());
		if (sameAxes!=null) {
			HashSet<String> sameFilters = sameAxes.get(signature.getFiltersSignature());
			if (sameFilters!=null) {
				String key = signature.buildCacheKey();
				if (!sameFilters.contains(key)) {
					boolean check = sameFilters.remove(key);
					return check;
				}
			}
		}
		// else
		return false;
	}

	/**
	 * Store the signature in the Smart Cache
	 * 
	 * Note that as a side effect the put() method is also responsible for initializing the cache structure. The structure is as follow:
	 * - a HashMap at the outermost level that stores the analysis "space": project, domain, axes and measures requested.
	 * - which contains a HashMap that stores the analysis "filters" : the ordered list of filters involved in the analysis
	 * - which contains a HashMap that stores the actual signatures
	 * @param signature
	 * @param dm 
	 */
	public boolean put(AnalysisSmartCacheRequest request) {
		Map<String, HashSet<String>> sameAxes = lookup.get(request.getAxesSignature());
		if (sameAxes==null) {
			sameAxes = new ConcurrentHashMap<String, HashSet<String>>();// create the filters map
			lookup.put(request.getAxesSignature(), sameAxes);
		}
		HashSet<String> sameFilters = sameAxes.get(request.getFiltersSignature());
		if (sameFilters==null) {
			sameFilters = new HashSet<String>();// create the signature set
			sameAxes.put(request.getFiltersSignature(), sameFilters);
		}
		String key = request.getSignature().buildCacheKey();
		if (!sameFilters.contains(key)) {
			sameFilters.add(key);
		}
		if (this.cache.getIfPresent(key) == null){
			this.cache.put(key, request.getSignature());	
		}
		//
		return contains.add(key);
	}
	
	/**
	 * return the signature for the request if in the cache and still valid
	 * @param request
	 * @return
	 */
	public AnalysisSmartCacheSignature get(AnalysisSmartCacheRequest request) {
		String key = request.getSignature().buildCacheKey();
		return this.cache.getIfPresent(key);
	}

	/**
	 * Check if the given request is in the smart-cache
	 * @param request query
	 * @return
	 */
	public boolean contains(AnalysisSmartCacheRequest request) {
		return contains.contains(request.getSignature().buildCacheKey());
	}

	/**
	 * copied from AnalysisCompute: check if there is a compatible groupByAxis in the analysis for this axis
	 * @param axis
	 * @param from
	 * @return
	 */
	private GroupByAxis findGroupingJoin(Axis axis, DashboardAnalysis from) {
		DateExpressionAssociativeTransformationExtractor checker = new DateExpressionAssociativeTransformationExtractor();
		ExpressionAST naked1 = checker.eval(axis.getDimension()!=null?axis.getReference():axis.getDefinitionSafe());
		IDomain d1 = axis.getDefinitionSafe().getImageDomain();
		for (GroupByAxis groupBy : from.getGrouping()) {
			IDomain d2 = groupBy.getAxis().getDefinitionSafe().getImageDomain();
			if (d1.isInstanceOf(IDomain.TEMPORAL) && d2.isInstanceOf(IDomain.TEMPORAL)) {
				// if 2 dates, try harder...
				// => the groupBy can be a associative transformation of the filter
				ExpressionAST naked2 = checker.eval(groupBy.getAxis().getDefinitionSafe());
				if (naked1.equals(naked2)) {
					return groupBy;
				}
			} else if (axis.equals(groupBy.getAxis())) {
				return groupBy;
			}
		}
		// else
		return null;
	}

}


