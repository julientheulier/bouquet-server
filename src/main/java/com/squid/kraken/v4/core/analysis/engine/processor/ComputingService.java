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
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.RenderingException;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainFacetCompute;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchyManager;
import com.squid.kraken.v4.core.analysis.engine.query.SimpleQuery;
import com.squid.kraken.v4.core.analysis.model.DashboardAnalysis;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.writers.QueryWriter;

/**
 * This is the main entry point for compute API. It supports performing execution of a analysisJob and a facetJob.
 * Also this service is responsible for initialization of domain Hierarchy: this is not exposed by API directly but called as a side-effect when needed.
 * 
 * The service is re-entrant, that is it is possible to call the ComputingService more than one time in the same callstack.
 * 
 * (bye bye the glitter-ball...)
 * @author sergefantino
 *
 */
public class ComputingService {
	
	public static final ComputingService INSTANCE = new ComputingService();
	
	private ComputingService() {
		//
	}
	
	/**
	 * execute the DashboardAnalysis. Delegate actual execution to an AnalysisCompute class.
	 * Return a DataMatrix. In order to do so the whole resultset will be loaded in memory.
	 * Once the DataMatrix is released, in memory resultset is managed by the RowCache.
	 * 
	 * @param analysis
	 * @param redis key if available
	 * @return
	 * @throws ComputingException
	 * @throws InterruptedException 
	 */

	public DataMatrix glitterAnalysis(final DashboardAnalysis analysis, String key ) throws ComputingException, InterruptedException {
		final AnalysisCompute compute = new AnalysisCompute(analysis.getUniverse());
		try {
			return compute.computeAnalysis(analysis);
		} catch (ScopeException | SQLScopeException e) {
			throw new ComputingException(e.getLocalizedMessage(), e);
		}
	}
	
	public DataMatrix glitterAnalysis(final DashboardAnalysis analysis ) throws ComputingException, InterruptedException {
		return this.glitterAnalysis(analysis, null);
	}
	
	/**
	 * execute the DashboardAnalysis but does not actually read the resultset: calling method is responsible for reading and managing in memory representation.
	 * It can be used to stream the result for instance without reading it in-memory first.
	 * That call will not cache the resultset as a RowCache.
	 * 
	 * @param analysis
	 * @return
	 * @throws ComputingException
	 * @throws InterruptedException 
	 */
 /*
	public ExecuteAnalysisResult executeAnalysis(final DashboardAnalysis analysis) throws ComputingException, InterruptedException {
		final AnalysisCompute compute = new AnalysisCompute(analysis.getUniverse());
		return compute.executeAnalysis(analysis);
	} */
	public void executeAnalysis(final DashboardAnalysis analysis, QueryWriter writer, boolean lazy) throws ComputingException, InterruptedException {
		final AnalysisCompute compute = new AnalysisCompute(analysis.getUniverse());
		compute.executeAnalysis(analysis, writer, lazy);
	}
	
	public String viewSQL(final DashboardAnalysis analysis) throws ComputingException, InterruptedException {
		try {
			final AnalysisCompute compute = new AnalysisCompute(analysis.getUniverse());
			String sql = compute.viewSQL(analysis);
			sql = "-- SQL code generated by Bouquet " + (new Date()) + "\n" + sql;
			return sql;
		} catch (ScopeException | SQLScopeException | RenderingException e) {
			throw new ComputingException(e.getLocalizedMessage(), e);
		}
	}

    public List<SimpleQuery> reinject(final DashboardAnalysis analysis) throws ComputingException, InterruptedException {
    	try {
    		final AnalysisCompute compute = new AnalysisCompute(analysis.getUniverse());
    		return compute.reinject(analysis);
		} catch (ScopeException | SQLScopeException | RenderingException e) {
			throw new ComputingException(e.getLocalizedMessage(), e);
		}
    }

    /**
     * compute the facets for the given domain.
     * @param ds
     * @param sel
     * @return
     * @throws ComputingException
     * @throws InterruptedException 
     * @throws ExecutionException 
     * @throws TimeoutException 
     */
    public Collection<Facet> glitterFacets(Universe universe, Domain domain, DashboardSelection sel, boolean includeDynamic) throws ComputingException, InterruptedException, TimeoutException {
        DomainFacetCompute compute = new DomainFacetCompute(universe);
        try {
            Collection<Facet> result = compute.computeDomainFacets(domain, sel, includeDynamic);
            // default is to block - calling thread have to handle timeout explicitly
            boolean needToWait = false;
            for (Facet facet : result) {
                if (!facet.isDone()) {
                    needToWait = true;
                    break;
                }
            }
            if (needToWait) {
                DomainHierarchyManager.INSTANCE.isHierarchyDone(universe, domain, null/*block until complete*/);
                return compute.computeDomainFacets(domain, sel, includeDynamic);
            } else {
                return result;
            }
        } catch (ScopeException e) {
            throw new ComputingException(e.getLocalizedMessage(), e);
        } catch (ExecutionException e) {
        	if (e.getCause()!=null) {
        		throw new ComputingException(e.getCause().getLocalizedMessage(), e.getCause());
        	} else {
        		throw new ComputingException(e.getLocalizedMessage(), e);
        	}
        }
    }
    
    public Collection<Facet> glitterFacets(Universe universe, Domain domain, DashboardSelection sel, Integer timeout, boolean includeDynamic) throws ComputingException, InterruptedException, TimeoutException {
        DomainFacetCompute compute = new DomainFacetCompute(universe);
        try {
            if (timeout!=null) {
                DomainHierarchyManager.INSTANCE.isHierarchyDone(universe, domain, timeout);
            }
            return compute.computeDomainFacets(domain, sel, includeDynamic);
        } catch (ScopeException e) {
            throw new ComputingException(e.getLocalizedMessage(), e);
        } catch (ExecutionException e) {
        	if (e.getCause()!=null) {
        		throw new ComputingException(e.getCause().getLocalizedMessage(), e.getCause());
        	} else {
        		throw new ComputingException(e.getLocalizedMessage(), e);
        	}
    }
    }
    
    /**
     * compute the facets for the given dimension
     * @param universe
     * @param domain
     * @param sel
     * @param axis
     * @param filter
     * @param offset
     * @param size
     * @param timeoutMs 
     * @return
     * @throws ComputingException
     * @throws InterruptedException
     * @throws TimeoutException 
     * @throws ExecutionException 
     */
    public Facet glitterFacet(Universe universe, Domain domain, DashboardSelection sel, Axis axis, String filter, int offset, int size, Integer timeoutMs) throws ComputingException, InterruptedException, TimeoutException {
        try {
        	DomainFacetCompute compute = new DomainFacetCompute(universe);
        	return compute.computeDimensionFacets(domain, sel, axis, filter, offset, size, timeoutMs);
        } catch (ExecutionException e) {
        	if (e.getCause()!=null) {
        		throw new ComputingException(e.getCause().getLocalizedMessage(), e.getCause());
        	} else {
        		throw new ComputingException(e.getLocalizedMessage(), e);
        	}
        }
    }

}
