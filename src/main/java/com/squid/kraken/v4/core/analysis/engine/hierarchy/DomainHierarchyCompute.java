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
package com.squid.kraken.v4.core.analysis.engine.hierarchy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.concurrent.CancellableCallable;
import com.squid.core.concurrent.ExecutionManager;
import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex.Status;
import com.squid.kraken.v4.core.analysis.engine.query.HierarchyQuery;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.DimensionMapping;

/**
 * Compute the Domain Hierarchy...
 * 
 * @author sergefantino
 *
 */
public class DomainHierarchyCompute 
extends DomainHierarchyQueryGenerator
implements CancellableCallable<Boolean> {
    
    static final Logger logger = LoggerFactory.getLogger(DomainHierarchyCompute.class);
    
    private List<Future<Boolean>> jobs;
    private HashMap<DimensionIndex, Future<Boolean>> jobLookup;
    private DomainHierarchy hierarchy ;
    
    public DomainHierarchyCompute(DomainHierarchy hierarchy) {
    	super(hierarchy);
    	this.hierarchy = hierarchy;
    	hierarchy.setCompute(this);
    }

	@Override
	public Boolean call() throws Exception {

		logger.info("starting computation for " + hierarchy.toString());
        // prepare the queries
		this.hierarchy.setState(DomainHierarchy.State.STARTED);
        try {
        	
            List<HierarchyQuery> queries = prepareQueries();
   
        // run the queries
            logger.info("Preparing queries for "+hierarchy+" : "+queries.size() + " queries");
            logger.info("class="+this.getClass().getName()+" size="+queries.size());
      

            CountDownLatch latch = runExecuteQueries(queries);
            //logger.info("queries executing");
            latch.await();
            //logger.info("computation ok");
			boolean result = true ;
			for (Future<Boolean> job : jobs) {
				if (job.isCancelled()) {
					result = false;
					break;
				}
			 }            
			this.hierarchy.setState(DomainHierarchy.State.DONE) ;
			return result;
        } catch (ScopeException | SQLScopeException | DatabaseServiceException e) {
            // unable to run any query
            // need to provide some feedback to the user ?
            //result.setFatalError(e);
			this.hierarchy.setState(DomainHierarchy.State.CANCELLED) ;
			return false;
        } catch (InterruptedException e) {
			// cancel the queries
        	for (Future<Boolean> job : jobs) {
				if (!job.isCancelled()) {
					job.cancel(true);
				}
			 } 
			this.hierarchy.setState(DomainHierarchy.State.CANCELLED) ;
			return false;
		}
    }

    /**
     * check if the domainHierarchy is done (for every dimension)
     * 
     * @param timeoutMs wait for timeout ms if > 0; if zero, won't block if not complete; if NULL, will block until it's complete
     * @return
     * @throws InterruptedException
     * @throws TimeoutException
     * @throws ExecutionException
     */
    public boolean isDone(Integer timeoutMs) throws InterruptedException, TimeoutException, ExecutionException {
        if (jobs==null) return false;
        long start = System.currentTimeMillis();
        long elapse = 0;
        for (Future<Boolean> job : jobs) {
            if (!job.isDone()) {
                if (timeoutMs==null) {
                    job.get();
                } else {
                    int remaining = timeoutMs - (int)elapse;
                    if (remaining>0) {
                        job.get(remaining,TimeUnit.MILLISECONDS);// wait for completion
                    } else {
                        // computing still in progress
                        return false;
                    }
                    elapse = System.currentTimeMillis() - start;
                }
            }
        }
        // ok
        return true;
    }

    public boolean isDone(DimensionIndex index, Integer timeoutMs) throws InterruptedException, ExecutionException, TimeoutException {
        if (index.getStatus()==Status.DONE || index.getStatus()==Status.ERROR) return true;
        if (jobLookup==null) return false;
        Future<Boolean> job = jobLookup.get(index);
        if (job==null) return false;
        if (!job.isDone()) {
        	if (timeoutMs==null) {
                job.get();
        	} else if (timeoutMs>0) {
                job.get(timeoutMs,TimeUnit.MILLISECONDS);
            } else {
                return false;
            }
        }
        return (index.getStatus()==Status.DONE || index.getStatus()==Status.ERROR);
    }
    
    /**
     * cancel the jobs execution
     */
    public void cancel() {
        if (jobs!=null) {
            for (Future<Boolean> job : jobs) {
                job.cancel(true);
            }
        }
    }


    /**
     * Execute the queries
     * @param queries
     */
    protected CountDownLatch runExecuteQueries(List<HierarchyQuery> queries) {
        // update the latches
        // -- arm the execution latch
 
        CountDownLatch latch = new CountDownLatch(queries.size()); 
        //logger.info ("latch armed " + latch.getCount());
        jobs = new ArrayList<>();
        jobLookup = new HashMap<>();
        for (final HierarchyQuery query : queries) {
            // start the job
        	String customerId = query.getSelect().getUniverse().getProject().getCustomerId();
            Future<Boolean> future = ExecutionManager.INSTANCE.submit(customerId,
                    new ExecuteHierarchyQuery(latch, query));
            // keep an eye on it
            jobs.add(future);
            for (DimensionMapping m : query.getDimensionMapping()) {
                jobLookup.put(m.getDimensionIndex(),future);
            }
        }
        return latch;
    }
}
