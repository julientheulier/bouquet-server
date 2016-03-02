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
package com.squid.kraken.v4.api.core;

import java.io.OutputStream;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.caching.NotInCacheException;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.export.ExportSourceWriter;
import com.squid.kraken.v4.model.ComputationJob;
import com.squid.kraken.v4.model.ComputationJob.Status;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.JobDAO;

/**
 * A Job runner.
 * 
 * @param <T>
 *            the ComputationJob to run
 * @param <PK>
 *            the ComputationJob PK
 */
public class JobTask<T extends ComputationJob<PK, R>, PK extends GenericPK, R extends JobResult> implements Callable<T> {

    static private final Logger logger = LoggerFactory.getLogger(JobTask.class);

    protected final T job;

    private Class<T> type;
    private JobComputer<T, PK, R> computer;
    private AppContext ctx;
    private Integer maxResults;
    private Integer startIndex;
    private boolean lazy;
    private boolean returnJobIfNotInCache= false; 
    
    
    private OutputStream outputStream = null;
    
    private ExportSourceWriter writer;

    /**
     * Constructor.
     * 
     * @param job
     *            job to compute
     * @param computer
     *            the job Computer
     * @param type
     *            job type
     * @param cache
     *            used to cache job results
     */
    public JobTask(AppContext ctx, T job, JobComputer<T, PK, R> computer, Class<T> type, OutputStream outputStream, ExportSourceWriter writer) {
        super();
        this.ctx = ctx;
        this.job = job;
        this.type = type;
        this.outputStream = outputStream;
        this.computer = computer;
        this.writer = writer;
    }
    
    public JobTask(AppContext ctx, T job, JobComputer<T, PK, R> computer, Class<T> type, Integer maxResults, Integer startIndex, boolean lazy) {
        super();
        this.ctx = ctx;
        this.job = job;
        this.type = type;
        this.computer = computer;
        this.maxResults = maxResults;
        this.startIndex = startIndex;
        this.lazy = lazy;
    }

    public JobTask(AppContext ctx, T job, JobComputer<T, PK, R> computer, Class<T> type, Integer maxResults, Integer startIndex, boolean lazy, boolean returnJob) {
        this(ctx, job, computer, type, maxResults, startIndex, lazy);
        this.returnJobIfNotInCache = returnJob;
    }

    
    
    
    @Override
    public T call() {
        logger.info("Running Job : " + job + "\n lazy?"+lazy );
        logger.info("JobId "+ job.getId());
        DAOFactory factory = DAOFactory.getDAOFactory();
        AppContext app = ServiceUtils.getInstance().getRootUserContext(job.getCustomerId());
        JobDAO<T, PK> dao = ((JobDAO<T, PK>) factory.getDAO(type));
        try {
            // set status to RUNNING
            job.setStatistics(new ComputationJob.Statistics(System.currentTimeMillis()));
            job.setStatus(Status.RUNNING);
            dao.update(app, job);

            // compute and get the results
            R results;
            if (outputStream == null) {
	            results = computer.compute(ctx, job, maxResults, startIndex, lazy);
            } else {
            	results = computer.compute(ctx, job, outputStream, writer, lazy);
            }
            job.setResultsSize(results.getTotalSize());
            job.setResults(results);
        	job.setError(null);
        } catch (Throwable e) {
            // set the job's 'error'
            logger.error("Error in Job : " + job, e);
            job.setError(new ComputationJob.Error("Computation error", e.getLocalizedMessage()));
            if ((e instanceof NotInCacheException)){
            	if (! this.returnJobIfNotInCache){
                	throw (NotInCacheException) e;
            	}else{
            		job.getError().setEnableRerun(true);
            	}
            } 
 		} finally {
            // update the jobs status and stats
            job.setStatus(Status.DONE);
            job.getStatistics().setEndTime(System.currentTimeMillis());
			//logger.info("task=JobTask"+" method=OverheadScheduler"+" jobid= "+job.getId()+" duration="+ (job.getStatistics().getStartTime()-job.getCreationTime())+" error=false status=done");

            dao.update(app, job);
        }
        return job;
    }

	public OutputStream getOutputStream() {
		return outputStream;
	}

	public void setOutputStream(OutputStream outputStream) {
		this.outputStream = outputStream;
	}

}