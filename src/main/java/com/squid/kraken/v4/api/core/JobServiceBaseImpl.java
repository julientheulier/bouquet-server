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

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Optional;
import com.squid.kraken.v4.api.core.APIException.ApiError;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.export.ExportSourceWriter;
import com.squid.kraken.v4.export.ExportSourceWriterCSV;
import com.squid.kraken.v4.model.ComputationJob;
import com.squid.kraken.v4.model.ComputationJob.Status;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.ProjectAnalysisJob;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.JobDAO;

/**
 * Base class for Job-based services.
 * 
 * @param <T>
 *            the ComputationJob type
 * @param <PK>
 *            the ComputationJob PK type
 * @param <R>
 *            the JobResult type
 */
public abstract class JobServiceBaseImpl<T extends ComputationJob<PK, R>, PK extends GenericPK, R extends JobResult>
extends GenericServiceImpl<T, PK> {

	private static final Logger logger = LoggerFactory
			.getLogger(JobServiceBaseImpl.class);

	public static enum OutputFormat {
		JSON, XLS, CSV, XML
	};

	public static enum OutputCompression {
		GZIP, NONE, NULL
	}

	public static final int JOBS_QUEUE_MAX_SIZE = 50;

	public static final int TEMP_JOB_MAX_AGE_SEC = 600;

	private ScheduledExecutorService jobsGC;

	private ExecutorService jobsExecutor;

	private ScheduledFuture<?> jobsGCThread;

	private final Class<T> type;

	private JobComputer<T, PK, R> computer;

	public JobServiceBaseImpl(Class<T> type, JobComputer<T, PK, R> computer) {
		super(type);
		this.type = type;
		this.computer = computer;
	}

	public JobComputer<T, PK, R> getComputer() {
		return computer;
	}

	public void setComputer(JobComputer<T, PK, R> computer) {
		this.computer = computer;
	}

	public void initJobsExecutor(int poolSize) {
		jobsExecutor = Executors.newFixedThreadPool(poolSize);
	}

	public void initJobsGC(int temporaryJobMaxAgeInSeconds) {
		try {
			// update all 'zombie' running tasks (caused by previous server
			// shutdown).
			DAOFactory factory = DAOFactory.getDAOFactory();
			JobDAO<T, PK> dao = ((JobDAO<T, PK>) factory.getDAO(type));
			List<T> findAllNotDone = dao.findAllNotDone();
			logger.info("Stopping " + findAllNotDone.size()
			+ " 'zombie' running tasks.");
			for (T job : findAllNotDone) {
				job.setError(new ComputationJob.Error("Task stopped",
						"Stopped due to server restart."));
				job.setStatus(Status.DONE);
				if (job.getStatistics() != null) {
					job.getStatistics().setEndTime(System.currentTimeMillis());
				}
				AppContext app = ServiceUtils.getInstance().getRootUserContext(
						job.getCustomerId());
				dao.update(app, job);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jobsGC = Executors.newSingleThreadScheduledExecutor();
			ModelGC<T, PK> gc = new ModelGC<T, PK>(temporaryJobMaxAgeInSeconds,
					this, type);
			jobsGCThread = jobsGC.scheduleWithFixedDelay(gc, 0, 1,
					TimeUnit.HOURS);

		}
	}

	public void shutdownJobsExecutor() {
		try {
			logger.info("stopping executor pool for "
					+ this.getClass().getName());
			if (jobsExecutor!=null) {
				jobsExecutor.shutdown();
				jobsExecutor.awaitTermination(2, TimeUnit.SECONDS);
				jobsExecutor.shutdownNow();
			}
			if (jobsGCThread!=null) {
				jobsGCThread.cancel(true);
			}
			logger.info("stopping jobs GC scheduler for "
					+ this.getClass().getName());
			if (jobsGC!=null) {
				jobsGC.shutdown();
				jobsGC.awaitTermination(2, TimeUnit.SECONDS);
				jobsGC.shutdownNow();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private ExecutorService getJobsExecutor() {
		if (jobsExecutor == null) {
			initJobsExecutor(JOBS_QUEUE_MAX_SIZE);
		} else {
			int size = ((ThreadPoolExecutor) jobsExecutor).getActiveCount();
			if (size > (JOBS_QUEUE_MAX_SIZE * (0.80))) {
				logger.warn("jobsExecutor active threads count > 80% : " + size);
			} else {
				// TODO set to debug level
				logger.info("jobsExecutor active threads count : " + size);
			}
		}
		return jobsExecutor;
	}

	/**
	 * Read job results.<br>
	 * If results are found in the cache, then results are returned.<br>
	 * If results are not found in the cache, and reRunIfNoResults is true, then
	 * the job computation is triggered and read the results.<br>
	 * 
	 * @param ctx
	 * @param jobId
	 * @param timeout
	 *            used if job computation is triggered
	 * @param reRunIfNoResults
	 * @param maxResults
	 * @param startIndex
	 * @return a {@link JobResult} or throws an APIException if job computing is
	 *         in progress or has failed.
	 * @throws InterruptedException 
	 */
	public R readResults(AppContext ctx, PK jobId, Integer timeoutMs,
			boolean reRunIfNoResults, Integer maxResults, Integer startIndex, boolean lazy, Integer retryDelayMs) {
		long start = System.currentTimeMillis();
		R results;
		// read the job state
		T job = read(ctx, jobId);
		// force run if autorun is false
		boolean forceRun = (job.getAutoRun() == false);
		if (job.getStatus().equals(Status.RUNNING)) {
			// job isn't done yet
			if ((retryDelayMs != null) && ((timeoutMs != null) && (timeoutMs > 0))) {
				// retry
				try {
					Thread.sleep(retryDelayMs);
				} catch (InterruptedException e) {
					throw new ComputingInProgressAPIException(null, ctx.isNoError(), null);
				}
				timeoutMs = timeoutMs - (int) (System.currentTimeMillis() - start);
				return readResults(ctx, jobId, timeoutMs, reRunIfNoResults, maxResults, startIndex,lazy, retryDelayMs);
			} else {
				throw new ComputingInProgressAPIException(null, ctx.isNoError(), null);
			}
		} else if (job.getStatus().equals(Status.PENDING)) {
			// start the job
			job = runJob(ctx, job, timeoutMs, forceRun, maxResults, startIndex, lazy);
			if (job.getStatus().equals(Status.DONE)) {
				return job.getResults();
			} else {
				throw new ComputingInProgressAPIException(null, ctx.isNoError(), null);
			}
		} else if (job.getError() == null) {
			// DONE
			try {
				results = computer.compute(ctx, job, maxResults, startIndex, lazy);
			} catch (ComputingException | InterruptedException e) {
				throw new APIException(job.getError().getMessage(), ctx.isNoError(), ApiError.COMPUTING_FAILED);
			}
		} else {
			// job completed with errors
			if (job.getTemporary()) {
				// temporary job - do not re-run
				throw new APIException(job.getError().getMessage(), ctx.isNoError(), ApiError.COMPUTING_FAILED);
			} else {
				if (reRunIfNoResults) {
					// re-run it and get the results
					store(ctx, job, timeoutMs);
					results = readResults(ctx, jobId, timeoutMs, false, maxResults,
							startIndex,lazy, retryDelayMs);
				} else {
					throw new APIException(job.getError().getMessage(), ctx.isNoError(), ApiError.COMPUTING_FAILED);
				}
			}
		}

		return results;
	}

	/**
	 * Write job results to output stream.
	 * 
	 * @param out
	 * @param ctx
	 * @param jobId
	 * @param timeout
	 * @param reRunIfNoResults
	 * @param maxResults
	 * @param startIndex
	 * @param outFormat
	 * @param outputCompression
	 * @param an optional ResultSetWriter (default is ResultSetWriterCSV)
	 * @throws InterruptedException 
	 */
	public void writeResults(OutputStream out, AppContext ctx, T job,
			Integer retryDelayMs, Integer timeoutMs, boolean reRunIfNoResults, Integer maxResults,
			Integer startIndex, boolean lazy, OutputFormat outFormat,
			OutputCompression outputCompression, ExportSourceWriter writer) throws InterruptedException {
		boolean jobRead = ((job.getId() != null) && (job.getId().getObjectId() != null));
		if ((outFormat == OutputFormat.JSON)
				&& (outputCompression == OutputCompression.NONE || outputCompression == OutputCompression.NULL)) {
			// return the results the default way
			JobResult results;
			if (jobRead) {
				results = readResults(ctx, job.getId(), timeoutMs, reRunIfNoResults,
						maxResults, startIndex,lazy, retryDelayMs);
			} else {
				results = store(ctx, job, timeoutMs).getResults();
			}
			// JSON
			ObjectMapper mapper = new ObjectMapper();
			try {
				ObjectWriter objectWriter = mapper.writer();
				objectWriter.writeValue(out, results);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			// return the results the "export" way
			if (writer == null) {
				writer = new ExportSourceWriterCSV();
			}
			if (jobRead) {
				job = read(ctx, job.getId());
			} else {
				job.setResultsSize(null);
				job.setStatistics(null);
				job.setError(null);
				job.setStatus(Status.PENDING);
				logger.info("job persist");
				job = super.store(ctx, job);
			}
			if (outputCompression == OutputCompression.GZIP) {
				try {
					out = new GZIPOutputStream(out);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
			runJob(ctx, job, out, true, timeoutMs, writer, maxResults, startIndex, lazy);
		}
	}

	protected abstract R paginateResults(R results, Integer maxResults,
			Integer startIndex);

	/**
	 * Same as {@link #store(String, ComputationJob, Integer)} with no timeout.
	 * 
	 * @return a job.
	 */
	@Override
	public T store(AppContext ctx, T job) {
		return store(ctx, job, null, true, null, null, false);
	}

	public T store(AppContext ctx, T job, Integer timeout) {
		return store(ctx, job, timeout, true, null, null, false);
	}

	public T store(AppContext ctx, T job, Integer timeout, Integer maxResults, Integer startIndex) {
		return store(ctx, job, timeout, true, maxResults, startIndex, false);
	}

	public T store(AppContext ctx, T job, Integer timeout, Integer maxResults, Integer startIndex, boolean lazy) {
		return store(ctx, job, timeout, true, maxResults, startIndex, lazy);
	}


	/**
	 * Create, update and/or re-compute a Job.<br>
	 * If a Job with the same Id already exist then (if non temporary, update
	 * it) and re-compute.<br>
	 * If a Job with the same Id does not exist then create it and compute.<br>
	 * If a created job has no jobId then it will be a temporary job. If a
	 * timeout is set, this method will return once the job is done or until the
	 * timeout is reached. If not, the job will simply be created but not
	 * executed.
	 * 
	 * @param customerId
	 * @param job
	 * @param timeout
	 *            in milliseconds
	 * @param run should the job start right away
	 * @return a job.
	 */
	public T store(AppContext ctx, T job, Integer timeout, boolean run, Integer maxResults, Integer startIndex, boolean lazy) {
		logger.info("job store begin + lazy? "+ lazy);
		T jobToStart = null;
		AppContext jobctx = ctx;
		T existingJob = null;
		
		boolean returnJob= true;
		if (job.getId() != null) {
			Optional<T> existingJobOpt = factory.getDAO(type).read(ctx, job.getId());
			if (existingJobOpt.isPresent()) {
				existingJob = existingJobOpt.get();
			}
		}
		if (existingJob != null) {
			if (!existingJob.getTemporary()) {
				// update the job and recompute (using root context)
				jobToStart = job;
				jobctx = ServiceUtils.getInstance().getRootUserContext(
						ctx.getCustomerId());
			} else {
				// just recompute
				jobToStart = existingJob;
			}
		} else {
			// job creation
			// treat temporary jobs
			if (job.getId().getObjectId() == null) {
				// object id is null : temporary
				job.setTemporary(true);
			} else {
				if ((job.getTemporary() == null) || (job.getTemporary())) {
					job.setTemporary(true);
				} else {
					// non-temporary
					job.setTemporary(false);
				}
			}
			// create a new job
			jobToStart = job;
		}

		// store the job as 'pending'
		jobToStart.setResultsSize(null);
		jobToStart.setStatistics(null);
		jobToStart.setError(null);
		jobToStart.setStatus(Status.PENDING);
		logger.info("job persist");
		jobToStart = super.store(jobctx, jobToStart);

		// start the job
		if (run) {
			jobToStart = runJob(jobctx, jobToStart, timeout, false, maxResults, startIndex, lazy, returnJob);
		}
		logger.info("job store end");
		return jobToStart;
	}

	private T runJob(AppContext ctx, T jobToStart, Integer timeout,
			boolean forceAutoRun, Integer maxResults, Integer startIndex, boolean lazy) {
		return this.runJob(ctx, jobToStart, timeout, forceAutoRun, maxResults, startIndex, lazy, false);
	}
	
	/**
	 * Run a job (if required). Conditions are : if its status is not RUNNING or
	 * if the job is autorun.

	 * @param ctx
	 * @param jobToStart
	 * @param timeout if not set will wait for results
	 * @param jobResultsCache
	 * @param forceAutoRun
	 *            bypass the autorun conditions check.
	 * @param lazy  use to return a Job only if the analysis is already in cache; else a HTTP 204 is returned
	 * @param returnJob if returnJob is set  to true, an AnalysisJob is always return, event in case NotInCacheException	 *
	 * 	 	This is used to return a job if it is newly created or updated  (e.g. this is called by store) 
	 * @return
	 */
	private T runJob(AppContext ctx, T jobToStart, Integer timeout,
			boolean forceAutoRun, Integer maxResults, Integer startIndex, boolean lazy, boolean returnJob) {
		logger.info("run job lazy?" + lazy);
		if ((!jobToStart.getStatus().equals(Status.RUNNING))
				&& ((jobToStart.getAutoRun() == true) || forceAutoRun)) {
			JobTask<T, PK, R> task = new JobTask<T, PK, R>(ctx, jobToStart, computer,
					type, maxResults, startIndex, lazy, returnJob);
			// and start it
			Future<T> submit = getJobsExecutor().submit(task);
			if (timeout != null) {
				logger.info("waiting for job results for " + timeout + " ms.");
				try {
					jobToStart = submit.get(timeout, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e1) {
					logger.info("job computation interrupted : "+e1.getMessage());
					jobToStart = super.read(ctx, jobToStart.getId());
				} catch (ExecutionException e1) {
					logger.warn("job computation exception : "+e1.getMessage(), e1);

					if ( e1.getCause() instanceof APIException){
						APIException ae  = (APIException) e1.getCause();
						throw ae;
					}else{
						jobToStart = super.read(ctx, jobToStart.getId());
					}
				} catch (CancellationException e1) {
					logger.info("job computation cancelled : "+e1.getMessage());
					jobToStart = super.read(ctx, jobToStart.getId());
				} catch (TimeoutException e1) {
					jobToStart = super.read(ctx, jobToStart.getId());
				}
			} else {
				try {
					jobToStart = submit.get();
				} catch (InterruptedException | ExecutionException e) {
					logger.warn("job computation exception : "+e.getMessage(), e);
					if ( e instanceof ExecutionException && e.getCause() instanceof APIException){
						APIException ae  = (APIException) e.getCause();
						throw ae;
					}
					jobToStart = super.read(ctx, jobToStart.getId());
				}
			}
		}
		return jobToStart;
	}

	/**
	 * Run a job (if required). Conditions are : if its status is not RUNNING or
	 * if the job is autorun.
	 * 
	 * @param ctx
	 * @param jobToStart
	 * @param out
	 *            an output stream to write the results in.
	 * @param forceAutoRun
	 *            bypass the autorun conditions check.
	 * @return
	 */
	private T runJob(AppContext ctx, T jobToStart,
			OutputStream out, boolean forceAutoRun, Integer timeout, ExportSourceWriter writer, Integer maxResults, Integer startIndex, boolean lazy) {
		if ((!jobToStart.getStatus().equals(Status.RUNNING)) || (forceAutoRun)) {
			JobTask<T, PK, R> task = new JobTask<T, PK, R>(ctx, jobToStart, computer,
					type, out, writer);

			if (writer == null) {
				// and start it asynchronously
				Future<T> submit = getJobsExecutor().submit(task);
				if (timeout != null) {
					logger.info("waiting for job results for " + timeout + " ms.");
					try {
						jobToStart = submit.get(timeout, TimeUnit.MILLISECONDS);
					} catch (InterruptedException e1) {
						logger.info("job computation interrupted : "+e1.getMessage());
						jobToStart = super.read(ctx, jobToStart.getId());
					} catch (ExecutionException e1) {
						logger.warn("job computation exception : "+e1.getMessage(), e1);
						if ( e1.getCause() instanceof APIException){
							APIException ae  = (APIException) e1.getCause();
							throw ae;
						}else{
							jobToStart = super.read(ctx, jobToStart.getId());
						}
					} catch (CancellationException e1) {
						logger.info("job computation cancelled : "+e1.getMessage());
						jobToStart = super.read(ctx, jobToStart.getId());
					} catch (TimeoutException e1) {
						jobToStart = super.read(ctx, jobToStart.getId());
					}
				}
			} else {
				// synchronous mode
				task.call();
				// check for errors
				if (jobToStart.getError() != null) {
					throw new APIException(jobToStart.getError().getMessage(), ctx.isNoError(), ApiError.COMPUTING_FAILED);
				}
			}
		}
		return jobToStart;
	}

	@Override
	public boolean delete(AppContext ctx, PK objectId) {
		return super.delete(ctx, objectId);
	}


}
