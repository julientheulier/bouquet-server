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
package com.squid.kraken.v4.model;

import java.io.Serializable;

import com.squid.kraken.v4.api.core.JobResult;

public interface ComputationJob<PK extends GenericPK, R extends JobResult> extends Persistent<PK> {

    static public enum Status {
        PENDING, RUNNING, DONE
    };

    /**
     * The current state of this job.<br>
     * <ul>
     * <li>PENDING - Queued</li>
     * <li>RUNNING - Running</li>
     * <li>DONE - Completed, either successfully or not. If unsuccessful, the error field should not be null</li>
     * </ul>
     */
    public Status getStatus();

    /**
     * Description of the job failure. <tt>null</tt> if the job is OK.
     * 
     * @return
     */
    public Error getError();

    /**
     * Some statistics about the job.
     * 
     * @return
     */
    public Statistics getStatistics();

    // static classes
    @SuppressWarnings("serial")
    public static class Error implements Serializable {
        private String reason;
        private String message;
        
        /**
         * Indicates if the error is not fatal and the job can be rerun .
         * Used for exemple if the job failed because of a lazy evaluation when the data was not in cache.
         */        
        private boolean enableRerun = false; 

        
        public Error() {
        }

        public boolean isEnableRerun() {
			return enableRerun;
		}

		public void setEnableRerun(boolean enableRerun) {
			this.enableRerun = enableRerun;
		}

		public Error(String reason, String message) {
            super();
            this.reason = reason;
            this.message = message;
        }

        public String getReason() {
            return reason;
        }

        public String getMessage() {
            return message;
        }

    }

    @SuppressWarnings("serial")
    public static class Statistics implements Serializable {
        private Long startTime;
        private Long endTime;
        private Long totalBytesProcessed;

        public Statistics() {
        }

        public Statistics(Long startTime) {
            super();
            this.startTime = startTime;
        }

        /**
         * Start time of this job, in milliseconds since the epoch. <br>
         * This starts ticking when the job status changes to RUNNING.
         */
        public Long getStartTime() {
            return startTime;
        }

        public void setStartTime(Long startTime) {
            this.startTime = startTime;
        }

        /**
         * End time of the job, in milliseconds since the epoch. <br>
         * This is when the status changed to DONE, successfully or not. If the job has not finished, this property will
         * not be present.
         */
        public Long getEndTime() {
            return endTime;
        }

        public void setEndTime(Long endTime) {
            this.endTime = endTime;
        }

        public Long getTotalBytesProcessed() {
            return totalBytesProcessed;
        }

        public void setTotalBytesProcessed(Long totalBytesProcessed) {
            this.totalBytesProcessed = totalBytesProcessed;
        }

    }

    public void setStatus(Status status);

    public void setError(Error error);

    public void setStatistics(Statistics statistics);

    public Long getResultsSize();

    public void setResultsSize(Long resultsSize);

    /**
     * Check if this job is temporary or not.<br>
     * Temporary jobs will be eligible for Garbage Collection and can not be updated (whereas non temporary jobs can).
     * They can be created by all users having read rights on its parent Dashboard.<br>
     * Non temporary jobs can only be created by users having write rights on its parent Dashboard.<br>
     * 
     * @return
     */
    public Boolean getTemporary();

    public void setTemporary(Boolean temporary);
    
    /**
     * Check is this job should run as soon as it is created (default : true)
     * @return
     */
    public Boolean getAutoRun();
    
    public void setAutoRun(Boolean autoRun);
    
    public R getResults();
    
    public void setResults(R results);
    
	public Long getCreationTime() ;

	public void setCreationTime(Long creationTime) ;

}
