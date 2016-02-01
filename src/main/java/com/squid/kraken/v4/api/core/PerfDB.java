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

import com.squid.kraken.v4.model.ProjectAnalysisJob;
import com.squid.kraken.v4.persistence.MongoDBHelper;
import org.mongodb.morphia.Datastore;
import org.slf4j.Logger;

/**
 * Created by lrabiet on 02/11/15.
 */
public class PerfDB {
    // Used for performance logs
    // and sql queries storage.

    public static Datastore INSTANCE = MongoDBHelper.getDatastore();

    public static void logPerf(Logger logger, ProjectAnalysisJob job, String method, Boolean error, long duration, String msg) {
        JobStats queryLog;
        long stop = System.currentTimeMillis();
        logger.info("task="+logger.getClass()+" method="+method+" jobid="+job.getId().getAnalysisJobId().toString()+" duration="+duration+ " error=true "+" "+msg);
        queryLog = new JobStats(job.getId().getAnalysisJobId().toString(),method, duration, job.getId().getProjectId());
        queryLog.setError(error);
        PerfDB.INSTANCE.save(queryLog);
    }
 }
