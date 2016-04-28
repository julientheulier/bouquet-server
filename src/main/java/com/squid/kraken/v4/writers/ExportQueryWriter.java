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
package com.squid.kraken.v4.writers;

import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.export.ExportSourceWriter;
import com.squid.kraken.v4.export.ExportSourceWriterVelocity;

/**
 * export a RawMatrix using a ExportSourceWriter
 * @author sergefantino
 *
 */
public class ExportQueryWriter extends QueryWriter {

	OutputStream out;
	ExportSourceWriter writer;
	long linesWritten;
	String jobID;

	static final Logger logger = LoggerFactory.getLogger(QueryWriter.class);

	public ExportQueryWriter(ExportSourceWriter w, OutputStream out, String jobId) {
		super(null);// no postProcessing
		this.out = out;
		this.writer = w;
		this.jobID = jobId;
	}

	@Override
	public void write() throws ComputingException {
		long startExport = System.currentTimeMillis();
		if (writer instanceof ExportSourceWriterVelocity) {
			((ExportSourceWriterVelocity) writer).setQueryMapper(this.mapper);
		}
		try {

			if (val instanceof RawMatrix) {
				this.linesWritten = writer.write((RawMatrix) val, out);
			}

			if (val instanceof RedisCacheValuesList) {
				this.linesWritten = writer.write((RedisCacheValuesList) val, out);
			}

			long stopExport = System.currentTimeMillis();
			logger.info("task=" + this.getClass().getName() + " method=compute.writeData" + " jobid=" + jobID
					+ " lineWritten=" + linesWritten + " duration=" + (stopExport - startExport)
					+ " error=false status=done");

		} catch (Throwable e) {
			logger.error("failed to export jobId=" + jobID + ":", e);
			throw e;
		}
	}

	public long getLinesWritten() {
		return this.linesWritten;
	}

}
