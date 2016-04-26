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
package com.squid.kraken.v4.caching.redis.queryworkerserver;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.jdbc.engine.IExecutionItem;
import com.squid.kraken.v4.caching.redis.RedisCacheException;
import com.squid.kraken.v4.caching.redis.datastruct.ChunkRef;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrixStreamExecRes;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;

/**
 * This internal class support continuing the matrix fetch from an ExecutionItem
 * in background
 * 
 * @author sergefantino
 *
 */
class CallableChunkedMatrixFetch implements Callable<Boolean> {

	static final Logger logger = LoggerFactory.getLogger(CallableChunkedMatrixFetch.class);

	private String key;
	private IExecutionItem item;
	private int ttl;
	private long nbLinesLeftToRead;
	private int nbBatches;
	private long batchLowerBound;
	private long batchUpperBound;
	private RedisCacheValuesList valuesList;
	private QueryWorkerServer server;

	public CallableChunkedMatrixFetch(QueryWorkerServer server, String key, RedisCacheValuesList valuesList,
			IExecutionItem item, int ttl, long nbLinesRead, long limit) {
		this.server = server;
		this.key = key;
		this.item = item;
		this.ttl = ttl;
		this.nbLinesLeftToRead = limit - nbLinesRead;
		this.batchLowerBound = 0;
		this.batchUpperBound = nbLinesRead;
		this.valuesList = valuesList;
		this.nbBatches = 1;
	}

	@Override
	public Boolean call() throws SQLException {
		boolean done = false;
		RawMatrixStreamExecRes nextBatch = null;
		boolean error = false;
		try {
			server.incrementLoad();
			do {
				try {
					nextBatch = RawMatrix.streamExecutionItemToByteArray(item, server.getMaxRecords(),
							nbLinesLeftToRead);
				} catch (IOException | SQLException e) {
					error = true;
				}
				if (error) {
					valuesList.setError();
				} else {
					nbLinesLeftToRead -= nextBatch.getNbLines();
					batchLowerBound = batchUpperBound;
					batchUpperBound = batchLowerBound + nextBatch.getNbLines();
					String batchKey = key + "_" + batchLowerBound + "-" + (batchUpperBound - 1);
					if (server.put(batchKey, nextBatch.getStreamedMatrix(), ttl)) {
						valuesList.addReferenceKey(new ChunkRef(batchKey, batchLowerBound, batchUpperBound));
						if (!nextBatch.hasMore()) {
							valuesList.setDone();
							done = true;
						}
					} else {
						valuesList.setError();
						error = true;
					}
				}
				server.put(key, valuesList);
				this.nbBatches += 1;

			} while (!done && !error);

			if (error) {
				throw new RedisCacheException(
						"We did not manage to store the result for queryid #" + item.getID() + "in redis");
			} else {
				logger.info("Result for SQLQuery#" + item.getID() + "was split into " + nbBatches + " batches; queryid=#" + item.getID());
			}
			return true;
		} finally {
			server.decrementLoad();
			if (item != null)
				item.close();

		}
	}

}