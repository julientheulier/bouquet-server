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

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;

import com.squid.core.jdbc.engine.IExecutionItem;
import com.squid.kraken.v4.model.ProjectPK;

/**
 * A POJO that holds information relative to a query worker job execution
 * @author sergefantino
 *
 */
public class QueryWorkerJobStatus {
	
	public enum Status {
		EXECUTING,
		READING,
		INDEXING
	}
	
	private Status status;

	private String userID;
	private ProjectPK projectPK;
	private String key;
	private int ID;
	private String SQL;
	private long start;
	private long elapse;
	private long lineRead;
	private int chunks;
	
	private String elapseTime;
	
	/**
	 * for hierarchyQueries
	 * @param ustatus
	 * @param projectPK2
	 * @param key2
	 * @param id2
	 * @param sqlQuery
	 * @param start time
	 * @param elapsed tume
	 */
	public QueryWorkerJobStatus(Status status, ProjectPK projectPK, String key, int ID, String SQL, long linesRead, long start, long elapse) {
		this.status = status;
		this.projectPK = projectPK;
		this.key = key;
		this.ID = ID;
		this.SQL = SQL;
		this.elapse = elapse;
		this.lineRead = linesRead;
		this.elapseTime = DurationFormatUtils.formatDurationHMS(elapse);
	}
	
	
	
	/**
	 * status is executing
	 * @param userID2
	 * @param projectPK2
	 * @param key2
	 * @param id2
	 * @param sqlQuery
	 */
	public QueryWorkerJobStatus(String userID, ProjectPK projectPK, String key, int ID, String SQL, long start, long elapse) {
		this.status = Status.EXECUTING;
		this.userID = userID;
		this.projectPK = projectPK;
		this.key = key;
		this.ID = ID;
		this.SQL = SQL;
		this.elapse = elapse;
		this.elapseTime = DurationFormatUtils.formatDurationHMS(elapse);
	}
	
	/**
	 * status is reading
	 * @param userID
	 * @param projectPK
	 * @param key
	 * @param ID
	 * @param SQL
	 * @param start
	 * @param read
	 * @param chunks
	 */
	public QueryWorkerJobStatus(String userID, ProjectPK projectPK, String key, int ID, String SQL, long start, long elapse, long read, int chunks) {
		this.status = Status.READING;
		this.userID = userID;
		this.projectPK = projectPK;
		this.key = key;
		this.ID = ID;
		this.SQL = SQL;
		this.elapse = elapse;
		this.elapseTime = DurationFormatUtils.formatDurationHMS(elapse);
		this.lineRead = read;
		this.chunks = chunks;
	}
	
	/**
	 * @param request
	 * @param item
	 * @param start2
	 * @param batchUpperBound
	 * @param nbBatches
	 */
	public QueryWorkerJobStatus(QueryWorkerJobRequest request, IExecutionItem item, long start, long elapse, long lineRead,
			int nbChunks) {
		this(request.getUserID(), request.getProjectPK(), request.getKey(), item.getID(), request.getSQLQuery(), start, elapse, lineRead, nbChunks);
	}

	/**
	 * @return the status
	 */
	public Status getStatus() {
		return status;
	}

	/**
	 * @return the userID
	 */
	public String getUserID() {
		return userID;
	}
	
	/**
	 * @return the projectPK
	 */
	public ProjectPK getProjectPK() {
		return projectPK;
	}

	public String getKey() {
		return key;
	}

	public int getID() {
		return ID;
	}

	public String getSQL() {
		return SQL;
	}
	
	public long getStart() {
		return start;
	}

	public long getElapse() {
		return elapse;
	}

	public long getLineRead() {
		return lineRead;
	}

	public int getChunks() {
		return chunks;
	}
	
	public String getElaspeTime() {
		return elapseTime;
	}
	
	public String getStartTime() {
		return DateFormatUtils.ISO_DATETIME_FORMAT.format(start);
	}
	
}
