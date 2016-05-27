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
package com.squid.kraken.v4.core.database.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.concurrent.CancellableCallable;
import com.squid.core.concurrent.ExecutionManager;
import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.database.impl.DataSourceReliable.FeatureSupport;
import com.squid.core.database.model.impl.DatabaseManager;
import com.squid.core.jdbc.engine.ExecutionItem;
import com.squid.core.jdbc.engine.IExecutionItem;
import com.squid.core.jdbc.formatter.IJDBCDataFormatter;
import com.squid.core.sql.render.ISkinFeatureSupport;
import com.squid.kraken.v4.api.core.PerfDB;
import com.squid.kraken.v4.api.core.SQLStats;

/**
 * handles query execution in order to support non-standard JDBC query
 * cancellation
 * 
 * @author sergefantino
 *
 */
public class ExecuteQueryTask implements CancellableCallable<IExecutionItem> {

	static final Logger logger = LoggerFactory.getLogger(ExecuteQueryTask.class);

	private DatabaseManager ds;
	private int queryNum;
	private String sql;

	// keep a pointer on the statement so we can call cancel on it
	private Connection connection;
	private Statement statement;

	private boolean prepared = false;
	private volatile boolean abort = false;

	private String workerId;
	private String jobId;
	
	public ExecuteQueryTask(DatabaseManager ds, int queryNum, String sql) {
		this.ds = ds;
		this.queryNum = queryNum;
		this.sql = sql;
	}

    public void cancel() {
        if (statement!=null) {
            try {
                statement.cancel();
	            logger.info("cancel SQLQuery#" + queryNum +" jobId "+jobId + " on worker " + this.workerId );
            } catch (SQLException e) {
	            logger.error("failed to cancel SQLQuery#" + queryNum + " jobId "+jobId + " on worker " + this.workerId);
            }
        }
        this.abort = true;// signal that client should abort asap
    }

	/**
	 * return true if cancel() has been called - this is useful since canceling
	 * the SQL statement may not work, or client code may be already reading the
	 * resultset
	 * 
	 * @return
	 */
	public boolean isInterrupted() {
		return abort;
	}

	public void setWorkerId(String workerId){
		this.workerId = workerId;
	}
	
	public void setJobId(String jobId){
		this.jobId = jobId;
	}
	
	/**
	 * optionally prepare the call and make sure to allocate a connection from
	 * the pool
	 * 
	 * @throws DatabaseServiceException
	 */
	public void prepare() throws SQLException {
		if (!prepared) {
			prepared = true;// if failed, don't call it again
			try {
				connection = ds.getDatasource().getConnectionBlocking();
				if (logger.isDebugEnabled()) {
					logger.debug("Driver used for the connection", connection
							.getMetaData().getDriverName());
				}
				statement = connection.createStatement();
			} catch (Exception e) {
				if (connection != null) {
					try {
						if (!connection.getAutoCommit()) {
							connection.rollback();
						}
						connection.close();
						ds.getDatasource().releaseSemaphore();
						connection = null;
					} catch (SQLException ee) {
						// ignore
					}
				}
				logger.error("failed to prepare SQLQuery#" + queryNum + " jobId "+jobId + " on worker " + this.workerId + " queryid=" + queryNum
						+ " method=prepare() status=error error=" + e.toString());
				throw e;
			}
		}
	}

	public IExecutionItem call() throws SQLException, ExecutionException {
		long now = System.currentTimeMillis();
		try {
			prepare();
			//
			boolean needCommit = false;

			if (connection == null || statement == null) {
				throw new SQLException("failed to connect SQLQuery#" + queryNum + " jobId "+jobId + " on worker " + this.workerId + " queryid=" + queryNum
						+ " method=call() status=error");
			}
			// ok to start
			ExecutionManager.INSTANCE.registerTask(this);
			//
			try {
				// make sure auto commit is false (for cursor based ResultSets
				// and postgresql)
				if (ds.getSkin().getFeatureSupport(FeatureSupport.AUTOCOMMIT) == ISkinFeatureSupport.IS_NOT_SUPPORTED) {
					connection.setAutoCommit(false);
					needCommit = true;
				} else {
					connection.setAutoCommit(true);
				}
				IJDBCDataFormatter formatter = ds.getDataFormatter(connection);

				statement.setFetchSize(formatter.getFetchSize());
				logger.info("starting SQLQuery#" + queryNum +" jobId "+jobId + " on worker " + this.workerId+ " jdbc=" + ds.getConfig().getJdbcUrl() + " sql=\n" + sql
						+ "\n hashcode=" + sql.hashCode() + " method=executeQuery" + " duration="
						+ " error=false status=done driver=" + connection.getMetaData().getDatabaseProductName()
						+" queryid=" + queryNum + " task=" + this.getClass().getName());
				Date start = new Date();
				// ResultSet result = statement.executeQuery(sql);
				boolean isResultset = statement.execute(sql);
				while (!isResultset && statement.getUpdateCount() >= -1) {
					isResultset = statement.getMoreResults();
				}
				ResultSet result = statement.getResultSet();

				long duration = (System.currentTimeMillis() - now);
				logger.info("finished SQLQuery#" + queryNum +" jobId "+jobId + " on worker " + this.workerId+  " method=executeQuery" + " duration=" + duration
						+ " error=false status=done driver=" + connection.getMetaData().getDatabaseProductName() +" queryid=" + queryNum
						+ " task=" + this.getClass().getName());
				SQLStats queryLog = new SQLStats(Integer.toString(queryNum), "executeQuery", sql, duration,
						connection.getMetaData().getDatabaseProductName());
				queryLog.setError(false);
				PerfDB.INSTANCE.save(queryLog);

				ExecutionItem ex = new ExecutionItem(ds.getDatabase(), ds.getDatasource(), connection, result,
						formatter, queryNum);
				ex.setExecutionDate(start);
				return ex;
			} catch (Exception e) {
				// ticket:2972
				// it is our responsibility to dispose connection and statement
				if (needCommit) {
					connection.rollback();
				}
				if (statement != null){
					statement.close();				
					statement = null;
				}
				if (connection != null) {
					if (!connection.getMetaData().getDatabaseProductName().equals("Spark SQL")) {
						connection.close();
						ds.getDatasource().releaseSemaphore();
					}
				}
				throw e;
			}
		} catch (Exception e) {
			if (connection != null) {
				if (!connection.getMetaData().getDatabaseProductName().equals("Spark SQL")) {
					connection.close();
					ds.getDatasource().releaseSemaphore();
				}
			}
			long duration = (System.currentTimeMillis() - now);
			logger.error("error SQLQuery#" + queryNum + " jobId "+jobId + " on worker " + this.workerId
					+ " method=executeQuery" + " duration=" + duration
					+ " status=error queryid=" + queryNum + " task=" + this.getClass().getName() 
					+ " error="+ e.toString());
			SQLStats queryLog = new SQLStats(Integer.toString(queryNum), "executeQuery", sql, duration,
					ds.getConfig().getJdbcUrl());
			queryLog.setError(true);
			PerfDB.INSTANCE.save(queryLog);
			throw new ExecutionException("SQLQuery#" + queryNum + "  jobId "+jobId + " on worker " + this.workerId
					+  " failed: " + e.getLocalizedMessage()
					+ "\nwhile executing the following SQL query:\n" + sql, e);
		} finally {

			// unregister
			ExecutionManager.INSTANCE.unregisterTask(this);
		}
	}

}
