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
package com.squid.kraken.v4.caching.awsredis;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import com.squid.kraken.v4.api.core.PerfDB;
import com.squid.kraken.v4.api.core.SQLStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.database.impl.DataSourceReliable;
import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.database.impl.DriverLoader;
import com.squid.core.database.lazy.LazyDatabaseFactory;
import com.squid.core.database.model.Database;
import com.squid.core.database.model.DatabaseFactory;
import com.squid.core.database.model.impl.DatabaseManager;
import com.squid.core.database.model.impl.JDBCConfig;
import com.squid.core.jdbc.engine.ExecutionItem;
import com.squid.core.jdbc.engine.IExecutionItem;
import com.squid.core.jdbc.vendor.VendorSupportRegistry;
import com.squid.kraken.v4.core.database.impl.HikariDataSourceReliable;
import com.zaxxer.hikari.HikariDataSource;
import com.squid.core.database.metadata.IMetadataEngine;


public class SimpleDatabaseManager extends DatabaseManager {

	static final Logger logger = LoggerFactory
			.getLogger(SimpleDatabaseManager.class);

	public static final AtomicInteger queryCnt = new AtomicInteger();
	
	protected String databaseName = "noname";
	protected int maximumPoolSize = 10;
	
	public SimpleDatabaseManager() {
		super();
	}

	public SimpleDatabaseManager(String jdbcURL, String username, String password)
			throws ExecutionException {
		super();
		this.config = new JDBCConfig(jdbcURL, username, password);
		setup();
	}

	protected HikariDataSourceReliable createDatasourceWithConfig(JDBCConfig config) {
		HikariDataSourceReliable ds = new HikariDataSourceReliable(maximumPoolSize);
		ds.setJdbcUrl(config.getJdbcUrl());
		ds.setUsername(config.getUsername());
		ds.setPassword(config.getPassword());
		//
		ds.setValidationTimeout(1001);
		ds.setIdleTimeout(10000); // in ms
		ds.setMaxLifetime(30000);
		//
		// connection timeout is actually a limit on getting a connection from the pool
		// ... but since now we are only trying to acquire a connection when we already know one is available,
		// ... it is safe to use a smaller value
		ds.setConnectionTimeout(4001);
		try {
            ds.setLoginTimeout(1001);
        } catch (SQLException e) {
            e.printStackTrace();
        }
		return ds;
	}

	protected void chooseDriver(HikariDataSource ds)
			throws DatabaseServiceException { // T117
        ds.setCustomClassloader(new DriverLoader());
		if (ds.getJdbcUrl().contains("jdbc:postgresql")) {
			ds.setDriverClassName("org.postgresql.Driver"); //So for redshift we are also using postgresql to detect the version. DRIVER IS CHANGED LATER.
		} else if (ds.getJdbcUrl().contains("jdbc:drill")) {
			try {
				Class.forName("org.apache.drill.jdbc.Driver");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				throw new DatabaseServiceException(
						"Could not load driver for drill");
			}
			ds.setDriverClassName("org.apache.drill.jdbc.Driver");
		} else if (ds.getJdbcUrl().contains("jdbc:hive2")) {
			ds.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
			ds.setAutoCommit(true);
			// Connection.isValid() method is not supported
			ds.setConnectionTestQuery("show databases");
		}
	}

	protected void setup() throws ExecutionException {
		HikariDataSourceReliable hikari = setupDataSource();
		this.ds = hikari;
		this.db = setupDatabase();
		setupFinalize(hikari, db);
	}

	protected HikariDataSourceReliable setupDataSource() throws DatabaseServiceException {
		HikariDataSourceReliable ds = createDatasourceWithConfig(config);
		chooseDriver(ds);

		// check the connection
		try {
			Connection conn = ds.getConnectionBlocking();
			try {
				conn.getMetaData().getDatabaseProductName();
				if (logger.isDebugEnabled()) {
					logger.debug("Driver used: "
							+ conn.getMetaData().getDriverName());
				}
			} finally {
				conn.close();
				ds.releaseSemaphore();
			}
		} catch (SQLException e) {
			throw new DatabaseServiceException("unable to connect to "
					+ config.getJdbcUrl() + ": \n" + e.getLocalizedMessage(), e);
		}
		return ds;
	}

	protected Database setupDatabase() throws ExecutionException {
		DatabaseFactory df = new LazyDatabaseFactory(this);
		Database db = df.createDatabase();
		db.setName(databaseName);
		db.setUrl(config.getJdbcUrl());
		//
		// setup vendor support
		this.vendor = VendorSupportRegistry.INSTANCE.getVendorSupport(db);
		this.stats = this.vendor.createDatabaseStatistics(ds);
		//
		return db;
	}
	
	protected void setupFinalize(HikariDataSourceReliable hikari, Database db) {
		// Now that we have fully detected the database type we can change to most accurate driver.
		if (db.getProductName().equals(IMetadataEngine.REDSHIFT_NAME)){
			hikari.setDriverClassName("com.amazon.redshift.jdbc41.Driver");
		}
		if (db.getUrl().contains("jdbc:drill")) {
			//Do nothing on purpose setAutocommit is/was not well supported.
		}else if (DataSourceReliable.FeatureSupport.IS_SUPPORTED == getSkin().getFeatureSupport(DataSourceReliable.FeatureSupport.AUTOCOMMIT)) {
			hikari.setAutoCommit(true);
		}else if(DataSourceReliable.FeatureSupport.IS_NOT_SUPPORTED == getSkin().getFeatureSupport(DataSourceReliable.FeatureSupport.AUTOCOMMIT)){
			hikari.setAutoCommit(false);
		}
	}

	public IExecutionItem executeQuery(String sql)
			throws ExecutionException {
		int queryNum = queryCnt.incrementAndGet();
		long now = System.currentTimeMillis();
		try {
			Connection connection = this.ds.getConnectionBlocking();
			if (logger.isDebugEnabled()) {
				logger.debug("Driver used for the connection", connection
						.getMetaData().getDriverName());
			}
			Statement statement = connection.createStatement();
			try {
				statement.setFetchSize(getDataFormatter(connection).getFetchSize());
				logger.info("running SQLQuery#" + queryNum + " on " + config.getJdbcUrl()
						+ ":\n" + sql +"\nHashcode="+sql.hashCode());
				Date start = new Date();
				boolean isResultset = statement.execute(sql);
				while (!isResultset && statement.getUpdateCount() >= -1) {
					isResultset = statement.getMoreResults();
				}
				ResultSet result = statement.getResultSet();
				/*
				 * logger.info("SQLQuery#" + queryNum + " executed in " +
				 * (System.currentTimeMillis() - now) + " ms.");
				 */
				double duration = (System.currentTimeMillis() - now);
				logger.info("task=" + this.getClass().getName()
						+ " method=executeQuery" + " duration=" + duration
						+ " error=false status=done driver="
						+ connection.getMetaData().getDatabaseProductName()
						+ " queryid=" + queryNum);
				SQLStats queryLog = new SQLStats(Integer.toString(queryNum), "executeQuery", sql, duration, connection.getMetaData().getDatabaseProductName());
				queryLog.setError(false);
				PerfDB.INSTANCE.save(queryLog);
				ExecutionItem ex = new ExecutionItem(db, ds, connection, result,
						getDataFormatter(connection), queryNum);
				ex.setExecutionDate(start);
				return ex;
			} catch (Exception e) {
				// ticket:2972
				// it is our responsibility to dispose connection and statement
				if (statement != null)
					statement.close();
				if (!connection.getMetaData().getDatabaseProductName()
						.equals("Spark SQL")) {
					if (connection != null) {
						connection.close();
						ds.releaseSemaphore();
					}
				}
				throw e;
			}
		} catch (Exception e) {
			long duration = (System.currentTimeMillis() - now);
			logger.error("task=" + this.getClass().getName()
					+ " method=executeQuery" + " duration="
					+ duration
					+ " error=true status=done queryid=" + queryNum);
			SQLStats queryLog = new SQLStats(Integer.toString(queryNum), "executeQuery", sql, duration, config.getJdbcUrl());
			queryLog.setError(true);
			PerfDB.INSTANCE.save(queryLog);
			logger.error("SQLQuery#" + queryNum + " failed: "+e.getLocalizedMessage() + "\nwhile executing the following SQL query:\n" + sql);
			throw new ExecutionException("SQLQuery#" + queryNum + " failed: "+e.getLocalizedMessage() + "\nwhile executing the following SQL query:\n" + sql, e);
		}
	}

}
