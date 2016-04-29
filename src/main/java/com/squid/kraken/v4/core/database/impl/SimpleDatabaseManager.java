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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.database.impl.DataSourceReliable;
import com.squid.core.database.impl.DataSourceReliable.FeatureSupport;
import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.database.impl.DriverLoader;
import com.squid.core.database.lazy.LazyDatabaseFactory;
import com.squid.core.database.metadata.IMetadataEngine;
import com.squid.core.database.model.Database;
import com.squid.core.database.model.DatabaseFactory;
import com.squid.core.database.model.impl.DatabaseManager;
import com.squid.core.database.model.impl.JDBCConfig;
import com.squid.core.jdbc.vendor.VendorSupportRegistry;
import com.squid.core.sql.render.ISkinFeatureSupport;
import com.squid.kraken.v4.api.core.PerfDB;
import com.squid.kraken.v4.api.core.SQLStats;
import com.zaxxer.hikari.HikariDataSource;


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
		this.setupConfig(jdbcURL, username, password);
		setup();
	}
	
	public JDBCConfig getConf(){
		return this.config;
	}

	protected HikariDataSourceReliable createDatasourceWithConfig(JDBCConfig config) {
		HikariDataSourceReliable ds = new HikariDataSourceReliable(maximumPoolSize);
		ds.setJdbcUrl(config.getJdbcUrl());
		ds.setUsername(config.getUsername());
		ds.setPassword(config.getPassword());
		//
		ds.setValidationTimeout(1001);
		ds.setIdleTimeout(60000); // in ms
		ds.setMaxLifetime(120000);
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

	public void setup() throws ExecutionException {
		HikariDataSourceReliable hikari = setupDataSource();
		this.ds = hikari;
		setupDatabase();
		setupFinalize(hikari, db);
	}

	public void setupConfig(String jdbcURL, String username, String password){
		this.config= new JDBCConfig(jdbcURL, username, password);
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

	public Database setupDatabase() throws ExecutionException {
		DatabaseFactory df = new LazyDatabaseFactory(this);
		Database newDb = df.createDatabase();
		newDb.setName(databaseName);
		newDb.setUrl(config.getJdbcUrl());
		//
		// setup vendor support
		this.vendor = VendorSupportRegistry.INSTANCE.getVendorSupport(newDb);
		this.stats = this.vendor.createDatabaseStatistics(ds);
		//
		this.db = newDb;
		return newDb;
	}
	
	public void setDatabaseName(String dbName){
		this.databaseName = dbName;		
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
	
	public ExecuteQueryTask createExecuteQueryTask(String sql){
		  int queryNum = queryCnt.incrementAndGet();
	        return new ExecuteQueryTask(this, queryNum, sql);
	}


	
	public Boolean execute(String sql)
			throws ExecutionException {
		int queryNum = queryCnt.incrementAndGet();
		long now = System.currentTimeMillis();
		try {
			boolean needCommit = false;
			Connection connection = this.getDatasource().getConnectionBlocking();
			Statement statement = connection.createStatement();
			try {
				//IJDBCDataFormatter formatter = ds.getDataFormatter(connection);
				logger.info("running SQLQuery#" + queryNum + " on " + this.getDatabase().getUrl()
						+ ":\n" + sql +"\nHashcode="+sql.hashCode());
				// make sure auto commit is false (for cursor based ResultSets and postgresql)
				if(this.getSkin().getFeatureSupport(FeatureSupport.AUTOCOMMIT) == ISkinFeatureSupport.IS_NOT_SUPPORTED){
					connection.setAutoCommit(false);
					needCommit = true;
				}else{
					connection.setAutoCommit(true);
				}
				statement.setFetchSize(10000);
				//Date start = new Date();
				Boolean result = statement.execute(sql);
				if (needCommit) {
					connection.commit();
				}
				/*logger.info("SQLQuery#" + queryNum + " executed in "
						+ (System.currentTimeMillis() - now) + " ms.");*/
				long duration = (System.currentTimeMillis() - now);
				logger.info("task="+this.getClass().getName()+" method=execute"+" duration="+ duration+" error=false status=done");
				//TODO get project instead of database
				SQLStats queryLog = new SQLStats(Integer.toString(queryNum), "execute",sql, duration, this.getDatabase().getProductName());
				queryLog.setError(false);
				PerfDB.INSTANCE.save(queryLog);

				return result;
			} catch (Exception e) {
				if (needCommit) {
					connection.rollback();
				}
				throw e;
			} finally {
				// ticket:2972
				// it is our responsibility to dispose connection and statement
				if (statement!=null) statement.close();
				if (connection!=null) {
					connection.close();
					this.getDatasource().releaseSemaphore();
				}
			}
		} catch (Exception e) {
			logger.info(e.toString());
			long duration = (System.currentTimeMillis() - now);
			/*logger.info("SQLQuery#" + queryNum + " failed in "
					+ (System.currentTimeMillis() - now) + " ms.");*/
			logger.info("task="+this.getClass().getName()+" method=execute"+" duration="+ duration+" error=true status=done");
			SQLStats queryLog = new SQLStats(Integer.toString(queryNum), "execute",sql, duration, this.getDatabase().getProductName());
			queryLog.setError(true);
			PerfDB.INSTANCE.save(queryLog);
			throw new ExecutionException("SQLQuery#" + queryNum + " failed:\n"+e.getLocalizedMessage(),e);
		}
	}

	
}
