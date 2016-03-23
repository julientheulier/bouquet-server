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
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.squid.kraken.v4.api.core.PerfDB;
import com.squid.kraken.v4.api.core.SQLStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.concurrent.ExecutionManager;
import com.squid.core.database.impl.DataSourceReliable.FeatureSupport;
import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.database.impl.DriverLoader;
import com.squid.core.database.impl.DriverShim;
import com.squid.core.database.model.Column;
import com.squid.core.database.model.Database;
import com.squid.core.database.model.Schema;
import com.squid.core.database.model.Table;
import com.squid.core.database.model.impl.DatabaseManager;
import com.squid.core.database.model.impl.ExecuteQueryTask;
import com.squid.core.database.statistics.IDatabaseStatistics;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.jdbc.engine.ExecutionItem;
import com.squid.core.jdbc.engine.IExecutionItem;
import com.squid.core.jdbc.formatter.IJDBCDataFormatter;
import com.squid.core.sql.render.ISkinFeatureSupport;
import com.squid.kraken.v4.core.expression.visitor.ExtractColumns;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;

public class DatabaseServiceImpl implements DatabaseService {

	static final Logger logger = LoggerFactory
			.getLogger(DatabaseServiceImpl.class);

	public static final DatabaseServiceImpl INSTANCE = initINSTANCE();

	public static final AtomicInteger queryCnt = new AtomicInteger();

	public static final String PLUGIN_DIR = new String(System.getProperty("kraken.plugin.dir", "/home/squid/drivers"));

	private ConcurrentHashMap<ProjectPK, Future<DatasourceDefinition>> customerAccess = new ConcurrentHashMap<ProjectPK, Future<DatasourceDefinition>>();

	// Need to have the servlet directory or configs
	//Accessed by QueryWorkers too;
	public synchronized static void initDriver(){

		ClassLoader classloader = Thread.currentThread().getContextClassLoader();

		String PathToPlugins = PLUGIN_DIR;
		if(PathToPlugins !=null){
			DriverLoader dd = new DriverLoader(PathToPlugins);
		
		
			// Will detect automatically JDBC 4 Compliant;
			// Drill and Spark JDBC plugins are not.
			ServiceLoader<java.sql.Driver> loader = ServiceLoader.load(java.sql.Driver.class,dd);
		    Iterator<Driver> driverIt = loader.iterator();
		    //LoggerFactory.getLogger(this.getClass()).debug("List of vendorSupport Providers");
		    while(driverIt.hasNext()){
				try {
					Driver driver = driverIt.next();
					Enumeration<Driver> availableDrivers = DriverManager.getDrivers();
					Boolean duplicate = false;
					while(availableDrivers.hasMoreElements()){
						Driver already = availableDrivers.nextElement();
						if(already instanceof DriverShim){
							if(((DriverShim) already).getName() == driver.getClass().getName()){
								duplicate = true;
							}
						}
					}
					if(logger.isDebugEnabled()){logger.debug("driver available "+driver.getClass());};
					if(!duplicate){DriverManager.registerDriver(new DriverShim(driver));};
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    }
		    
		    // load registered drivers
	    	for (String driverName : drivers) {
	    		try {
		    		Driver driver = (Driver)Class.forName(driverName, true, dd).newInstance();
		    		Enumeration<Driver> availableDrivers = DriverManager.getDrivers();
					Boolean duplicate = false;
					while(availableDrivers.hasMoreElements()){
						Driver already = availableDrivers.nextElement();
						if(already instanceof DriverShim){
							if(((DriverShim) already).getName() == driver.getClass().getName()){
								duplicate = true;
							}
						}
					}
					logger.info("driver available "+driver.getClass());
					if(!duplicate){DriverManager.registerDriver(new DriverShim(driver));};
	    		} catch (Throwable	e){
	    			logger.warn("Cannot find the jdbc driver for "+driverName);
	    		}
	    	}
		}
		Thread.currentThread().setContextClassLoader(classloader);

		
		
	}

	
	// Without servlet directory or configs
	public synchronized static void initDriver(String path){

		String PathToPlugins = path;
		DriverLoader dd = new DriverLoader(PathToPlugins);
		
		// Will detect automatically JDBC 4 Compliant;
		// Drill and Spark JDBC plugins are not.
		ServiceLoader<java.sql.Driver> loader = ServiceLoader.load(java.sql.Driver.class,dd);
	    Iterator<Driver> driverIt = loader.iterator();
	    //LoggerFactory.getLogger(this.getClass()).debug("List of vendorSupport Providers");
	    while(driverIt.hasNext()){
			try {
				Driver driver = driverIt.next();
				Enumeration<Driver> availableDrivers = DriverManager.getDrivers();
				Boolean duplicate = false;
				while(availableDrivers.hasMoreElements()){
					Driver already = availableDrivers.nextElement();
					if(already instanceof DriverShim){
						if(((DriverShim) already).getName() == driver.getClass().getName()){
							duplicate = true;
						}
					}
				}
				if(logger.isDebugEnabled()){logger.debug("driver available "+driver.getClass());};
				if(!duplicate){DriverManager.registerDriver(new DriverShim(driver));};
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    
	    try {
	    	for (String driverName : drivers) {
	    		Driver driver = (Driver)Class.forName(driverName, true, dd).newInstance();
	    		Enumeration<Driver> availableDrivers = DriverManager.getDrivers();
				Boolean duplicate = false;
				while(availableDrivers.hasMoreElements()){
					Driver already = availableDrivers.nextElement();
					if(already instanceof DriverShim){
						if(((DriverShim) already).getName() == driver.getClass().getName()){
							duplicate = true;
						}
					}
				}
				if(logger.isDebugEnabled()){logger.debug("driver available "+driver.getClass());};
				if(!duplicate){DriverManager.registerDriver(new DriverShim(driver));};
	    	}
	    	
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    Thread.currentThread().setContextClassLoader(dd);

		
		
	}
	
	@SuppressWarnings("unused")
	@Deprecated
	private void loadDrivers() {
		// ClassForName is not useful anymore.
		// load the drivers
		for (String driver : drivers) {
			try {
				if (logger.isDebugEnabled()) {
					logger.debug("Loading driver " + driver);
				}
				Class.forName(driver);
			} catch (ClassNotFoundException e) {
				logger.error("unable to load driver " + driver);
			}
		}
		//
	}
	
	private static DatabaseServiceImpl initINSTANCE() {
		initDriver(); //Side effect; Modify Thread classloader
		return new DatabaseServiceImpl();
	}
	
	@Deprecated
	public void reloadDrivers(){
		// load the drivers
				for (String driver : drivers) {
					try {
						if(logger.isDebugEnabled()){
							logger.debug("Loading driver " + driver);
						}
						Class.forName(driver);
					} catch (ClassNotFoundException e) {
						logger.error("unable to load driver " + driver);
					}
				}
	}

	/**
	 * return the DatasourceDefinition for the project or raise an DatabaseServiceException if cannot instanciate one
	 * @param project
	 * @return
	 * @throws DatabaseServiceException
	 */
	public DatasourceDefinition getDatasourceDefinition(Project project)
			throws DatabaseServiceException {
		Future<DatasourceDefinition> mapping = customerAccess.get(project.getId());
		if (mapping == null) {
			synchronized (customerAccess) {
				// double check
				mapping = customerAccess.get(project.getId());
				if (mapping==null) {
					mapping = openDatabase(project);// it will use a future to avoid
													// locking too long (involve a DB
													// access, don't want to timeout...)
				}
			}
		}
		if (mapping != null) {
			try {
				return mapping.get();
			} catch (Exception e) {
				if (e.getCause() instanceof DatabaseServiceException) {
					throw (DatabaseServiceException) e.getCause();
				} else {
					throw new DatabaseServiceException(
							"An error occured while initializing database for the project '"
									+ project.getName() + "': "
									+ e.getMessage(), e);
				}
			}
		} else {
			throw new DatabaseServiceException(
					"Unable to setup a database for the project '"
							+ project.getName() + "'");
		}
	}

	/**
	 * return the database associated with the project. If it is the first call,
	 * it will try to initialize the database and the underlying connection. It
	 * always return a valid database or throws an exception
	 * 
	 * @return the database (never NULL)
	 * @throws a
	 *             DatabaseException if cannot get a valid database (we must be
	 *             able to connect)
	 */
	@Override
	public Database getDatabase(Project project)
			throws DatabaseServiceException {
		return getDatasourceDefinition(project).getDatabase();
	}
	
	private ExecutorService openDatabaseService = Executors.newCachedThreadPool();

	/**
	 * init the database object connected with the project
	 * 
	 * @param project
	 * @return
	 * @throws DatabaseServiceException
	 */
	private Future<DatasourceDefinition> openDatabase(final Project project) {
		Future<DatasourceDefinition> future = customerAccess.get(project
				.getId());// double check
		if (future == null) {
			future = openDatabaseService.submit(
					new Callable<DatasourceDefinition>() {
						@Override
						public DatasourceDefinition call() throws Exception {
							try {
								return new DatasourceDefinition(
										project);
							} catch (DatabaseServiceException e) {
								// reset the cache
								customerAccess.remove(project.getId());
								throw e;
							}
						}
					});
			customerAccess.put(project.getId(), future);
		}
		return future;
	}

	/**
	 * shutdown all running jobs, for every customers
	 */
    public void shutdownJobsExecutor() {
    	synchronized (this) {
    		openDatabaseService.shutdown();
    		try {
				openDatabaseService.awaitTermination(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
	    		openDatabaseService.shutdown();
			}
    		for (Future<DatasourceDefinition> future : customerAccess.values()) {
    			if (future.isDone()) {
    				try {
						future.get().close();
					} catch (InterruptedException | ExecutionException e) {
						//
					}
    			} else if (future.isCancelled()) {
    				// ok ignore
    			} else {
    				future.cancel(true);
    			}
    		}
		}
    }

	/**
	 * get a list of the Tables available for the project
	 * 
	 * @param projectPK
	 * @return
	 */
	public List<Table> getTables(Project project)
			throws ExecutionException {
		List<Table> result = new ArrayList<Table>();
		List<Schema> schemas = getAuthorizedShemas(project);
		for (Schema schema : schemas) {
			result.addAll(schema.getTables());
		}
		return result;
	}

	/**
	 * List the Schemas from the database.
	 * 
	 * @param project
	 * @return
	 * @throws DatabaseServiceException
	 */
	public List<Schema> getSchemas(Project project)
			throws DatabaseServiceException {
		DatasourceDefinition ds = getDatasourceDefinition(project);
		return ds.getDatabase().getSchemas();
	}

	/**
	 * List the authorized Schemas for the project.
	 * 
	 * @param project
	 * @return
	 * @throws DatabaseServiceException
	 */
	public List<Schema> getAuthorizedShemas(Project project)
			throws DatabaseServiceException {
		DatasourceDefinition ds = getDatasourceDefinition(project);
		return ds.getAccess();
	}

	/**
	 * lookup a table reference in the Project scope.
	 * 
	 * @param projectPK
	 *            - the project ID
	 * @param tableReference
	 *            - the table reference: can be just the table name or prefixed
	 *            like schema:tablename
	 * @return the table
	 * @throws DatabaseServiceException if database not available
	 * @throws ScopeException if the table reference cannot be resolve
	 *
	 */
	public Table lookupTable(Project project, String tableReference)
			throws ScopeException, ExecutionException {
		// check the project access
		List<Schema> schemas = getAuthorizedShemas(project);
		if (schemas==null || schemas.isEmpty()) {
			throw new ScopeException("cannot lookup table '"+tableReference+"' because there is no authorized schema defined in the Project");
		}
		// parse the table reference
		int separator = tableReference.indexOf(':');
		Schema schema = null;
		if (separator < 0) {
			// look in any schema
		} else if (separator == 0) {
			// public schema ?
			schema = find(schemas, "");
		} else {
			String schemaName = tableReference.substring(0, separator);
			schema = find(schemas, schemaName);
		}
		if (schema != null) {
			String tableName = tableReference.substring(separator + 1,
					tableReference.length());
			Table result = schema.findTable(tableName);
			if (result==null) {
				throw new ScopeException("cannot lookup table '"+tableName+"' in schema '"+schema.getName()+"'");
			} else {
				return result;
			}
		} else {
			Table singleton = null;
			for (Schema loop : schemas) {
				Table table = loop.findTable(tableReference);
				if (table != null) {
					if (singleton != null) {
						// ambiguous name...
						throw new ScopeException("cannot lookup table '"+tableReference+"' because it is ambiguous:"
								+ "add a schema prefix, e.g: '"
								+ singleton.getSchema().getName() + ":"
								+ tableReference + " or '"
								+ table.getSchema().getName() + ":"
								+ tableReference + "'");
					} else {
						singleton = table;
					}
				}
			}
			if (singleton==null) {
				ArrayList<String> names = new ArrayList<String>(schemas.size());
				for (Schema s : schemas) {
					names.add(s.getName());
				}
				throw new ScopeException("cannot lookup table '"+tableReference+"' in schemas "+names);
			} else {
				return singleton;
			}
		}
	}

	private Schema find(List<Schema> schemas, String name) {
		for (Schema schema : schemas) {
			if (schema.getName().equals(name)) {
				return schema;
			}
		}
		// else
		return null;
	}
	
	public ExecuteQueryTask executeQueryTask(DatabaseManager ds, String sql) {
        int queryNum = queryCnt.incrementAndGet();
        return new ExecuteQueryTask(ds, queryNum, sql);
	}

	@Override
	public IExecutionItem executeQuery(DatabaseManager ds, String sql)
			throws ExecutionException {
		int queryNum = queryCnt.incrementAndGet();
		long now = System.currentTimeMillis();
		try {
			boolean needCommit = false;
			Connection connection = ds.getDatasource().getConnectionBlocking();
			Statement statement = connection.createStatement();
			try {
				IJDBCDataFormatter formatter = ds.getDataFormatter(connection);
				logger.info("running SQLQuery#" + queryNum + " on " + ds.getDatabase().getUrl()
						+ ":\n" + sql +"\nHashcode="+sql.hashCode());
				// make sure auto commit is false (for cursor based ResultSets and postgresql)
				if(ds.getSkin().getFeatureSupport(FeatureSupport.AUTOCOMMIT) == ISkinFeatureSupport.IS_NOT_SUPPORTED){
                	connection.setAutoCommit(false);
					needCommit = true;
                }else{
                	connection.setAutoCommit(true);
                }
				statement.setFetchSize(formatter.getFetchSize());
				Date start = new Date();
				//ResultSet result = statement.executeQuery(sql);
				boolean isResultset = statement.execute(sql);
				while (!isResultset && statement.getUpdateCount() >= -1) {
					isResultset = statement.getMoreResults();
				}
				ResultSet result = statement.getResultSet();
				
				
				/*logger.info("SQLQuery#" + queryNum + " executed in "
						+ (System.currentTimeMillis() - now) + " ms.");*/
				long duration = (System.currentTimeMillis() - now);
    			logger.info("task="+this.getClass().getName()+" method=executeQuery"+" duration="+ duration+" error=false status=done");
				//TODO get project instead of database
				SQLStats queryLog = new SQLStats(Integer.toString(queryNum), "executeQuery",sql, duration, ds.getDatabase().getProductName());
				queryLog.setError(false);
				PerfDB.INSTANCE.save(queryLog);

				ExecutionItem ex = new ExecutionItem(ds.getDatabase(), ds.getDatasource(), connection, result, formatter, queryNum);
				ex.setExecutionDate(start);
				return ex;
			} catch (Exception e) {
				// ticket:2972
				// it is our responsibility to dispose connection and statement
				if (needCommit) {
					connection.rollback();
				}
				if (statement!=null) statement.close();
				if (connection!=null) {
					connection.close();
					ds.getDatasource().releaseSemaphore();
				}
				throw e;
			}
		} catch (Exception e) {
			logger.info(e.toString());
			long duration = (System.currentTimeMillis() - now);
			/*logger.info("SQLQuery#" + queryNum + " failed in "
					+ (System.currentTimeMillis() - now) + " ms.");*/
			logger.info("task="+this.getClass().getName()+" method=executeQuery"+" duration="+ duration+" error=true status=done");
			SQLStats queryLog = new SQLStats(Integer.toString(queryNum), "executeQuery",sql, duration, ds.getDatabase().getProductName());
			queryLog.setError(true);
			PerfDB.INSTANCE.save(queryLog);
			throw new ExecutionException("SQLQuery#" + queryNum + " failed:\n"+e.getLocalizedMessage(),e);
		}
	}


	@Override
	public Boolean execute(DatabaseManager ds, String sql)
			throws ExecutionException {
		int queryNum = queryCnt.incrementAndGet();
		long now = System.currentTimeMillis();
		try {
			boolean needCommit = false;
			Connection connection = ds.getDatasource().getConnectionBlocking();
			Statement statement = connection.createStatement();
			try {
				//IJDBCDataFormatter formatter = ds.getDataFormatter(connection);
				logger.info("running SQLQuery#" + queryNum + " on " + ds.getDatabase().getUrl()
						+ ":\n" + sql +"\nHashcode="+sql.hashCode());
				// make sure auto commit is false (for cursor based ResultSets and postgresql)
				if(ds.getSkin().getFeatureSupport(FeatureSupport.AUTOCOMMIT) == ISkinFeatureSupport.IS_NOT_SUPPORTED){
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
				SQLStats queryLog = new SQLStats(Integer.toString(queryNum), "execute",sql, duration, ds.getDatabase().getProductName());
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
					ds.getDatasource().releaseSemaphore();
				}
			}
		} catch (Exception e) {
			logger.info(e.toString());
			long duration = (System.currentTimeMillis() - now);
			/*logger.info("SQLQuery#" + queryNum + " failed in "
					+ (System.currentTimeMillis() - now) + " ms.");*/
			logger.info("task="+this.getClass().getName()+" method=execute"+" duration="+ duration+" error=true status=done");
			SQLStats queryLog = new SQLStats(Integer.toString(queryNum), "execute",sql, duration, ds.getDatabase().getProductName());
			queryLog.setError(true);
			PerfDB.INSTANCE.save(queryLog);
			throw new ExecutionException("SQLQuery#" + queryNum + " failed:\n"+e.getLocalizedMessage(),e);
		}
	}


	/**
	 * invalidate cached data for the project
	 * 
	 * @param project
	 */
	public boolean invalidate(final Project project, boolean force) {
		ProjectPK projectID = project.getId();
		Future<DatasourceDefinition> future = customerAccess.get(projectID);
		if (future != null) {
			DatasourceDefinition old = null;
			try {
				old = future.get();
			} catch (Exception e) {
				// if the init failed, create a new one
			}
			if (old == null || force || !old.checkValide(project)) {
				// create a new datasourceDefinition
				future = ExecutionManager.INSTANCE.submit(
						projectID.getCustomerId(),
						new Callable<DatasourceDefinition>() {
							@Override
							public DatasourceDefinition call()
									throws Exception {
								try {
									DatasourceDefinition def = new DatasourceDefinition(
											project);
									return def;
								} catch (DatabaseServiceException e) {
									// reset the cache
									customerAccess.remove(project.getId());
									throw e;
								}
							}
						});
				customerAccess.put(projectID, future);// replace with the
														// new future
														// definition
				if (old!=null) {
					old.getDatabase().setStale();// now we can invalidate the database
					old.close();
				}
				return true;
			} else {
				old.refreshProject();
				return false;
			}
		} else {
			return true;
		}
	}
	
	/**
	 * return the statistics for the expression based on the database internal statistics; return -1 if no stats are available or unknown for that expression
	 * @param project
	 * @param expr
	 * @return
	 * @throws ScopeException
	 * @throws DatabaseServiceException
	 */
	public float computeStatistics(Project project, ExpressionAST expr) throws ScopeException, ExecutionException {
		DatasourceDefinition ds = getDatasourceDefinition(project);
		IDatabaseStatistics stats = ds.getStatistics();
		if (stats!=null) {
			ExtractColumns visitor = new ExtractColumns();
			List<Column> columns = visitor.apply(expr);
			return DatabaseServiceImpl.INSTANCE.getStatisticsMax(stats,columns);
		} else {
			return -1;
		}
	}

	/**
	 * return statistics for column, applying MAX to individual statistic
	 * @param columns
	 * @return
	 * @throws ExecutionException 
	 */
	protected float getStatisticsMax(IDatabaseStatistics stats, List<Column> columns) throws ExecutionException {
	    if (columns.isEmpty()) {
	        return 1;// the expression is a constant
	    }
		float estimate = -1;
		for (Column column : columns) {
			float x = stats.getSize(column);
			estimate = x>estimate?x:estimate;
		}
		return estimate;
	}

}
