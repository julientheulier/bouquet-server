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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.database.model.Database;
import com.squid.core.database.model.Schema;
import com.squid.core.database.model.impl.JDBCConfig;
import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.caching.redis.RedisCacheManager;
import com.squid.kraken.v4.caching.redis.SimpleDatabaseManager;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.ProjectDAO;

/**
 * the object store the datasource definition and create the link between the
 * actual DataSource object and the Database object
 */
public class DatasourceDefinition extends SimpleDatabaseManager {

	static final Logger logger = LoggerFactory.getLogger(DatasourceDefinition.class);

	public static final boolean SPARK_FLAG = new Boolean(KrakenConfig.getProperty("feature.spark", "false"));
	
	public static final String SPARK_USER = new String(KrakenConfig.getProperty("feature.spark.spark_user", "hive"));
	public static final String SPARK_PASSWORD = new String(KrakenConfig.getProperty("feature.spark.spark_password", "hive"));
	
	public static final String SPARK_JDBC_URL = new String(KrakenConfig.getProperty("feature.spark.spark_jdbc_url", "jdbc:hive2://192.168.25.28:10000/default"));
	public static final String SPARK_ID = "SPARK";
	public static final String SPARK_PREFIX = SPARK_ID+"_";

	private List<Schema> access = null;

	private ProjectPK projectId = null;
	private String projectGenKey = null;

	public DatasourceDefinition() {
		super();
		this.maximumPoolSize = 10;
	}
	
	public DatasourceDefinition(Project project) throws ExecutionException {
		super();
		this.projectId = project.getId();
		initialize(projectId);
		setup();
	}
	
	/**
	 * refresh the database - this operation will invoke the JDBC connection
	 */
	public void refreshDatabase() {
		Database old = this.db;
		old.setStale();// anyone trying to use it should instead redirect to
						// DatabaseService
		try {
			this.db = setupDatabase();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * refresh project metadata : mainly the access list
	 * 
	 * @param project
	 */
	public void refreshProject() {
		synchronized (this) {
			this.access = null;// this will trigger an update when needed
		}
	}

	private void initialize(ProjectPK projectId) throws ExecutionException {
		//
		// reload the project as the root context
		AppContext rootctx = ServiceUtils.getInstance().getRootUserContext(projectId.getCustomerId());
		Project projectRoot = ((ProjectDAO) DAOFactory.getDAOFactory().getDAO(Project.class)).read(rootctx, projectId)
				.get();
		//
		// record the project genKey
		projectGenKey = RedisCacheManager.getInstance().getKey(projectRoot.getId().toUUID()).getStringKey();
		//
		// setup the datasource
		String jdbcUrl = (projectRoot.getDbUrl() != null ? projectRoot.getDbUrl() : "").trim();
		String username = projectRoot.getDbUser() != null ? projectRoot.getDbUser() : "";
		String password = projectRoot.getDbPassword() != null ? projectRoot.getDbPassword() : "";
		//
		config = new JDBCConfig(jdbcUrl, username, password);
		databaseName = projectRoot.getName();
	}

	/**
	 * check if the datasource is defined for the given project settings
	 * 
	 * @param project
	 * @return
	 */
	public boolean checkValide(Project project) {
		return config.getJdbcUrl().equals(project.getDbUrl()) && config.getUsername().equals(project.getDbUser())
				&& config.getPassword().equals(project.getDbPassword());
	}

	public boolean isStale() {
		String check = RedisCacheManager.getInstance().getKey(projectId.toUUID()).getStringKey();
		if (!check.equals(projectGenKey)) {
			projectGenKey = check;// up to the caller to take action
			return true;
		} else {
			return false;
		}
	}

	public List<Schema> getAccess() throws DatabaseServiceException {
		List<Schema> temp = access;// atomic copy of the current value
		if (temp == null) {
			synchronized (this) {
				if (access == null) {// double check
					access = new ArrayList<Schema>();// always clear, we could
														// be here because the
														// db is stale
					for (String schemaName : getProjectSchemas(projectId)) {

						Schema schema = db.findSchema(schemaName);
						if (schema == null) {
							throw new DatabaseServiceException(
									"the schema '" + schemaName + "' does not exist or is not accessible");
						} else {
							access.add(schema);
						}
					}					
					temp = access;
				}
			}
		}
		return temp;
	}

	private List<String> getProjectSchemas(ProjectPK projectPK) {
		//
		// reload the project as the root context
		AppContext rootctx = ServiceUtils.getInstance().getRootUserContext(projectPK.getCustomerId());
		Project projectRoot = ((ProjectDAO) DAOFactory.getDAOFactory().getDAO(Project.class)).read(rootctx, projectPK)
				.get();
		//
		return projectRoot.getDbSchemas();
	}

}
