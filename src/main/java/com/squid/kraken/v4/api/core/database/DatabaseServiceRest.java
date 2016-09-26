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
package com.squid.kraken.v4.api.core.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.database.impl.DataSourceReliable;
import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.database.model.Database;
import com.squid.core.database.model.Schema;
import com.squid.core.database.model.Table;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.BaseServiceRest;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.api.core.project.ProjectServiceBaseImpl;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.database.impl.DatabaseServiceImpl;
import com.squid.kraken.v4.core.database.impl.DatasourceDefinition;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AppContext;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;

@Api(hidden=false, value = "database", authorizations = { @Authorization(value = "kraken_auth", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
@Produces({ MediaType.APPLICATION_JSON })
public class DatabaseServiceRest extends BaseServiceRest {
	
	static final Logger logger = LoggerFactory.getLogger(DatabaseServiceRest.class);

	private static final String SCHEMA_NAME = "schemaName";
	private static final String TABLE_NAME = "tableName";

	public DatabaseServiceRest(AppContext userContext) {
		super(userContext);
	}
	
	@GET
	@Path("")
	@ApiOperation(value = "Get Database Status and vendor information")
	public DatabaseInfo getDatabaseStatus(@PathParam("projectId") String projectId) throws ExecutionException {
		ProjectPK projectPK = new ProjectPK(userContext.getCustomerId(), projectId);
		Project project = ProjectServiceBaseImpl.getInstance().read(userContext, projectPK);
		AccessRightsUtils.getInstance().checkRole(userContext, project, Role.WRITE);
		DatasourceDefinition dd = DatabaseServiceImpl.INSTANCE.getDatasourceDefinition(project);
		DataSourceReliable ds = dd.getDBManager().getDatasource(); //Appropriate driver should be set already.
		Connection conn = null;
		try {
			conn = ds.getConnectionBlocking();
			conn.close();
			ds.releaseSemaphore();
			return new DatabaseInfo(dd);
		} catch (SQLException e) {
			try {
				conn.close();
				ds.releaseSemaphore();
			} catch (SQLException e1) {
				logger.error("Unable to close Connection");
				throw new ExecutionException(e1);
			}
			throw new ExecutionException(e);
		}
	}
	
	@GET
	@Path("schemas/")
	@ApiOperation(value = "list the database schemas")
	public List<?> readSchemas(@PathParam("projectId") String projectId) throws DatabaseServiceException, SQLScopeException {
		ProjectPK projectPK = new ProjectPK(userContext.getCustomerId(), projectId);
		Project project = ProjectServiceBaseImpl.getInstance().read(userContext, projectPK);
		AccessRightsUtils.getInstance().checkRole(userContext, project, Role.WRITE);
		Database database = DatabaseServiceImpl.INSTANCE.getDatabase(project);
		List<String> result = new ArrayList<String>();
		for (Schema schema : database.getSchemas()) {
			result.add(schema.getName());
		}
		return result;
	}
	
	@GET
	@Path("schemas/{"+SCHEMA_NAME+"}")
	@ApiOperation(value = "list the schema's tables")
	public List<?> readSchema(@PathParam("projectId") String projectId, @PathParam(SCHEMA_NAME) String schemaName) throws ExecutionException, SQLScopeException {
		ProjectPK projectPK = new ProjectPK(userContext.getCustomerId(), projectId);
		Project project = ProjectServiceBaseImpl.getInstance().read(userContext, projectPK);
		AccessRightsUtils.getInstance().checkRole(userContext, project, Role.WRITE);
		Database database = DatabaseServiceImpl.INSTANCE.getDatabase(project);
		Schema schema = database.findSchema(schemaName);
		if (schema==null) {
			throw new ObjectNotFoundAPIException("cannot lookup schema '"+schemaName+"'", true);
		} else {
			List<String> result = new ArrayList<String>();
			for (Table table : schema.getTables()) {
				result.add(table.toString());
			}
			return result;
		}
	}
	
	@GET
	@Path("schemas/{"+SCHEMA_NAME+"}/tables/{"+TABLE_NAME+"}")
	@ApiOperation(value = "get the table definition")
	public Object readTable(
			@PathParam("projectId") String projectId,
			@PathParam(SCHEMA_NAME) String schemaName,
			@PathParam(TABLE_NAME) String tableName
		) throws ExecutionException, SQLScopeException {
		ProjectPK projectPK = new ProjectPK(userContext.getCustomerId(), projectId);
		Project project = ProjectServiceBaseImpl.getInstance().read(userContext, projectPK);
		AccessRightsUtils.getInstance().checkRole(userContext, project, Role.WRITE);
		Database database = DatabaseServiceImpl.INSTANCE.getDatabase(project);
		Schema schema = database.findSchema(schemaName);
		if (schema==null) {
			throw new ObjectNotFoundAPIException("cannot lookup Schema '"+schemaName+"'", true);
		} else {
			Table table = schema.findTable(tableName);
			if (table==null) {
				throw new ObjectNotFoundAPIException("cannot lookup Table '"+tableName+"'", true);
			} else {
				return table;
			}
		}
	}
	
	@GET
	@Path("schemas/{"+SCHEMA_NAME+"}/tables/{"+TABLE_NAME+"}/refresh")
	@ApiOperation(value = "refresh the table definition. If it is a new table, makes it available. Note that this won't clear the cache.")
	public Object refreshTable(
			@PathParam("projectId") String projectId,
			@PathParam(SCHEMA_NAME) String schemaName,
			@PathParam(TABLE_NAME) String tableName
		) throws ExecutionException, SQLScopeException {
		ProjectPK projectPk = new ProjectPK(userContext.getCustomerId(), projectId);
		Project project = ProjectServiceBaseImpl.getInstance().read(userContext, projectPk);
		AccessRightsUtils.getInstance().checkRole(userContext, project, Role.WRITE);
		Database database = DatabaseServiceImpl.INSTANCE.getDatabase(project);
		Schema schema = database.findSchema(schemaName);
		if (schema==null) {
			throw new ObjectNotFoundAPIException("cannot lookup Schema '"+schemaName+"'", true);
		} else {
			Table before = schema.findTable(tableName);
			// refresh can work either if the table is already known or not
			Table after = database.getEngine().refreshTable(schema, tableName);
			if (before==null && after==null) {
				throw new ObjectNotFoundAPIException("Table '"+tableName+"' does not exist or is not visible", true);
			}
			// refresh the project
			ProjectManager.INSTANCE.refreshContent(projectPk);
			if (before!=null && after==null) {
				throw new ObjectNotFoundAPIException("Table '"+tableName+"' vanished, or it's no more visible", true);
			}
			return after;
		}
	}

}
