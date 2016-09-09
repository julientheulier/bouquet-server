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
package com.squid.kraken.v4.api.core.connection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.database.lazy.LazyDatabaseFactory;
import com.squid.core.database.model.Database;
import com.squid.core.database.model.Schema;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.jdbc.vendor.IVendorSupport;
import com.squid.core.jdbc.vendor.VendorSupportRegistry;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.BaseServiceRest;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.database.impl.DatabaseServiceImpl;
import com.squid.kraken.v4.core.database.impl.SimpleDatabaseManager;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Customer;
import com.squid.kraken.v4.model.ExpressionSuggestion;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;

/**
 * handles connections management
 */
@Api(value = "connections", hidden = true, authorizations = { @Authorization(value = "kraken_auth", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
@Produces({ MediaType.APPLICATION_JSON })
public class ConnectionServiceRest extends BaseServiceRest {
	
	static final Logger logger = LoggerFactory.getLogger(ConnectionServiceRest.class);
	
	public ConnectionServiceRest(AppContext userContext) {
		super(userContext);
		// TODO Auto-generated constructor stub
	}

	/**
	 * validate the JDBC connection parameters (without requiring an actual project), and return a list of schemas or an error
	 */
	@GET
	@Path("/validate")
	@ApiOperation(value = "Validate connection definition and return a list of available schemas as a suggestion")
	public ExpressionSuggestion validate(
			@ApiParam(value = "the JDBC url") @QueryParam("url") String url,
			@ApiParam(value = "the username") @QueryParam("username") String username,
			@ApiParam(value = "the password") @QueryParam("password") String password,
			@ApiParam(value = "the project if already defined") @QueryParam("projectId") String projectId
			) {
		//
		// check user role
		Customer customer = DAOFactory.getDAOFactory().getDAO(Customer.class).readNotNull(userContext, userContext.getCustomerPk());
		AccessRightsUtils.getInstance().checkRole(userContext, customer, Role.WRITE);
 		//
		SimpleDatabaseManager manager = null;
		try {
			boolean useExistingDatabase = false;
			// if the projectId is set, load it
			Project rootProject = checkProject(projectId);
			// if the projectId is provided and no password, try to use the stored password
			if (rootProject!=null) {
				if (url==null && username==null && password==null) {
					// just check
					useExistingDatabase = true;
				} else
				if (url!=null && url.equals(rootProject.getDbUrl()) && username!=null && username.equals(rootProject.getDbUser())) {
					if (password==null || password.length()==0) {
						// ok, we will use the project password,
						// but then it can just look up the database schemas
						useExistingDatabase = true;
					} else if (password.equals(rootProject.getDbPassword())) {
						// same password
						useExistingDatabase = true;
					}
				} else
				if (url==null || username==null || password==null) {
					throw new APIException("invalid request, must specify {url,username,password}", false);
				}
			}
			List<Schema> schemas = null;
			if (useExistingDatabase) {
				// just lookup existing database schemas
				schemas = DatabaseServiceImpl.INSTANCE.getSchemas(rootProject);
			} else {
				manager = new SimpleDatabaseManager(url, username, password);
				LazyDatabaseFactory factory = new LazyDatabaseFactory(manager);
				Database database = factory.createDatabase();
				schemas = database.getSchemas();
			}
	        ExpressionSuggestion result = new ExpressionSuggestion();
	        List<String> schemaNames = new ArrayList<String>();
	        for (Schema schema : schemas) {
	        	if (!schema.isSystem()) {
	        		schemaNames.add(schema.getName());
	        	}
	        }
	        result.setDefinitions(schemaNames);
	        return result;
		} catch (ExecutionException | ScopeException e) {
			throw new APIException(e.getMessage(), e, false);
		} finally {
			if (manager!=null) {
				manager.close();
			}
		}
	}

	/**
	 * validate the JDBC connection parameters (without requiring an actual project), and return a list of schemas or an error
	 */
	@PUT
	@Path("/validate")
	@ApiOperation(value = "Validate connection definition and return a list of available schemas as a suggestion")
	public ConnectionInfo validatePost(
			@ApiParam(value = "the project connection information") Project project
			) {
		//
		// check user role
		Customer customer = DAOFactory.getDAOFactory().getDAO(Customer.class).readNotNull(userContext, userContext.getCustomerPk());
		AccessRightsUtils.getInstance().checkRole(userContext, customer, Role.WRITE);
 		//
		SimpleDatabaseManager manager = null;
		try {
			boolean useExistingDatabase = false;
			String vendorId = project.getDbVendorId();
			// get url, user and pwd
			String url = project.getDbUrl();
			String username = project.getDbUser();
			String password = project.getDbPassword();
			// generate the URL ?
			if (vendorId!=null && project.getDbArguments()!=null && !project.getDbArguments().isEmpty()) {
				IVendorSupport vendor = VendorSupportRegistry.INSTANCE.getVendorSupportByID(vendorId);
				if (vendor==null) {
					throw new IllegalArgumentException("Invalid database vendorId, or this one is not available - check installed plugins in the $DRIVER folder");
				}
				url = vendor.buildJdbcUrl(project.getDbArguments());
			}
			// if the projectId is set, load it
			String projectId = project.getId()!=null?project.getId().getProjectId():null;
			Project checkProject = checkProject(projectId);
			// if the projectId is provided and no password, try to use the stored password
			if (checkProject!=null) {
				if (url==null && username==null && password==null) {
					// just check
					useExistingDatabase = true;
				} else
				if (url!=null && url.equals(checkProject.getDbUrl()) && username!=null && username.equals(checkProject.getDbUser())) {
					if (password==null || password.length()==0) {
						// ok, we will use the project password,
						// but then it can just look up the database schemas
						useExistingDatabase = true;
					} else if (password.equals(checkProject.getDbPassword())) {
						// same password
						useExistingDatabase = true;
					}
				} else
				if (url==null || username==null || password==null) {
					throw new APIException("invalid request, must specify {url,username,password}", false);
				}
			}
			List<Schema> schemas = null;
			if (useExistingDatabase) {
				// just lookup existing database schemas
				schemas = DatabaseServiceImpl.INSTANCE.getSchemas(checkProject);
			} else {
				manager = new SimpleDatabaseManager(url, username, password);
				LazyDatabaseFactory factory = new LazyDatabaseFactory(manager);
				Database database = factory.createDatabase();
				vendorId = manager.getVendor().getVendorId();
				schemas = database.getSchemas();
			}
	        List<String> schemaNames = new ArrayList<String>();
	        for (Schema schema : schemas) {
	        	if (!schema.isSystem()) {
	        		schemaNames.add(schema.getName());
	        	}
	        }
	        return new ConnectionInfo(vendorId, url, schemaNames);
		} catch (ExecutionException | ScopeException e) {
			throw new APIException(e.getMessage(), e, false);
		} finally {
			if (manager!=null) {
				manager.close();
			}
		}
	}

	private Project checkProject(String projectId) throws ScopeException {
		if (projectId!=null && projectId.length()>0) {
			ProjectPK projectPk = new ProjectPK(userContext.getClientId(), projectId);
			// check user access
			ProjectManager.INSTANCE.getProject(userContext, projectPk);
			// now escalate to get the password
			AppContext root = ServiceUtils.getInstance().getRootUserContext(userContext);
			return ProjectManager.INSTANCE.getProject(root, projectPk);
		} else {
			// not defined
			return null;
		}
	}
	
	
}
