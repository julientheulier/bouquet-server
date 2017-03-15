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
package com.squid.kraken.v4.api.core.user;

import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.squid.kraken.v4.api.core.BaseServiceRest;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Customer;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserPK;
import com.squid.kraken.v4.persistence.AppContext;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;

/**
 * {@link User} management service.
 */
@Produces( { MediaType.APPLICATION_JSON })
@Api(value = "users", authorizations = { @Authorization(value = "kraken_auth", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
public class UserServiceRest extends BaseServiceRest {

    private final static String PARAM_NAME = "userId";

    private UserServiceBaseImpl delegate = UserServiceBaseImpl.getInstance();

    public UserServiceRest(AppContext userContext) {
        super(userContext);
    }

    /**
     * Delete an existing {@link User}.<br>
     * Role {@link Role#OWNER} is required on the customer to perform the operation.
     */
    @DELETE
    @Path("{"+PARAM_NAME+"}")
	@ApiOperation(value = "Deletes a User")
    public boolean delete(@PathParam(PARAM_NAME) String userId) {
        return delegate.delete(userContext, new UserPK(userContext.getCustomerId(), userId));
    }

    /**
     * Read an existing {@link User}.<br>
     * Role {@link Role#READ} is required on the {@link Customer} to perform the operation except if the callee is
     * itself the caller.
     */
    @GET
	@Path("{"+PARAM_NAME+"}")
	@ApiOperation(value = "Gets a User")
    public User read(@PathParam(PARAM_NAME) String userId) {
        return delegate.read(userContext, new UserPK(userContext.getCustomerId(), userId));
    }
    
    /**
     * Read all {@link Users}.<br>
     * Requires READ Role on the Customer.
     */
    @GET
	@Path("")
	@ApiOperation(value = "Gets all Users")
    public List<User> readAll() {
        return delegate.readAll(userContext);
    }
    
    /**
     * Create a {@link User}.<br>
     * Role {@link Role#OWNER} on the parent object is required to perform the operation.<br>
     * Enforced rule : A User can be added to a UserGroup only if caller has WRITE right on the UserGroup.
     */
	@POST
	@Path("")
	@ApiOperation(value = "Creates a User")
    public User store(@ApiParam(required = true) User user) {
        return delegate.store(userContext, user);
    }

    /**
     * Create a {@link User}.<br>
     * Role {@link Role#OWNER} on the parent object is required to perform the operation.<br>
     * Enforced rule : A User can be added to a UserGroup only if caller has WRITE right on the UserGroup.
     */
	@POST
	@Path("{"+PARAM_NAME+"}")
	@ApiOperation(value = "Creates a User")
    public User store(@PathParam(PARAM_NAME) String userId, @ApiParam(required = true) User user) {
        return delegate.store(userContext, user);
    }
    
    /**
     * Update an existing {@link User}.<br>
     * Role {@link Role#OWNER} on the parent object is required to perform the operation.<br>
     * If updating and password in User data is null, then the existing password will not be changed.<br>
     * To change a User password, the existing password must be passed concatenated to the new password string and
     * separated by a space, such as "oldpassword newpassword".<br>
     * Enforced rule : A User can be added to a UserGroup only if caller has WRITE right on the UserGroup.
     */
    @PUT
    @Path("{"+PARAM_NAME+"}")
	@ApiOperation(value = "Updates a User")
    public User update(@PathParam(PARAM_NAME) String userId, @ApiParam(required = true) User user) {
        return delegate.store(userContext, user);
    }

}
