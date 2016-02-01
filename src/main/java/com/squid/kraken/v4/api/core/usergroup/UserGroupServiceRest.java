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
package com.squid.kraken.v4.api.core.usergroup;

import java.util.List;
import java.util.Set;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.squid.kraken.v4.api.core.BaseServiceRest;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.UserGroup;
import com.squid.kraken.v4.model.UserGroupPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.Authorization;

/**
 * {@link UserGroup} management service.
 */
@Produces( { MediaType.APPLICATION_JSON })
@Api(hidden = true, value = "usergroups", authorizations = { @Authorization(value = "kraken_auth", type = "oauth2") })
public class UserGroupServiceRest extends BaseServiceRest {

    private final static String PARAM_NAME = "userGroupId";

    private UserGroupServiceBaseImpl delegate = UserGroupServiceBaseImpl.getInstance();

    public UserGroupServiceRest(AppContext userContext) {
        super(userContext);
    }

    /**
     * Delete an existing {@link UserGroup}.
     */
    @DELETE
    @Path("{"+PARAM_NAME+"}")
    @ApiOperation(value = "Deletes a UserGroup")
    public boolean delete(@PathParam(PARAM_NAME) String userGroupId) {
        return delegate.delete(userContext, new UserGroupPK(userContext.getCustomerId(), userGroupId));
    }

    /**
     * Read an existing {@link UserGroup}.
     */
    @GET
    @Path("{"+PARAM_NAME+"}")
    @ApiOperation(value = "Gets a UserGroup")
    public UserGroup read(@PathParam(PARAM_NAME) String userGroupId) {
        return delegate.read(userContext, new UserGroupPK(userContext.getCustomerId(), userGroupId));
    }
    
	/**
	 * Get the list of UserGroup.<br>
	 * Requires READ Role on the Customer.
	 */
	@Path("")
	@GET
	@ApiOperation(value = "Gets All UserGroups")
	public List<UserGroup> readAll() {
		return delegate.readAll(userContext);
	}
	
    /**
     * Create a {@link UserGroup}. (requires WRITE Role on the Customer)
     */
    @POST
    @Path("")
    @ApiOperation(value = "Creates a UserGroup")
    public UserGroup store(@ApiParam(required = true) UserGroup group) {
        return delegate.store(userContext, group);
    }

    /**
     * Create a {@link UserGroup}. (requires WRITE Role on the Customer)
     */
    @POST
    @Path("{"+PARAM_NAME+"}")
    @ApiOperation(value = "Creates a UserGroup")
    public UserGroup store(@PathParam(PARAM_NAME) String userGroupId, @ApiParam(required = true) UserGroup group) {
        return delegate.store(userContext, group);
    }
    
    /**
     * Update an existing {@link UserGroup}. (requires WRITE Role on the Customer)
     */
    @PUT
    @Path("{"+PARAM_NAME+"}")
    @ApiOperation(value = "Updates a UserGroup")
    public UserGroup update(@PathParam(PARAM_NAME) String userGroupId, @ApiParam(required = true) UserGroup group) {
        return delegate.store(userContext, group);
    }

    @Path("{"+PARAM_NAME+"}"+"/access")
    @GET
    public Set<AccessRight> readAccessRights(@PathParam(PARAM_NAME) String objectId) {
        return delegate.readAccessRights(userContext, new UserGroupPK(userContext.getCustomerId(), objectId));
    }
    
    @Path("{"+PARAM_NAME+"}"+"/access")
    @POST
    public Set<AccessRight> storeAccessRights(@PathParam(PARAM_NAME) String objectId,
    		@ApiParam(required = true) Set<AccessRight> accessRights) {
        return delegate.storeAccessRights(userContext, new UserGroupPK(userContext.getCustomerId(), objectId),
                accessRights);
    }

}
