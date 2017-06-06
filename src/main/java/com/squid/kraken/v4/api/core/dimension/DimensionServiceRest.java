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
package com.squid.kraken.v4.api.core.dimension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang.SerializationUtils;

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.BaseServiceRest;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.api.core.attribute.AttributeServiceRest;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.DimensionOption;
import com.squid.kraken.v4.model.DimensionPK;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.ExpressionSuggestion;
import com.squid.kraken.v4.model.ValueType;
import com.squid.kraken.v4.persistence.AppContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;

@Produces({ MediaType.APPLICATION_JSON })
@Api(hidden=false, value = "dimensions", authorizations = { @Authorization(value = "kraken_auth", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
public class DimensionServiceRest extends BaseServiceRest {

	private final static String PARAM_NAME = "dimensionId";

	private DimensionServiceBaseImpl delegate = DimensionServiceBaseImpl
			.getInstance();

	public DimensionServiceRest(AppContext userContext) {
		super(userContext);
	}

	/**
	 * Get all Dimensions for the Domain (including child dimensions).
	 * @throws InterruptedException 
	 * @throws ComputingException 
	 */
	@GET
	@Path("")
	@ApiOperation(value = "Get all Dimensions for the Domain (including child dimensions).")
	public List<Dimension> readAllDimensions(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId) throws ComputingException, InterruptedException {
		return delegate.readAll(userContext,
				new DomainPK(userContext.getCustomerId(), projectId, domainId));
	}

	@DELETE
	@Path("{"+PARAM_NAME+"}")
	@ApiOperation(value = "Deletes a dimension")
	public boolean deleteDimension(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam(PARAM_NAME) String dimensionId) {
		return delegate.delete(userContext,
				new DimensionPK(userContext.getCustomerId(), projectId,
						domainId, dimensionId));
	}

	@GET
	@Path("{"+PARAM_NAME+"}")
	@ApiOperation(value = "Gets a dimension")
	public Dimension readDimension(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam(PARAM_NAME) String dimensionId, @QueryParam("deepread") Boolean deepread) {
		return delegate.read(userContext,
				new DimensionPK(userContext.getCustomerId(), projectId,
						domainId, dimensionId));
	}

	@GET
	@Path("{"+PARAM_NAME+"}/options")
	@ApiOperation(value = "Gets all dimension options")
	public List<DimensionOption> readOptions(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam(PARAM_NAME) String dimensionId) {
		Dimension dimension =  delegate.read(userContext,
				new DimensionPK(userContext.getCustomerId(), projectId,
						domainId, dimensionId));
		return dimension.getOptions()!=null?dimension.getOptions():Collections.emptyList();
	}

	@POST
	@Path("{"+PARAM_NAME+"}/options")
	@ApiOperation(value = "Adds a dimension option")
	public List<DimensionOption> addOptions(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam(PARAM_NAME) String dimensionId,
			@ApiParam(required = true) DimensionOption option) throws ScopeException {
		Dimension dimension =  delegate.read(userContext,
				new DimensionPK(userContext.getCustomerId(), projectId,
						domainId, dimensionId));
		Dimension clone = (Dimension)SerializationUtils.clone(dimension);
		delegate.checkDimensionOption(userContext, clone, option);
		List<DimensionOption> options = clone.getOptions()!=null?clone.getOptions():new ArrayList<>();
		options.add(option);
		clone.setOptions(options);
		Dimension check = delegate.store(userContext, clone);
		return check.getOptions()!=null?check.getOptions():Collections.emptyList();
	}

	@POST
	@PUT
	@Path("{"+PARAM_NAME+"}/options/{optionId}")
	@ApiOperation(value = "Updates a dimension option")
	public List<DimensionOption> updateOptions(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam(PARAM_NAME) String dimensionId,
			@PathParam("optionId") String optionId,
			@ApiParam(required = true) DimensionOption update) throws ScopeException {
		Dimension dimension =  delegate.read(userContext,
				new DimensionPK(userContext.getCustomerId(), projectId,
						domainId, dimensionId));
		Dimension clone = (Dimension)SerializationUtils.clone(dimension);
		delegate.checkDimensionOption(userContext, clone, update);
		if (clone.getOptions()!=null) {
			DimensionOption found = null;
			ArrayList<DimensionOption> options = new ArrayList<>();
			for (DimensionOption option : clone.getOptions()) {
				if (!option.getId().getObjectId().equals(optionId)) {
					options.add(option);
				} else {
					found = option;
					options.add(update);
				}
			}
			if (found!=null) {
				clone.setOptions(options);
				Dimension check = delegate.store(userContext, clone);
				return check.getOptions();
			}
		}
		throw new ObjectNotFoundAPIException("cannot find the Dimension Option with ID="+optionId, false);
	}

	@GET
	@Path("{"+PARAM_NAME+"}/options/{optionId}")
	@ApiOperation(value = "Gets a dimension option")
	public DimensionOption readOption(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam(PARAM_NAME) String dimensionId,
			@PathParam("optionId") String optionId) {
		Dimension dimension =  delegate.read(userContext,
				new DimensionPK(userContext.getCustomerId(), projectId,
						domainId, dimensionId));
		Dimension clone = (Dimension)SerializationUtils.clone(dimension);
		if (clone.getOptions()!=null) {
			for (DimensionOption option : clone.getOptions()) {
				if (option.getId().getObjectId().equals(optionId)) {
					return option;
				}
			}
		}
		throw new ObjectNotFoundAPIException("cannot find the Dimension Option with ID="+optionId, false);
	}

	@DELETE
	@Path("{"+PARAM_NAME+"}/options/{optionId}")
	@ApiOperation(value = "Delete a dimension option")
	public List<DimensionOption> deleteOption(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam(PARAM_NAME) String dimensionId,
			@PathParam("optionId") String optionId) {
		Dimension dimension =  delegate.read(userContext,
				new DimensionPK(userContext.getCustomerId(), projectId,
						domainId, dimensionId));
		Dimension clone = (Dimension)SerializationUtils.clone(dimension);
		if (dimension.getOptions()!=null) {
			DimensionOption found = null;
			ArrayList<DimensionOption> options = new ArrayList<>();
			for (DimensionOption option : clone.getOptions()) {
				if (!option.getId().getObjectId().equals(optionId)) {
					options.add(option);
				} else {
					found = option;
				}
			}
			if (found!=null) {
				clone.setOptions(options);
				Dimension check = delegate.store(userContext, clone);
				return check.getOptions();
			}
		}
		throw new ObjectNotFoundAPIException("cannot find the Dimension Option with ID="+optionId, false);
	}

	@POST
	@Path("")
	@ApiOperation(value = "Creates a dimension")
	public Dimension storeDimension(@PathParam("projectId") String projectId, @PathParam("domainId") String domainId,
			@ApiParam(required = true) Dimension dimension) {
		DomainPK domainPK = new DomainPK(userContext.getCustomerId(), projectId, domainId);
		String dimensionId;
		if (dimension.getId() != null) {
			dimensionId = dimension.getId().getDimensionId();
		} else {
			dimensionId = null;
		}
		DimensionPK pk = new DimensionPK(domainPK, dimensionId);
		dimension.setId(pk);
		return delegate.store(userContext, dimension);
	}
	
	@POST
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Creates a dimension")
	public Dimension storeDimension2(@PathParam("projectId") String projectId, @PathParam("domainId") String domainId,
			@PathParam(PARAM_NAME) String dimensionId, @ApiParam(required = true) Dimension dimension) {
		DomainPK domainPK = new DomainPK(userContext.getCustomerId(), projectId, domainId);
		DimensionPK pk = new DimensionPK(domainPK, dimensionId);
		dimension.setId(pk);
		return delegate.store(userContext, dimension);
	}
	
	@PUT
	@Path("{"+PARAM_NAME+"}")
	@ApiOperation(value = "Updates a dimension")
	public Dimension updateDimension(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam(PARAM_NAME) String dimensionId,@ApiParam(required = true) Dimension dimension) {
		return delegate.store(userContext, dimension);
	}


	@Path("{"+PARAM_NAME+"}"+"/access")
	@GET
	@ApiOperation(value = "Gets a dimension's access rights")
	public Set<AccessRight> readAccessRightsDimension(
			@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam(PARAM_NAME) String dimensionId) {
		return delegate.readAccessRights(userContext, new DimensionPK(
				userContext.getCustomerId(), projectId, domainId, dimensionId));
	}

	@Path("{"+PARAM_NAME+"}"+"/access")
	@POST
	@ApiOperation(value = "Sets a dimension's access rights")
	public Set<AccessRight> storeAccessRightsDimension(
			@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam(PARAM_NAME) String dimensionId,
			@ApiParam(required = true) Set<AccessRight> accessRights) {
		return delegate.storeAccessRights(userContext, new DimensionPK(
				userContext.getCustomerId(), projectId, domainId, dimensionId),
				accessRights);
	}

	@Path("{"+PARAM_NAME+"}"+"/attributes-suggestion")
	@GET
	public ExpressionSuggestion getAttributeSuggestion(
			@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam("dimensionId") String dimensionId,
			@QueryParam("expression") String expression,
			@QueryParam("offset") int offset,
			@QueryParam("filterType") ValueType filterType) {
		return delegate.getAttributeSuggestion(userContext, projectId,
				domainId, dimensionId, expression, offset, filterType);
	}

	@Path("{"+PARAM_NAME+"}"+"/attributes")
	@ApiOperation(value = "Gets Attributes")
	public AttributeServiceRest getAttributeService() {
		return new AttributeServiceRest(userContext);
	}

	@Path("{"+PARAM_NAME+"}"+"/subdimensions")
	@GET
	@ApiOperation(value = "Gets the SubDimensions")
	public List<Dimension> readSubDimensions(
			@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId,
			@PathParam(PARAM_NAME) String dimensionId) {
		return delegate.readSubDimensions(userContext, new DimensionPK(
				userContext.getCustomerId(), projectId, domainId, dimensionId));
	}

}
