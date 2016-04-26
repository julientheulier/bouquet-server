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
package com.squid.kraken.v4.api.core.relation;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.squid.core.expression.reference.Cardinality;
import com.squid.kraken.v4.api.core.BaseServiceRest;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.Relation;
import com.squid.kraken.v4.persistence.AppContext;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.Authorization;
import com.wordnik.swagger.annotations.AuthorizationScope;

@Produces({ MediaType.APPLICATION_JSON })
@Api(value = "relations", hidden = true, authorizations = { @Authorization(value = "kraken_auth", type = "oauth2", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
public class DomainRelationServiceRest  extends BaseServiceRest {


	private RelationServiceBaseImpl delegate = RelationServiceBaseImpl
			.getInstance();

	public DomainRelationServiceRest(AppContext userContext) {
		super(userContext);
		// TODO Auto-generated constructor stub
	}
	
	@GET
	@Path("")
	@ApiOperation(value = "Get all Relations for the Domain.")
	public List<Relation> readAll(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId) throws ComputingException, InterruptedException {
		return delegate.readAll(userContext,
				new DomainPK(userContext.getCustomerId(), projectId, domainId));
	}
	
	@GET
	@Path("/new")
	@ApiOperation(value = "Get new default relation for the Domain.")
	public Relation createNew(@PathParam("projectId") String projectId,
			@PathParam("domainId") String domainId) throws ComputingException, InterruptedException {
		//
		Relation rel = new Relation();// no PK
		DomainPK leftId = new DomainPK(new ProjectPK(userContext.getCustomerId(), projectId), domainId);
		rel.setLeftId(leftId);
		rel.setLeftCardinality(Cardinality.MANY);
		rel.setRightCardinality(Cardinality.ONE);
		return rel;
	}

}
