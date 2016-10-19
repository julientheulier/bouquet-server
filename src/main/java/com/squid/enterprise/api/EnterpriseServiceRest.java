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
package com.squid.enterprise.api;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.google.common.base.Optional;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.enterprise.model.Invitation;
import com.squid.enterprise.model.ObjectReference;
import com.squid.enterprise.model.ShareQuery;
import com.squid.enterprise.model.ShareReply;
import com.squid.enterprise.model.Status;
import com.squid.enterprise.model.Invitation.Role;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.api.core.bookmark.BookmarkServiceBaseImpl;
import com.squid.kraken.v4.api.core.customer.CoreAuthenticatedServiceRest;
import com.squid.kraken.v4.api.core.project.ProjectServiceBaseImpl;
import com.squid.kraken.v4.api.core.user.UserServiceBaseImpl;
import com.squid.kraken.v4.core.analysis.scope.GlobalExpressionScope;
import com.squid.kraken.v4.core.analysis.scope.ProjectExpressionRef;
import com.squid.kraken.v4.core.analysis.scope.SpaceExpression;
import com.squid.kraken.v4.core.analysis.scope.UniverseScope;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.BookmarkPK;
import com.squid.kraken.v4.model.ExpressionObject;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.PersistentBaseImpl;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.ReferencePK;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.UserDAO;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;

/**
 * This is the API for OB enterprise features
 * @author sergefantino
 *
 */
@Path("/enterprise")
@Api(hidden = true, value = "enterprise", authorizations = { @Authorization(value = "kraken_auth", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
@Produces({ MediaType.APPLICATION_JSON })
public class EnterpriseServiceRest extends CoreAuthenticatedServiceRest {

	
	@GET
	@Path("/status")
	@ApiOperation(
			value = "Open Bouquet Enterprise Service status",
			notes = "")
	public Status getStatus(
			@Context HttpServletRequest request) 
	{
		return new Status();
	}
	
	@POST
	@Path("/test")
	@ApiOperation(
			value = "Share some resources with some users",
			notes = "")
	public ShareQuery test() 
	{
		return new ShareQuery();
	}
	
	/**
	 * POST method used by the client app to share some contents with a set of users.
	 * As of 4.2.28, content can be Project or Bookmark. User can get access as Editor (Write Role) or Viewer (Read Role only).
	 * Note that an User may not be already a registered user n the system. If the user is not yet part of the team, it will provision a "dummy" user locally (identified by its email).
	 * Once the new user will join the team, he will find his resources with access control already set.
	 * 
	 * This method performs two tasks:
	 * - granting local access to the resources to the users
	 * - forwarding an invitation for each team member (or future one) to OB.io
	 * 
	 * @param request
	 * @param query
	 * @return
	 */
	@POST
	@Path("/share")
	@ApiOperation(
			value = "Share some resources with some users",
			notes = "")
	public ShareReply postShare(
			@Context HttpServletRequest request,
			ShareQuery query) 
	{
		return doShare(request, query);
	}
	
	private ShareReply doShare(
			HttpServletRequest request,
			ShareQuery query) {
		AppContext ctx = getUserContext(request);
		// check the resources
		ArrayList<PersistentBaseImpl<? extends GenericPK>> resources = new ArrayList<>();
		if (query.getResources()!=null) {
			for (ObjectReference ref : query.getResources()) {
				if (ref.getLegacyPK()!=null) {
					PersistentBaseImpl<? extends GenericPK> value = findReference(ctx, ref.getLegacyPK().getReference());
					if (value!=null) {
						resources.add(value);
					}
				} else if (ref.getObjectID()!=null) {
					PersistentBaseImpl<? extends GenericPK> value = parseReference(ctx, ref.getObjectID());
					if (value!=null) {
						resources.add(value);
					}
				} else {
					// invalid but ignore
				}
			}
		}
		if (resources.isEmpty()) {
			throw new APIException("Nothing to share");
		}
		AppContext root = ServiceUtils.getInstance().getRootUserContext(ctx);
		if (query.getInvitations()!=null) {
			for (Invitation invitation : query.getInvitations()) {
				User user = findUserByEmail(root, invitation.getUserID());
				if (user==null) {
					// provision a new user with the same email
					UserDAO userDAO = ((UserDAO) DAOFactory.getDAOFactory().getDAO(User.class));
					// register a brand new user
					user = new User();
					user.setEmail(invitation.getUserID());
					user = userDAO.create(ctx, user);
				}
				if (user!=null) {
					// give access right for the user
					for (PersistentBaseImpl<? extends GenericPK> resource : resources) {
						addAccessRole(ctx, resource, user, invitation.getRole());
					}
					// send invitation
					
				} else {
					// some error while creating the user?
				}
			}
		} else {
			throw new APIException("No-one to share with");
		}
		return new ShareReply();
	}
	
	private PersistentBaseImpl<? extends GenericPK> parseReference(AppContext ctx, String BBID) {
		try {
			GlobalExpressionScope scope = new GlobalExpressionScope(ctx);
			ExpressionAST expr = scope.parseExpression(BBID);
			if (expr instanceof SpaceExpression) {
				SpaceExpression ref = (SpaceExpression)expr;
				Space space = ref.getSpace();
				if (space.hasBookmark()) {
					return space.getBookmark();
				} else {
					return space.getDomain();
				}
			} else if (expr instanceof ProjectExpressionRef) {
				ProjectExpressionRef ref = (ProjectExpressionRef)expr;
				return ref.getProject();
			}
		} catch (ScopeException e) {
			return null;
		}
		// else
		return null;
	}
	
	private PersistentBaseImpl<? extends GenericPK> findReference(AppContext ctx, Object reference) {
		if (reference instanceof ReferencePK<?>) {
			ReferencePK<?> unwrap = (ReferencePK<?>) reference;
			GenericPK ref = unwrap.getReference();
			if (ref instanceof ProjectPK) {
				// this is a project
				ProjectPK projectPK = (ProjectPK)ref;
				return ProjectServiceBaseImpl.getInstance().read(ctx, projectPK);
			} else if (ref instanceof BookmarkPK) {
				// this is a bookmark
				BookmarkPK bookmarkPK = (BookmarkPK)ref;
				return BookmarkServiceBaseImpl.getInstance().read(ctx, bookmarkPK);
			}
		}
		// else
		return null;
	}
	
	private void addAccessRole(AppContext ctx, PersistentBaseImpl<? extends GenericPK> resource, User user, Role role) {
		AccessRight right = new AccessRight(convertRole(role), user.getId().getUserId(), null);
		HashSet<AccessRight> rights = new HashSet<>(resource.getAccessRights());
		rights.add(right);
		resource.setAccessRights(rights);
		DAOFactory.getDAOFactory().getDAO(resource.getClass()).update(ctx, resource);
	}
	
	private com.squid.kraken.v4.model.AccessRight.Role convertRole(Role role) {
		switch (role) {
		case EDITOR:
			return com.squid.kraken.v4.model.AccessRight.Role.WRITE;
		case VIEW:
		default:
			return com.squid.kraken.v4.model.AccessRight.Role.READ;
		}
	}
	
	private User findUserByEmail(AppContext ctx, String email) {
		List<User> users = UserServiceBaseImpl.getInstance().readAll(ctx);
		for (User user : users) {
			if (user.getEmail()!=null && user.getEmail().equalsIgnoreCase(email)) {
				// same email, that's the one...
				return user;
			}
		}
		// else
		return null;
	}

}
