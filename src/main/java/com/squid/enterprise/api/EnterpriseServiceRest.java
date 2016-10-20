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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.enterprise.model.UserAcessLevel;
import com.squid.enterprise.model.UserAcessLevel.AccessLevel;
import com.squid.enterprise.model.Invitation;
import com.squid.enterprise.model.ObjectReference;
import com.squid.enterprise.model.ObjectReference.Binding;
import com.squid.enterprise.model.ShareQuery;
import com.squid.enterprise.model.ShareReply;
import com.squid.enterprise.model.Snippet;
import com.squid.enterprise.model.Status;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.OBioApiHelper;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.api.core.customer.CoreAuthenticatedServiceRest;
import com.squid.kraken.v4.api.core.customer.CustomerServiceBaseImpl;
import com.squid.kraken.v4.api.core.user.UserServiceBaseImpl;
import com.squid.kraken.v4.core.analysis.scope.GlobalExpressionScope;
import com.squid.kraken.v4.core.analysis.scope.ProjectExpressionRef;
import com.squid.kraken.v4.core.analysis.scope.SpaceExpression;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Customer.AUTH_MODE;
import com.squid.kraken.v4.model.Customer;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.LzPersistentBaseImpl;
import com.squid.kraken.v4.model.PersistentBaseImpl;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.UserDAO;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;

/**
 * This is the API for OB enterprise features
 * @author serge.fantino
 *
 */
@Path("/enterprise")
@Api(hidden = true, value = "enterprise", authorizations = { @Authorization(value = "kraken_auth", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
@Produces({ MediaType.APPLICATION_JSON })
public class EnterpriseServiceRest extends CoreAuthenticatedServiceRest {

	static final Logger logger = LoggerFactory
			.getLogger(EnterpriseServiceRest.class);

	public final static String BBID_PARAM_NAME = "REFERENCE";

	private UserDAO userDAO = ((UserDAO) DAOFactory.getDAOFactory().getDAO(User.class));
	
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
	
	@GET
	@Path("/share/{" + BBID_PARAM_NAME + "}")
	@ApiOperation(
			value = "Get information regarding who is sharing this resource",
			notes = "")
	public ShareQuery getShare(
			@Context HttpServletRequest request,
			@PathParam(BBID_PARAM_NAME) String reference) 
	{
		AppContext ctx = getUserContext(request);
		// lookup the resource
		PersistentBaseImpl<? extends GenericPK> resource = parseReference(ctx, reference);
		if (resource==null) {
			throw new ObjectNotFoundAPIException("cannot find resource with reference="+reference, true);
		}
		HashSet<AccessRight> rights = new HashSet<>(resource.getAccessRights());
		ObjectReference ref = new ObjectReference(reference);
		ArrayList<UserAcessLevel> invitations = new ArrayList<>();
		for (AccessRight right : rights) {
			UserAcessLevel invitation = createInvitation(ctx, right);
			if (invitation!=null) {
				invitations.add(invitation);
			}
		}
		ShareQuery query = new ShareQuery();
		query.setResources(Collections.singletonList(ref));
		query.setSharing(invitations);
		return query;
	}
	
	private UserAcessLevel createInvitation(AppContext ctx, AccessRight right) {
		AccessLevel role = convertRole(right.getRole());
		if (role!=null) {
			// lookup the user
			String userId = right.getUserId();
			UserPK pk = new UserPK(ctx.getCustomerId(), userId);
			Optional<User> optional = userDAO.read(ctx, pk);
			if (optional.isPresent()) {
				User user = optional.get();
				if (user.getEmail()!=null) {// we are using the email as the identifier for external
					UserAcessLevel invitation = new UserAcessLevel();
					invitation.setUserID(user.getEmail());
					invitation.setAccessLevel(role);
					return invitation;
				}
			}
		}
		// else
		return null;
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
		// check that the customer is valid for using OB.io
		Customer customer = CustomerServiceBaseImpl.getInstance().read(ctx, ctx.getCustomerPk());
		if (customer==null || customer.getAuthMode()!=AUTH_MODE.OBIO || customer.getTeamId()==null || customer.getTeamId().equals("")) {
			throw new APIException("this API is not available for this customer");
		}
		if (ctx.getToken().getAuthorizationCode()==null) {
			throw new APIException("this API is not available for this session");
		}
		// check the resources
		ArrayList<ObjectReference.Binding<LzPersistentBaseImpl<? extends GenericPK>>> resources = new ArrayList<>();
		if (query.getResources()!=null) {
			for (ObjectReference ref : query.getResources()) {
				if (ref.getReference()!=null) {
					LzPersistentBaseImpl<? extends GenericPK> value = parseReference(ctx, ref.getReference());
					if (value!=null 
						// check if the ctx can modify the resource
						&& AccessRightsUtils.getInstance().hasRole(ctx, value, Role.WRITE)) 
					{
						ObjectReference.Binding<LzPersistentBaseImpl<? extends GenericPK>> binding = ref.<LzPersistentBaseImpl<? extends GenericPK>>bind(value);
						resources.add(binding);
					}
				} else {
					// invalid but ignore
				}
			}
		}
		if (resources.isEmpty()) {
			throw new APIException("Nothing to share");
		}
		//boolean multiple = resources.size()>1;
		AppContext root = ServiceUtils.getInstance().getRootUserContext(ctx);
		if (query.getSharing()!=null) {
			for (UserAcessLevel sharing : query.getSharing()) {
				User user = findUserByEmail(root, sharing.getUserID());
				if (user==null) {
					// provision a new user with the same email
					user = new User();
					user.setEmail(sharing.getUserID());
					user = userDAO.create(ctx, user);
				}
				if (user!=null) {
					// give access right for the user
					ArrayList<Snippet> snippets = new ArrayList<>();
					for (ObjectReference.Binding<LzPersistentBaseImpl<? extends GenericPK>> resource : resources) {
						if (setAccessRole(ctx, resource.getObject(), user, sharing.getAccessLevel())) {
							snippets.add(createSnippet(resource));
						}
					}
					// send invitation if required
					if (!snippets.isEmpty()) {
						Invitation invitation = new Invitation(sharing, snippets);
						sendInvitation(ctx, customer, user, invitation);
					}
				} else {
					// some error while creating the user?
				}
			}
		} else {
			throw new APIException("No-one to share with");
		}
		return new ShareReply();
	}
	
	/**
	 * send the invitation to the user through OB.io
	 * @param customer 
	 * @param user 
	 * @param invitation
	 * @param snippets
	 */
	private boolean sendInvitation(AppContext ctx, Customer customer, User user, Invitation invitation) {
		try {
			String teamId = customer.getTeamId();
			String authorization = ctx.getToken().getAuthorizationCode();
			ObjectMapper json = new ObjectMapper();
			String data = json.writeValueAsString(invitation);
			if (user.getAuthId()!=null) {
				// we already know the guy
				OBioApiHelper.getInstance().getMembershipService().inviteMember(authorization, teamId, user.getAuthId(), data);
			} else {
				// we don't know him
				OBioApiHelper.getInstance().getMembershipService().inviteMember(authorization, teamId, user.getEmail(), data);
			}
			return true;
		} catch (Exception e) {
			logger.error("/share failed to send invite to user "+user.getEmail()+" due to:"+e.getMessage(), e);
			return false;
		}
	}

	/**
	 * @param resources
	 * @return
	 */
	private Snippet createSnippet(Binding<LzPersistentBaseImpl<? extends GenericPK>> binding) {
		Snippet snippet = new Snippet(binding.getObjectReference());
		snippet.setName(binding.getObject().getName());
		snippet.setDescription(binding.getObject().getDescription());
		snippet.setType(binding.getObject().getClass().getName());
		return snippet;
	}

	private LzPersistentBaseImpl<? extends GenericPK> parseReference(AppContext ctx, String BBID) {
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
	
	/**
	 * update the accessRole, return false if not modified
	 * @param ctx
	 * @param resource
	 * @param user
	 * @param role
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private boolean setAccessRole(AppContext ctx, PersistentBaseImpl<? extends GenericPK> resource, User user, AccessLevel role) {
		com.squid.kraken.v4.model.AccessRight.Role acl = convertRole(role);
		HashSet<AccessRight> rights = new HashSet<>(resource.getAccessRights());
		AccessRight right = new AccessRight(acl, user.getId().getUserId(), null);
		if (!AccessRightsUtils.getInstance().hasRole(user, rights, acl)) {
			rights.add(right);
			resource.setAccessRights(rights);
			DAOFactory.getDAOFactory().getDAO(resource.getClass()).update(ctx, resource);
			return true;
		} else {
			// already has it or better, what to do?
			Role current = AccessRightsUtils.getInstance().getRole(user, resource);
			if (current==Role.WRITE && acl==Role.READ) {
				// someone is asking for downgrade
				return true;
			}
		}
		// else
		return false;
	}
	
	private Role convertRole(AccessLevel role) {
		switch (role) {
		case EDITOR:
			return Role.WRITE;
		case VIEW:
		default:
			return Role.READ;
		}
	}
	
	private AccessLevel convertRole(Role role) {
		switch (role) {
		case WRITE: return AccessLevel.EDITOR;
		case READ: return AccessLevel.VIEW;
		case OWNER:// ignore, we never change OWNER role
		case NONE:
		default:
			return null;
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
