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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
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
import com.squid.kraken.v4.api.core.bookmark.BookmarkServiceBaseImpl;
import com.squid.kraken.v4.api.core.customer.CoreAuthenticatedServiceRest;
import com.squid.kraken.v4.api.core.customer.CustomerServiceBaseImpl;
import com.squid.kraken.v4.api.core.user.UserServiceBaseImpl;
import com.squid.kraken.v4.api.core.usergroup.UserGroupServiceBaseImpl;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.scope.GlobalExpressionScope;
import com.squid.kraken.v4.core.analysis.scope.ProjectExpressionRef;
import com.squid.kraken.v4.core.analysis.scope.SpaceExpression;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.Customer.AUTH_MODE;
import com.squid.kraken.v4.model.Customer;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.LzPersistentBaseImpl;
import com.squid.kraken.v4.model.PersistentBaseImpl;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserGroup;
import com.squid.kraken.v4.model.UserGroupPK;
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
@Path("/rs/enterprise")
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
		// escalate to root when needing to deal with Customer or updating User's rights
		AppContext root = ServiceUtils.getInstance().getRootUserContext(ctx);
		// lookup the resource
		PersistentBaseImpl<? extends GenericPK> resource = parseReference(ctx, reference);
		if (resource==null) {
			throw new ObjectNotFoundAPIException("cannot find resource with reference="+reference, true);
		}
		if (resource instanceof Bookmark) {
			return getBookmarkShare(ctx, root, reference, (Bookmark)resource);
		} else {
			return getStandardShare(ctx, root, reference, resource);
		}
	}

	/**
	 * use bookmark rules for computing the sharing
	 * @param ctx
	 * @param root
	 * @param reference
	 * @param resource
	 * @return
	 */
	private ShareQuery getBookmarkShare(AppContext ctx, AppContext root, String reference, Bookmark resource) {
		ArrayList<UserAcessLevel> accesses = new ArrayList<>();
		for (AccessRight right : resource.getAccessRights()) {
			if (right.getUserId()!=null) {
				UserAcessLevel ual = getUserAccessLevel(ctx, right);
				if (ual!=null) {
					accesses.add(ual);
				}
			}
		}
		ShareQuery query = new ShareQuery();
		query.setResources(Collections.singletonList(new ObjectReference(reference)));
		query.setSharing(accesses);
		return query;
	}
	
	/**
	 * use the standard ACL implementation for computing the sharing
	 * @param ctx
	 * @param root
	 * @param reference
	 * @param resource
	 * @return
	 */
	private ShareQuery getStandardShare(AppContext ctx, AppContext root, String reference, PersistentBaseImpl<? extends GenericPK> resource) {
		ArrayList<UserAcessLevel> accesses = new ArrayList<>();
		logger.info("ACL for object: "+resource.getId().toString());
		String objectID = "";
		if (resource.getId() instanceof ProjectPK) {
			// get it by inheritance
			objectID = ((ProjectPK)resource.getId()).getProjectId();
		}
		for (AccessRight right : resource.getAccessRights()) {
			if (right.getUserId()!=null) {
				UserAcessLevel ual = getUserAccessLevel(ctx, right);
				if (ual!=null) {
					accesses.add(ual);
				}
			} else if (right.getGroupId()!=null) {
				try {
					// escalate to root to deal with groups
					UserGroupPK pk = new UserGroupPK(ctx.getCustomerId(), right.getGroupId());
					UserGroup group = UserGroupServiceBaseImpl.getInstance().read(root, pk, true);
					logger.info(group.toString());
					if (right.getGroupId().equals("admin_"+objectID) || right.getGroupId().equals("guest_"+objectID)) {
						Collection<User> users = findUsersWithGroup(root, right.getGroupId());
						for (User user : users) {
							logger.info(user.toString()+" as part of "+group.toString());
							UserAcessLevel invitation = getUserAccessLevel(ctx, right, user);
							if (invitation!=null) {
								accesses.add(invitation);
							}
						}
					}
				} catch (ObjectNotFoundAPIException e) {
					// ignore
				}
			}
		}
		ShareQuery query = new ShareQuery();
		query.setResources(Collections.singletonList(new ObjectReference(reference)));
		query.setSharing(accesses);
		return query;
	}
	
	private Collection<User> findUsersWithGroup(AppContext ctx, String groupID) {
		List<User> users = UserServiceBaseImpl.getInstance().readAll(ctx);
		ArrayList<User> filter = new ArrayList<>();
		for (User user: users) {
			if (user.getGroups().contains(groupID)) {
				filter.add(user);
			}
		}
		return filter;
	}
	
	private UserAcessLevel getUserAccessLevel(AppContext ctx, AccessRight right) {
		AccessLevel acl = convertRole(right.getRole());
		if (acl!=null && !acl.equals(AccessLevel.NONE)) {
			// lookup the user
			String userId = right.getUserId();
			UserPK pk = new UserPK(ctx.getCustomerId(), userId);
			Optional<User> optional = userDAO.read(ctx, pk);
			if (optional.isPresent()) {
				User user = optional.get();
				if (user.getEmail()!=null) {// we are using the email as the identifier for external
					UserAcessLevel ual = new UserAcessLevel();
					ual.setUserID(user.getEmail());
					ual.setAccessLevel(acl);
					return ual;
				}
			}
		}
		// else
		return null;
	}
	
	private UserAcessLevel getUserAccessLevel(AppContext ctx, AccessRight right, User user) {
		AccessLevel role = convertRole(right.getRole());
		if (role!=null) {
			if (user.getEmail()!=null) {// we are using the email as the identifier for external
				UserAcessLevel ual = new UserAcessLevel();
				ual.setUserID(user.getEmail());
				ual.setAccessLevel(role);
				return ual;
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
	public ShareReply doShare(
			@Context HttpServletRequest request,
			ShareQuery query) 
	{
		AppContext ctx = getUserContext(request);
		return doShare(ctx, query);
	}
	
	private ShareReply doShare(
			AppContext ctx,
			ShareQuery query) {
		Collection<String> errors = new ArrayList<>();
		// escalate to root when needing to deal with Customer or updating User's rights
		AppContext root = ServiceUtils.getInstance().getRootUserContext(ctx);
		// check that the customer is valid for using OB.io
		Customer customer = CustomerServiceBaseImpl.getInstance().read(root, ctx.getCustomerPk());
		if (customer==null || customer.getAuthMode()!=AUTH_MODE.OBIO || customer.getTeamId()==null || customer.getTeamId().equals("")) {
			if (System.getProperty("enterprise.debug")==null || System.getProperty("enterprise.debug").equals("false")) {
				throw new APIException("this API is not available for this customer");
			}
		}
		if (ctx.getToken().getAuthorizationCode()==null) {
			if (System.getProperty("enterprise.debug")==null || System.getProperty("enterprise.debug").equals("false")) {
				throw new APIException("this API is not available for this session");
			}
		}
		// check the resources
		ArrayList<ObjectReference.Binding<LzPersistentBaseImpl<? extends GenericPK>>> bindings = new ArrayList<>();
		if (query.getResources()!=null) {
			for (ObjectReference ref : query.getResources()) {
				if (ref.getReference()!=null) {
					LzPersistentBaseImpl<? extends GenericPK> value = parseReference(ctx, ref.getReference());
					if (value!=null 
						// check if the ctx can modify the resource
						&& AccessRightsUtils.getInstance().hasRole(ctx, value, Role.WRITE)) 
					{
						ObjectReference.Binding<LzPersistentBaseImpl<? extends GenericPK>> binding = ref.<LzPersistentBaseImpl<? extends GenericPK>>bind(value);
						bindings.add(binding);
					} else {
						if (value==null) {
							errors.add("unable to lookup reference "+ref.getReference()+": user may not have access");
						} else {
							errors.add("unable to share reference "+ref.getReference()+": user doesn't have EDITOR access");
						}
					}
				} else {
					// invalid but ignore
				}
			}
		}
		if (bindings.isEmpty()) {
			ShareReply reply = new ShareReply();
			reply.setErrors(errors);
			return reply;
		}
		//boolean multiple = resources.size()>1;
		if (query.getSharing()!=null) {
			Collection<Invitation> invitations = new ArrayList<>();
			for (UserAcessLevel sharing : query.getSharing()) {
				User user = findUserByEmail(root, sharing.getUserID());
				if (user==null) {
					// provision a new user with the same email
					user = new User();
					user.setId(new UserPK(ctx.getCustomerId()));
					user.setLogin(sharing.getUserID());// using the email for login...
					user.setEmail(sharing.getUserID());
					// need to escalate in order to be able to create the user
					user = userDAO.create(root, user);
				}
				if (user!=null) {
					// give access right for the user
					ArrayList<Snippet> snippets = new ArrayList<>();
					for (ObjectReference.Binding<LzPersistentBaseImpl<? extends GenericPK>> binding : bindings) {
						// here we escalate to root so we can actually update the rights
						if (updateAccessRole(root, binding.getObject(), user, sharing.getAccessLevel())) {
							snippets.add(createSnippet(binding));
						}
					}
					// send invitation if required
					if (!snippets.isEmpty()) {
						Invitation invitation = new Invitation(sharing, snippets);
						invitations.add(invitation);
						if (System.getProperty("enterprise.debug")==null || System.getProperty("enterprise.debug").equals("false")) {
							sendInvitation(ctx, customer, user, invitation);
						}
					}
				} else {
					// some error while creating the user?
				}
			}
			ShareReply reply = new ShareReply(invitations);
			if (!errors.isEmpty()) reply.setErrors(errors);
			return reply;
		} else {
			throw new APIException("No-one to share with");
		}
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
			String authorization = "Bearer " + ctx.getToken().getAuthorizationCode();
			if (user.getAuthId()!=null) {
				// we already know the guy
				OBioApiHelper.getInstance().getMembershipService().inviteMember(authorization, teamId, user.getAuthId(), invitation);
			} else {
				// we don't know him
				OBioApiHelper.getInstance().getMembershipService().inviteMember(authorization, teamId, user.getEmail(), invitation);
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
		snippet.setType(binding.getObject().getClass().getSimpleName());
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
	 * @param ctx
	 * @param object
	 * @param user
	 * @param accessLevel
	 * @return
	 */
	private boolean updateAccessRole(AppContext ctx, LzPersistentBaseImpl<? extends GenericPK> object, User user,
			AccessLevel accessLevel) {
		if (object instanceof Bookmark) {
			return updateAccessRole(ctx, (Bookmark)object, user, accessLevel);
		} else if (object instanceof Project) {
			return updateAccessRole(ctx, (Project)object, user, accessLevel);
		} else {
			throw new APIException("not supported object type");
		}
	}
	
	/**
	 * update the accessRole, return false if not modified
	 * @param ctx
	 * @param resource
	 * @param user
	 * @param role
	 * @return
	 */
	private boolean updateAccessRole(AppContext ctx, Project resource, User user, AccessLevel acl) {
		// in order to give full access to the project, the user must be added to one of the special groups (admin_$ID or guet_$ID)
		Role role = convertRole(acl);
		Role currentRole = AccessRightsUtils.getInstance().getRole(user, resource);
		if (currentRole==null || !currentRole.equals(role)) {
			UserGroup group = getUserGroupForRole(ctx, resource, acl);
			// if group is null that means revoking access!!!
			ArrayList<String> copy = new ArrayList<>(user.getGroups());
			Iterator<String> iter = copy.iterator();
			while (iter.hasNext()) {
				String check = iter.next();
				if (check!=null && check.endsWith("_"+resource.getId().getProjectId())) {
					if (check.equals("guest_"+resource.getId().getProjectId())
					||	check.equals("admin_"+resource.getId().getProjectId())) 
					{
						// remove any other group here
						iter.remove();
					}
				}
			}
			if (group!=null) copy.add(group.getId().getUserGroupId());
			user.setGroups(copy);
			userDAO.update(ctx, user);
			// return true only if new invitation is needed
			return currentRole!=null && currentRole.ordinal()<role.ordinal();
		} else {
			return false;
		}
	}
	
	/**
	 * return the group to use given the access level, or null if no access is permitted
	 * @param ctx
	 * @param resource
	 * @param role
	 * @return
	 */
	private UserGroup getUserGroupForRole(AppContext ctx, Project resource, AccessLevel role) {
		String prefix = "";
		if (role==AccessLevel.EDITOR) prefix="admin_";
		else if (role==AccessLevel.VIEWER) prefix="guest_";
		else return null;// nothing to do
		String groupId = prefix+resource.getId().getProjectId();
		UserGroupPK PK = new UserGroupPK(ctx.getCustomerId(), groupId);
		try {
			return UserGroupServiceBaseImpl.getInstance().read(ctx, PK);
		} catch (ObjectNotFoundAPIException e) {
			return null;
		}
	}
	
	/**
	 * update the accessRole, return false if not modified
	 * @param ctx
	 * @param resource
	 * @param user
	 * @param role
	 * @return
	 */
	private boolean updateAccessRole(AppContext ctx, Bookmark bookmark, User user, AccessLevel acl) {
		Role role = convertRole(acl);
		// check what to do
		Role currentRole = AccessRightsUtils.getInstance().getRole(user, bookmark);
		if (currentRole==null || currentRole.ordinal()!=role.ordinal()) {
			// grant access to the bookmark
			if (grantRole(ctx, bookmark, user, role)) {
				if (!role.equals(Role.NONE)) {
					// give the user access to the project with role=EXECUTE if necessary
					try {
						Project project = ProjectManager.INSTANCE.getProject(ctx, bookmark.getId().getProjectId());
						grantRole(ctx, project, user, Role.EXECUTE);// if already granted it's a no-op
						return true;
					} catch (ScopeException e) {
						return false;
					}
				} else {
					// remove access if no more sharing
					try {
						Project project = ProjectManager.INSTANCE.getProject(ctx, bookmark.getId().getProjectId());
						if (hasSharedWithMeBookmark(ctx, bookmark.getId().getProjectId())) {
							grantRole(ctx, project, user, Role.NONE);
						}
						return false;// no invitation
					} catch (ScopeException e) {
						return false;
					}
				}
			}
		} else {
			// already have it
			return false;
		}
		return false;
	}
	
	private boolean hasSharedWithMeBookmark(AppContext ctx, String projectID) {
		String pathPrefix = Bookmark.SEPARATOR+Bookmark.Folder.USER+Bookmark.SEPARATOR;
		List<Bookmark> bookmarks = BookmarkServiceBaseImpl.getInstance().readAll(ctx, projectID, pathPrefix);
		pathPrefix += ctx.getUser().getOid()+Bookmark.SEPARATOR;
		for (Bookmark bookmark : bookmarks) {
			if (!bookmark.getPath().startsWith(pathPrefix)) {
				return true;
			}
		}
		// else
		return false;
	}
	
	/**
	 * Add the role for the user to the resource - not taking the groups into account
	 * If the role is NONE, that will remove any Role for that user
	 * @param ctx
	 * @param project
	 * @param user
	 */
	@SuppressWarnings("unchecked")
	private boolean grantRole(AppContext ctx, PersistentBaseImpl<? extends GenericPK> resource, User user, Role role) {
		HashSet<AccessRight> rights = new HashSet<>(resource.getAccessRights());
		AccessRight right = new AccessRight(role, user.getId().getUserId(), null);
		removeRightsFor(rights, user);// just in case
		if (!role.equals(Role.NONE)) rights.add(right);
		resource.setAccessRights(rights);
		DAOFactory.getDAOFactory().getDAO(resource.getClass()).update(ctx, resource);
		return true;
	}

	/**
	 * @param rights
	 * @param user
	 */
	private void removeRightsFor(HashSet<AccessRight> rights, User user) {
		Iterator<AccessRight> iter = rights.iterator();
		while (iter.hasNext()) {
			AccessRight acr = iter.next();
			if (acr.getUserId()!=null && acr.getUserId().equals(user.getOid())) {
				iter.remove();
			}
		}
	}

	private Role convertRole(AccessLevel role) {
		switch (role) {
		case NONE:
			return Role.NONE;
		case EDITOR:
			return Role.WRITE;
		case VIEWER:
			return Role.READ;
		default:
			return null;
		}
	}
	
	private AccessLevel convertRole(Role role) {
		switch (role) {
		case WRITE: return AccessLevel.EDITOR;
		case READ: return AccessLevel.VIEWER;
		case EXECUTE: return AccessLevel.NONE;
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
