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
package com.squid.kraken.v4.persistence.dao;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;

import com.google.common.base.Optional;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.APIException.ApiError;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.InvalidCredentialsAPIException;
import com.squid.kraken.v4.api.core.RolePriorityMapper;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.caching.Cache;
import com.squid.kraken.v4.caching.CacheFactoryEHCache;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.AccessToken;
import com.squid.kraken.v4.model.Customer;
import com.squid.kraken.v4.model.CustomerPK;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserGroup;
import com.squid.kraken.v4.model.UserGroupPK;
import com.squid.kraken.v4.model.UserPK;
import com.squid.kraken.v4.persistence.AccessRightsPersistentDAO;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.DataStore;
import com.squid.kraken.v4.persistence.DataStoreEvent;
import com.squid.kraken.v4.persistence.DataStoreQueryField;

public class UserDAO extends AccessRightsPersistentDAO<User, UserPK> {

	public static final String PASSWORD_REGEX = "((?=[\\S]+$)).+";

	public static final String PASSWORD_REGEX2 = "(?=.*[0-9A-Z]).+";

	private Cache<CustomerPK, List<UserPK>> findByCustomerCache;

	public UserDAO(DataStore ds) {
		super(User.class, ds);
		this.findByCustomerCache = CacheFactoryEHCache.getCacheFactory()
				.getCollectionsCache(UserPK.class, "findByCustomer");
	}

	public Optional<User> findByLogin(AppContext app, String login) {
		List<DataStoreQueryField> queryFields = new LinkedList<DataStoreQueryField>();
		queryFields.add(new DataStoreQueryField("login", login.toLowerCase()));
		List<User> find = ds.find(app, type, queryFields, null);
		if (find.isEmpty()) {
			return Optional.absent();
		} else {
			return Optional.of(find.get(0));
		}
	}

	public Optional<User> findByEmail(AppContext app, String email) {
		List<DataStoreQueryField> queryFields = new LinkedList<DataStoreQueryField>();
		queryFields.add(new DataStoreQueryField("email", email));
		List<User> find = ds.find(app, type, queryFields, null);
		if (find.isEmpty()) {
			return Optional.absent();
		} else {
			return Optional.of(find.get(0));
		}
	}

	public List<User> findByCustomer(AppContext app, CustomerPK customerId) {
		return super.find(app, customerId, null, findByCustomerCache);
	}

	@Override
	public Optional<User> read(AppContext ctx, UserPK id) {
		Optional<User> object = ds.read(ctx, type, id);
		if (object.isPresent()) {
			applyUserRigths(ctx, object.get());
		}
		return object;
	}

	@Override
	public User readNotNull(AppContext ctx, UserPK id) {
		User object = ds.readNotNull(ctx, type, id);
		applyUserRigths(ctx, object);
		return object;
	}

	/**
	 * Implement specific access rules :
	 * <ul>
	 * <li>READ right on Customer is required</li>
	 * <li>if the current user if the user read, it has OWNER rights, otherwise
	 * it will inherit the right from its parent</li>
	 * </ul>
	 */
	private void applyUserRigths(AppContext ctx, User object) {
		if (!object.getId().equals(ctx.getUser().getId())) {
			Persistent<? extends GenericPK> parent = object
					.getParentObject(ctx);
			// set the parent's role as userRole
			object.setUserRole(AccessRightsUtils.getInstance().getRole(
					ctx.getUser(), parent));
			// check the READ access rights
			AccessRightsUtils.getInstance().checkRole(ctx, parent, Role.READ);
		} else {
			// the calling user is itself the user
			object.setUserRole(Role.OWNER);
		}
	}

	@Override
	public void notifyEvent(DataStoreEvent event) {
		UserPK id = null;
		if (event.getSource() instanceof UserPK) {
			// deletion
			id = (UserPK) event.getSource();
			instanceCache.remove(id);
		}
		if (event.getSource() instanceof User) {
			// creation or update
			User source = (User) event.getSource();
			id = source.getId();
			instanceCache.put(id, source);
		}
		if (id != null) {
			// finder cache invalidation
			findByCustomerCache.remove(new CustomerPK(id.getCustomerId()));
		}
	}

	@Override
	public User create(AppContext ctx, User userData) {
		if (userData.getPassword() == null) {
			// set a random password
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < 8; i++) {
				sb.append(RandomStringUtils.randomAlphanumeric(8));
			}
			userData.setPassword(sb.toString());
		}
		// hash the password
		userData.setPassword(hashPassword(ctx, userData.getPassword()));

		// check for duplicates
		if (findByEmail(ctx, userData.getEmail()).isPresent()) {
			throw new APIException("Duplicate email", ctx.isNoError(),
					ApiError.DUPLICATE_EMAIL);
		}
		if (findByLogin(ctx, userData.getLogin()).isPresent()) {
			throw new APIException("Duplicate login", ctx.isNoError(),
					ApiError.DUPLICATE_LOGIN);
		}

		// check the access rights
		Persistent<? extends GenericPK> parent = userData.getParentObject(ctx);

		// need write role on parent
		AccessRightsUtils.getInstance().checkRole(ctx, parent, Role.WRITE);

		checkUserGroupAccess(ctx, userData.getGroups());

		return ds.create(ctx, userData);
	}

	@Override
	public void update(AppContext ctx, User userData) {
		User existingUser = DAOFactory.getDAOFactory().getDAO(User.class)
				.readNotNull(ctx, userData.getId());
		if (userData.getPassword() != null) {

			AppContext rootUserContext = ServiceUtils.getInstance()
					.getRootUserContext(ctx.getCustomerId());
			Customer customer = DAOFactory.getDAOFactory()
					.getDAO(Customer.class)
					.readNotNull(rootUserContext, ctx.getCustomerPk());
			boolean doUpdate = false;
			// check for conditions to update the password
			if ((ctx.getToken() != null)
					&& (ctx.getToken().getType()
							.equals(AccessToken.Type.RESET_PWD))) {
				// the token is a reset pwd token
				doUpdate = true;
			} else if (AccessRightsUtils.getInstance().hasRole(ctx, customer,
					Role.WRITE)) {
				// the user is an admin
				doUpdate = true;
			} else {
				int index = userData.getPassword().indexOf(" ");
				if (index >= 0) {
					String oldPassword = userData.getPassword().substring(0,
							index);
					if (ServiceUtils.getInstance().matchPassword(ctx,
							existingUser, oldPassword)) {
						// old and new passwords match
						doUpdate = true;
					}
				} else {
					throw new InvalidCredentialsAPIException(
							"Sent password string should be '[new-password] [old-password]'",
							ctx.isNoError());
				}
			}
			if (doUpdate) {
				// update the password
				String newPassword = userData.getPassword().substring(
						userData.getPassword().indexOf(" ") + 1);
				userData.setPassword(hashPassword(ctx, newPassword));
			} else {
				throw new InvalidCredentialsAPIException(
						"Invalid old password", ctx.isNoError());
			}
		} else {
			// do not update the password
			userData.setPassword(existingUser.getPassword());
		}

		// check for duplicates
		Optional<User> findByEmail = findByEmail(ctx, userData.getEmail());
		if (findByEmail.isPresent()) {
			if (!findByEmail.get().getId().equals(userData.getId())) {
				throw new APIException("Duplicate email", ctx.isNoError(),
						ApiError.DUPLICATE_EMAIL);
			}
		}
		Optional<User> findByLogin = findByLogin(ctx, userData.getLogin());
		if (findByLogin.isPresent()) {
			if (!findByLogin.get().getId().equals(userData.getId())) {
				throw new APIException("Duplicate login", ctx.isNoError(),
						ApiError.DUPLICATE_LOGIN);
			}
		}

		// check the access rights
		// prepare neccessary information

		String ctxUserId = ctx.getUser().getId().getUserId();
		String newUserId = userData.getId().getUserId();

		// caller != callee
		if (!ctxUserId.equalsIgnoreCase(newUserId)) {
			// get the customer and its access rights
			Customer customer = DAOFactory.getDAOFactory()
					.getDAO(Customer.class)
					.readNotNull(ctx, ctx.getCustomerPk());
			Set<AccessRight> accessRights = customer.getAccessRights();

			// retrieve access rights of the context user and the new user from
			// access rights of customer
			List<RolePriorityMapper> ctxAccessRights = new ArrayList<RolePriorityMapper>();
			List<RolePriorityMapper> newUserAccessRights = new ArrayList<RolePriorityMapper>();
			for (AccessRight ar : accessRights) {
				if ((ar.getUserId() != null)
						&& (ar.getUserId().equals(ctxUserId))) {
					ctxAccessRights.add(new RolePriorityMapper(ar.getRole()));
				}
				if ((ar.getUserId() != null)
						&& (ar.getUserId().equals(newUserId))) {
					newUserAccessRights
							.add(new RolePriorityMapper(ar.getRole()));
				}
			}

			// compare access right with new user
			RolePriorityMapper ctxHighestRole = getHighestRole(ctxAccessRights);
			RolePriorityMapper newUserHighestRole = getHighestRole(newUserAccessRights);
			if ((newUserHighestRole != null) && (ctxHighestRole != null)) {
				if ((newUserHighestRole.getRole() != Role.OWNER)
						|| (ctxHighestRole.getRole() != Role.OWNER)) {
					if (newUserHighestRole.getPriority() >= ctxHighestRole
							.getPriority()) {
						throw new InvalidCredentialsAPIException(
								"Insufficient privileges : caller hasn't "
										+ newUserHighestRole.getRole().name()
										+ " role on " + ctxUserId,
								ctx.isNoError());
					}
				}
			} else {
				if ((ctxHighestRole != null)
						&& (ctxHighestRole.getRole() != Role.WRITE)
						&& (ctxHighestRole.getRole() != Role.OWNER)) {
					throw new InvalidCredentialsAPIException(
							"Insufficient privileges : caller hasn't WRITE/OWNER role on"
									+ ctxUserId, ctx.isNoError());
				}
			}
		}

		// check for groups membership update
		if ((userData.getGroups() != null) && (!userData.getGroups().equals(existingUser.getGroups()))) {
			List<String> added = new ArrayList<String>(userData.getGroups());
			added.removeAll(existingUser.getGroups());
			checkUserGroupAccess(ctx, added);
		} else {
			// do not update groups
			userData.setGroups(existingUser.getGroups());
		}
		ds.update(ctx, userData);
	}

	private String hashPassword(AppContext ctx, String pwd) {
		if (pwd != null) {
			// validate password
			if (!(pwd.length() >= 8 && pwd.length() <= 100)) {
				throw new APIException(
						"Invalid password (must be from 8 to 100 chars)",
						ctx.isNoError(), ApiError.PASSWORD_INVALID_SIZE);
			} else if (!pwd.matches(PASSWORD_REGEX)) {
				throw new APIException(
						"Invalid password (cannot contain whitespaces or any special chars)",
						ctx.isNoError(), ApiError.PASSWORD_INVALID_CHAR);
			} else if (!pwd.matches(PASSWORD_REGEX2)) {
				throw new APIException(
						"Invalid password (should contain at least a capital letter or a digit)",
						ctx.isNoError(), ApiError.PASSWORD_INVALID_RULES);
			}
		}
		// hash using customer's salt
		Customer customer = ServiceUtils.getInstance().getContextCustomer(ctx);
		String hashedPassword = ServiceUtils.getInstance().md5(
				customer.getMD5Salt(), pwd);
		return hashedPassword;
	}

	@Override
	public void delete(AppContext ctx, UserPK id) {
		User user = DAOFactory.getDAOFactory().getDAO(User.class)
				.readNotNull(ctx, id);
		Persistent<? extends GenericPK> parent = user.getParentObject(ctx);
		// check specific access rights
		Role currentUserRole = AccessRightsUtils.getInstance().getRole(
				ctx.getUser(), parent);
		Role targetUserRole = AccessRightsUtils.getInstance().getRole(user,
				parent);
		boolean doDelete = false;
		if (ctx.getUser().isSuperUser()) {
			doDelete = true;
		}
		if ((!doDelete)
				&& ((currentUserRole != null)
						&& (currentUserRole.ordinal() > Role.READ.ordinal()) && (targetUserRole == null || (currentUserRole
						.ordinal() > targetUserRole.ordinal())))) {
			// of we can delete
			doDelete = true;
		}
		if (doDelete) {
			ds.delete(ctx, type, id);
		} else {
			throw new InvalidCredentialsAPIException("Insufficient privileges",
					ctx.isNoError());
		}

	}

	/**
	 * Get the highest priority role from a list of assigned roles.
	 * 
	 * @param roles
	 *            access right list
	 * @return the highest priority role, NULL if the list in argument is empty
	 *         or null
	 */
	private RolePriorityMapper getHighestRole(List<RolePriorityMapper> roles) {
		if ((roles != null) && !roles.isEmpty()) {
			Collections.sort(roles);
			return roles.get(0);
		}
		return null;
	}

	/**
	 * Check if current user has write role on usergroups
	 * 
	 * @param ctx
	 * @param groups
	 */
	private void checkUserGroupAccess(AppContext ctx, List<String> groups) {
		for (String addedGroupId : groups) {
			UserGroupDAO dao = (UserGroupDAO) DAOFactory.getDAOFactory()
					.getDAO(UserGroup.class);
			UserGroup userGroup = dao.readNotNull(ctx,
					new UserGroupPK(ctx.getCustomerId(), addedGroupId));
			AccessRightsUtils.getInstance().checkRole(ctx, userGroup,
					Role.WRITE);
		}
	}
}
