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
package com.squid.kraken.v4.api.core;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.HasAccessRights;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.persistence.AppContext;

public class AccessRightsUtils {

	private final static AccessRightsUtils instance = new AccessRightsUtils();

	public static AccessRightsUtils getInstance() {
		return instance;
	}

	/**
	 * Check if a User has a given Role (or superior Role) for an object.
	 */
	public boolean hasRole(User user, Set<AccessRight> accessRights, Role role) {
		boolean hasRole = false;
		if (user.isSuperUser()) {
			hasRole = true;
		} else {
			if (accessRights != null) {
				for (AccessRight right : accessRights) {
					boolean roleOk = false;
					switch (role) {
					case OWNER:
						if (right.getRole() == Role.OWNER) {
							roleOk = true;
						}
						break;
					case WRITE:
						if (right.getRole() == Role.OWNER
								|| right.getRole() == Role.WRITE) {
							roleOk = true;
						}
						break;
					case READ:
						if (right.getRole() == Role.OWNER
								|| right.getRole() == Role.WRITE
								|| right.getRole() == Role.READ) {
							roleOk = true;
						}
						break;
					case EXECUTE:
						if (right.getRole() == Role.OWNER
								|| right.getRole() == Role.WRITE
								|| right.getRole() == Role.READ
								|| right.getRole() == Role.EXECUTE) {
							roleOk = true;
						}
						break;
					case NONE:
						if (right.getRole() == Role.OWNER
								|| right.getRole() == Role.WRITE
								|| right.getRole() == Role.READ
								|| right.getRole() == Role.NONE) {
							roleOk = true;
						}
						break;
					}
					if (roleOk) {
						// check if user has right
						if ((right.getUserId() != null)
								&& right.getUserId().equals(
										user.getId().getUserId())) {
							hasRole = true;
							return hasRole;// since hasRole will stay true, we can exit now
						} else {
							// check if one of user's groups has right
							for (String group : user.getGroupsAndUpgrades()) {
								if ((right.getGroupId() != null)
										&& right.getGroupId().equals(group)) {
									hasRole = true;
									return hasRole;// since hasRole will stay true, we can exit now
								}
							}
						}
					}
				}
			}
		}
		return hasRole;
	}

	/**
	 * Set the Role of the current user for an object.
	 */
	public HasAccessRights setRole(AppContext ctx, HasAccessRights object) {
		if (ctx != null) {
			Role role = Role.NONE;
			User user = ctx.getUser();
			if (user.isSuperUser()) {
				role = Role.OWNER;
			} else {
				Set<AccessRight> accessRights = object.getAccessRights();
				if (accessRights != null) {
					for (AccessRight right : accessRights) {
						if ((right.getUserId() != null)
								&& right.getUserId().equals(user.getId().getUserId())) {
							// check user rights
							role = maxRole(role, right.getRole());
						} else {
							// check groups rights
							for (String group : user.getGroups()) {
								if ((right.getGroupId() != null)
										&& right.getGroupId().equals(group)) {
									role = maxRole(role, right.getRole());
								}
							}
						}
					}
				}
			}
			object.setUserRole(role);
		}
		return object;
	}

	private Role maxRole(Role role1, Role role2) {
		if (role1.ordinal() > role2.ordinal()) {
			return role1;
		} else {
			return role2;
		}
	}

	/**
	 * Check if the context user has a ROLE over an object.
	 * 
	 * @param ctx
	 *            AppContext of the caller
	 * @param object
	 *            to check access rights for.
	 * @param role
	 *            role required
	 * @throws InvalidCredentialsAPIException
	 *             if user hasn't required role.
	 */
	public void checkRole(AppContext ctx, Persistent<?> object, Role role) {
		Role objectRole = object.getUserRole();
		boolean hasRole;
		if (objectRole != null) {
			// use object's role
			hasRole = (maxRole(role, objectRole) == objectRole);
		} else {
			// use object's access rights
			hasRole = hasRole(ctx.getUser(), object.getAccessRights(), role);
		}
		// if not superuser and hasn't right
		if (!hasRole) {
			throw new InvalidCredentialsAPIException(
					"Insufficient privileges : caller hasn't " + role.name()
							+ " role on " + object.getId(), ctx.isNoError());
		}
	}

	public boolean hasRole(AppContext ctx, Persistent<?> object, Role role) {
		return hasRole(ctx.getUser(), object.getAccessRights(), role);
	}

	/**
	 * Get the role of a User on a Persistent.
	 * 
	 * @param ctx
	 * @param object
	 * @return
	 */
	public Role getRole(User user, Persistent<?> object) {
		Role role = null;
		if (user.isSuperUser()) {
			role = Role.OWNER;
		} else {
			Set<AccessRight> accessRights = object.getAccessRights();
			for (AccessRight right : accessRights) {
				if (right.getUserId() != null) {
					if (right.getUserId().equals(user.getOid())) {
						if ((role == null)
								|| (role.ordinal() < right.getRole().ordinal())) {
							role = right.getRole();
						}
					}
				} else {
					for (String group : user.getGroupsAndUpgrades()) {
						if ((right.getGroupId() != null)
								&& right.getGroupId().equals(group)) {
							if ((role == null)
									|| (role.ordinal() < right.getRole().ordinal())) {
								role = right.getRole();
							}
						}
					}
				}
			}
		}
		return role;
	}

	/**
	 * Compute the access rights to apply based on the current user rights.<br>
	 * 
	 * @param ctx
	 * @param accessRights
	 *            from rights
	 * @param newAccessRights
	 *            to rights
	 * @return a new set of AccessRights
	 */
	public Set<AccessRight> applyAccessRights(AppContext ctx,
			Set<AccessRight> accessRights, Set<AccessRight> newAccessRights) {

		Set<AccessRight> rightsToAdd = new HashSet<AccessRight>();
		Set<AccessRight> rightsToRemove = new HashSet<AccessRight>();
		Set<AccessRight> rights = new HashSet<AccessRight>(accessRights);

		User user = ctx.getUser();

		for (AccessRight newRight : newAccessRights) {
			if (!accessRights.contains(newRight)) {
				if (hasRole(user, newAccessRights, newRight.getRole())) {
					rightsToAdd.add(newRight);
				} else {
					throw new InvalidCredentialsAPIException(
							"Insufficient privileges : " + newRight.getRole()
									+ " required", ctx.isNoError());
				}
			}
		}

		if (!newAccessRights.isEmpty()) {
			for (AccessRight right : accessRights) {
				if (!newAccessRights.contains(right)) {
					if (hasRole(user, newAccessRights, right.getRole())) {
						rightsToRemove.add(right);
					} else {
						throw new InvalidCredentialsAPIException(
								"Insufficient privileges " + right.getRole()
										+ " required", ctx.isNoError());
					}
				}
			}
		}

		rights.addAll(rightsToAdd);
		rights.removeAll(rightsToRemove);

		return rights;
	}
	
	/**
	 * Set the access rights from current user's rights and the target object's
	 * parent rights.
	 * 
	 * @param ctx
	 *            user context
	 * @param target
	 *            object to set rights to
	 * @param parent
	 *            object's parent
	 */
	public void setAccessRights(AppContext ctx, Persistent<?> target,
			Persistent<?> parent) {
		// target object is only visible from this thread so we can safely modify its properties
		Set<AccessRight> accessRights = target.getAccessRights();
		// parent object may be shared with the Universe - do not alter, us a copy
		Set<AccessRight> parentAccessRights = new HashSet<>(parent.getAccessRights());
		User currentUser = ctx.getUser();
		// add the current user as owner (if not super user)
		if (!currentUser.isSuperUser()) {
			// first remove any existing rights from the user
			for (Iterator<AccessRight> i = parentAccessRights.iterator(); i
					.hasNext();) {
				AccessRight r = i.next();
				if (currentUser.getId().getUserId().equals(r.getUserId())) {
					i.remove();
				}
				// removing right with EXECUTE
				else if (Role.EXECUTE.equals(r.getRole())) {
					i.remove();
				}
			}
			// add the current user
			AccessRight ownerRight = new AccessRight();
			ownerRight.setRole(Role.OWNER);
			ownerRight.setUserId(ctx.getUser().getId().getUserId());
			parentAccessRights.add(ownerRight);
		}
		Set<AccessRight> newAccessRights = AccessRightsUtils.getInstance()
				.applyAccessRights(ctx, target.getAccessRights(),
						parentAccessRights);
		accessRights.addAll(newAccessRights);
		newAccessRights = AccessRightsUtils.getInstance().applyAccessRights(
				ctx, newAccessRights, accessRights);
		target.setAccessRights(newAccessRights);
	}

}
