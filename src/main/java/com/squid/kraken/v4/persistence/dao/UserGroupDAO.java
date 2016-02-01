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

import java.util.List;

import com.google.common.base.Optional;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.InvalidCredentialsAPIException;
import com.squid.kraken.v4.caching.Cache;
import com.squid.kraken.v4.caching.CacheFactoryEHCache;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.CustomerPK;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.model.UserGroup;
import com.squid.kraken.v4.model.UserGroupPK;
import com.squid.kraken.v4.persistence.AccessRightsPersistentDAO;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DataStore;
import com.squid.kraken.v4.persistence.DataStoreEvent;

public class UserGroupDAO extends
		AccessRightsPersistentDAO<UserGroup, UserGroupPK> {

	private Cache<CustomerPK, List<UserGroupPK>> findByCustomerCache;

	public UserGroupDAO(DataStore ds) {
		super(UserGroup.class, ds);
		this.findByCustomerCache = CacheFactoryEHCache.getCacheFactory()
				.getCollectionsCache(UserGroupPK.class, "findByCustomer");
	}

	public List<UserGroup> findByCustomer(AppContext ctx, CustomerPK customerId) {
		return super.find(ctx, customerId, null, findByCustomerCache);
	}

	@Override
	public Optional<UserGroup> read(AppContext ctx, UserGroupPK id) {
		Optional<UserGroup> object = ds.read(ctx, type, id);
		if (object.isPresent()) {
			applyUserRigths(ctx, object.get());
		}
		return object;
	}

	@Override
	public UserGroup readNotNull(AppContext ctx, UserGroupPK id) {
		UserGroup object = ds.readNotNull(ctx, type, id);
		applyUserRigths(ctx, object);
		return object;
	}

	/**
	 * Implement specific access rules :
	 * <ul>
	 * <li>READ right on Customer is required</li>
	 * <li>the usergroup will inherit the rights from its parent (Customer).</li>
	 * </ul>
	 */
	private void applyUserRigths(AppContext ctx, UserGroup object) {
		Persistent<? extends GenericPK> parent = null;
		Role role;
		try {
			parent = object.getParentObject(ctx);
			role = AccessRightsUtils.getInstance().getRole(ctx.getUser(), parent);
			object.setUserRole(role);
		} catch (InvalidCredentialsAPIException e) {
			role = null;
		}
		// set the parent's role as userRole
		if ((role == null) || (role.ordinal()<Role.READ.ordinal())) {
			// if the user is member of this group
			for (String groutOid : ctx.getUser().getGroups()) {
				if (groutOid.equals(object.getOid())) {
					object.setUserRole(Role.READ);
				}
			}
		}
		// check the READ access rights
		AccessRightsUtils.getInstance().checkRole(ctx, object, Role.READ);

	}

	@Override
	public void notifyEvent(DataStoreEvent event) {
		UserGroupPK id = null;
		if (event.getSource() instanceof UserGroupPK) {
			// deletion
			id = (UserGroupPK) event.getSource();
			instanceCache.remove(id);
		}
		if (event.getSource() instanceof UserGroup) {
			// creation or update
			UserGroup source = (UserGroup) event.getSource();
			id = source.getId();
			instanceCache.put(id, source);
		}
		if (id != null) {
			// finder cache invalidation
			findByCustomerCache.remove(new CustomerPK(id.getCustomerId()));
		}
	}

}
