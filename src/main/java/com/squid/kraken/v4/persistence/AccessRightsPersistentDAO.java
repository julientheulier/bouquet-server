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
package com.squid.kraken.v4.persistence;

import java.util.Set;

import com.google.common.base.Optional;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;

/**
 * Implements default access rights check for basic CRUD operations.<br>
 */
public abstract class AccessRightsPersistentDAO<T extends Persistent<PK>, PK extends GenericPK>
		extends PersistentDAO<T, PK> {

	public AccessRightsPersistentDAO(Class<T> type, DataStore ds) {
		super(type, ds);
	}

    /**
     * Role {@link Role#WRITE} on the parent object is required to perform the operation.<br>
     * Access rights are inherited from its parent (see
     * {@link AccessRightsUtils#setAccessRights(AppContext, Persistent, Persistent)}).<br>
     */
	@Override
	public T create(AppContext ctx, T newInstance) {
		// check the access rights
		Persistent<? extends GenericPK> parent = newInstance
				.getParentObject(ctx);

		// need write role on parent
		AccessRightsUtils.getInstance().checkRole(ctx, parent, Role.WRITE);

		// set the access rights
		AccessRightsUtils.getInstance().setAccessRights(ctx, newInstance, parent);

		return super.create(ctx, newInstance);
	}

    /**
     * Role {@link Role#READ} is required to perform the operation.<br>
     */
	@Override
	public Optional<T> read(AppContext ctx, PK id) {
		Optional<T> object = ds.read(ctx, type, id);
		if (object.isPresent()) {
			// check the access rights
			AccessRightsUtils.getInstance().checkRole(ctx, object.get(),
					Role.READ);
		}
		return super.read(ctx, id);
	}

    /**
     * Role {@link Role#READ} is required to perform the operation.<br>
     */
	@Override
	public T readNotNull(AppContext ctx, PK id) {
		T object = ds.readNotNull(ctx, type, id);
		// check the access rights
		AccessRightsUtils.getInstance().checkRole(ctx, object, Role.READ);
		return super.readNotNull(ctx, id);
	}

    /**
     * Role {@link Role#WRITE} on the object is required to perform the operation.<br>
     */
	@Override
	public void update(AppContext ctx, T newInstance) {
		T toUpdate = ds.read(ctx, type, newInstance.getId()).get();

		// check the access rights
		AccessRightsUtils.getInstance().checkRole(ctx, toUpdate, Role.WRITE);

		// set the access rights
		Set<AccessRight> newAccessRights = AccessRightsUtils.getInstance()
				.applyAccessRights(ctx, toUpdate.getAccessRights(),
						newInstance.getAccessRights());
		newInstance.setAccessRights(newAccessRights);

		super.update(ctx, newInstance);
	}

    /**
     * Role {@link Role#WRITE} is required to perform the operation.<br>
     */
	@Override
	public void delete(AppContext ctx, PK id) {
		Optional<T> object = read(ctx, id);
		if (object.isPresent()) {
			// check the access rights
			AccessRightsUtils.getInstance().checkRole(ctx, object.get(),
					Role.WRITE);
			super.delete(ctx, id);
		}
	}

}
