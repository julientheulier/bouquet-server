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
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Optional;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.InvalidIdAPIException;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.caching.Cache;
import com.squid.kraken.v4.caching.CacheFactoryEHCache;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.CustomerPK;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.model.Shortcut;
import com.squid.kraken.v4.model.ShortcutPK;
import com.squid.kraken.v4.model.State;
import com.squid.kraken.v4.model.StatePK;
import com.squid.kraken.v4.persistence.AccessRightsPersistentDAO;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.DataStore;
import com.squid.kraken.v4.persistence.DataStoreEvent;
import com.squid.kraken.v4.persistence.DataStoreEventObserver;
import com.squid.kraken.v4.persistence.DataStoreFilterOperator;
import com.squid.kraken.v4.persistence.DataStoreQueryField;

public class ShortcutDAO extends
		AccessRightsPersistentDAO<Shortcut, ShortcutPK> implements
		DataStoreEventObserver {

	private final Cache<CustomerPK, List<ShortcutPK>> findByParentCache;

	public ShortcutDAO(DataStore ds) {
		super(Shortcut.class, ds);
		findByParentCache = CacheFactoryEHCache.getCacheFactory()
				.getCollectionsCache(ShortcutPK.class, "findByParent");
	}

	public List<Shortcut> findByParent(AppContext app, CustomerPK parentId) {
		List<DataStoreQueryField> queryFields = new LinkedList<DataStoreQueryField>();
		queryFields.add(new DataStoreQueryField("id.customerId", parentId
				.getCustomerId()));
		return super.find(app, parentId, queryFields, findByParentCache);
	}
	
	public List<Shortcut> findByOwner(AppContext app) {
		List<DataStoreQueryField> queryFields = new LinkedList<DataStoreQueryField>();
        List<DataStoreFilterOperator> operators = new ArrayList<DataStoreFilterOperator>();
        List<AccessRight> rights = new ArrayList<AccessRight>();
        rights.add(new AccessRight(Role.OWNER, app.getUser().getOid(), null));
        queryFields.add(new DataStoreQueryField("accessRights", rights));
        operators.add(DataStoreFilterOperator.IN);
        return super.find(app, null, queryFields, operators, null);
	}

	/**
	 * No Role is required to perform the operation.<br>
	 */
	@Override
	public Optional<Shortcut> read(AppContext ctx, ShortcutPK id) {
		return ds.read(ctx, type, id);
	}

	/**
	 * No Role is required to perform the operation.<br>
	 */
	@Override
	public Shortcut readNotNull(AppContext ctx, ShortcutPK id) {
		return ds.readNotNull(ctx, type, id);
	}
	
    /**
     * No Role is required to perform a creation operation.<br>
     */
	@Override
	public Shortcut create(AppContext ctx, Shortcut newInstance) {
		// create
		Persistent<? extends GenericPK> parent = newInstance
				.getParentObject(ServiceUtils.getInstance().getRootUserContext(ctx));
		// set the access rights
		AccessRightsUtils.getInstance().setAccessRights(ctx, newInstance, parent);
		Shortcut shortcut = ds.create(ctx, newInstance);
		if (shortcut.getStateId() != null) {
			updateAssociatedState(ctx, shortcut);
		}
		return shortcut;
	}

	@Override
	public void update(AppContext ctx, Shortcut shortcut) {
		unAssociateState(ctx, shortcut.getId(), shortcut.getStateId());
		super.update(ctx, shortcut);
		if (shortcut.getStateId() != null) {
			updateAssociatedState(ctx, shortcut);
		}
	}

	@Override
	public void delete(AppContext ctx, ShortcutPK id) {
		unAssociateState(ctx, id, null);
		super.delete(ctx, id);
	}

	@Override
	public void notifyEvent(DataStoreEvent event) {
		ShortcutPK id = null;
		if (event.getSource() instanceof ShortcutPK) {
			// deletion
			id = (ShortcutPK) event.getSource();
			instanceCache.remove(id);
		}
		if (event.getSource() instanceof Shortcut) {
			// creation or update
			Shortcut source = (Shortcut) event.getSource();
			id = source.getId();
			instanceCache.put(id, source);
		}
		if (id != null) {
			// finder cache invalidation
			findByParentCache.remove(new CustomerPK(id.getCustomerId()));
		}
	}
	
	private void unAssociateState(AppContext ctx, ShortcutPK shortcutPk, String stateId) {
		Shortcut shortcut = DAOFactory.getDAOFactory().getDAO(Shortcut.class).readNotNull(ctx, shortcutPk);
		if ((shortcut.getStateId() != null) && (!shortcut.getStateId().equals(stateId))) {
			StatePK statePk = new StatePK(shortcut.getCustomerId(), shortcut.getStateId());
			Optional<State> read = DAOFactory.getDAOFactory().getDAO(State.class).read(ctx, statePk);
			if (read.isPresent()) {
				State state = read.get();
				state.setShortcutId(null);
				ds.update(ctx, state);
			}
		}
	}
	
	private void updateAssociatedState(AppContext ctx, Shortcut shortcut) {
		StatePK statePk = new StatePK(shortcut.getCustomerId(), shortcut.getStateId());
		Optional<State> read = DAOFactory.getDAOFactory().getDAO(State.class).read(ctx, statePk);
		if (read.isPresent()) {
			State state = read.get();
			if (state.getShortcutId()==null) {
				// no one has attached the shortcut
				state.setShortcutId(shortcut.getOid());
				ds.update(ctx, state);
			} else {
				// if it is not orphan nor mine, check if I can claim it
				if (!state.getShortcutId().equals(shortcut.getOid())) {
					ShortcutPK shortcutPk = new ShortcutPK(ctx.getCustomerId(), state.getShortcutId());
					Optional<Shortcut> check = DAOFactory.getDAOFactory().getDAO(Shortcut.class).read(ctx, shortcutPk);
					if (!check.isPresent()) {
						// ok you can claim it
						state.setShortcutId(shortcut.getOid());
						ds.update(ctx, state);
					} else {
						throw new InvalidIdAPIException("the stateID is already used by another shortcut, you cannot stole it",true);
					}
				}
			}
			// the state is mine, update it if needed
			if (shortcut.getState()!=null) {
				// update the new state
				state = shortcut.getState();
				state.setShortcutId(shortcut.getOid());// make sure it's linked
				ds.update(ctx, state);
			}
		} else {
			if (shortcut.getState()==null) {
				// you are trying to create a shortcut with no state!
				throw new ObjectNotFoundAPIException("cannot the shortcut stateID, and no state provided (use deepread to read the shortcut state)",true);
			}
			// write the new state
			State state = shortcut.getState();
			state.getId().setStateId(shortcut.getStateId());// make sure it i the right one
			state.setShortcutId(shortcut.getOid());// make sure it's linked
			ds.create(ctx, state);
		}
	}

}
