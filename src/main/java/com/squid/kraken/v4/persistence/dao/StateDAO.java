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

import org.mongodb.morphia.query.Query;

import com.google.common.base.Optional;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.caching.Cache;
import com.squid.kraken.v4.caching.CacheFactoryEHCache;
import com.squid.kraken.v4.model.CustomerPK;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.model.State;
import com.squid.kraken.v4.model.StatePK;
import com.squid.kraken.v4.persistence.AccessRightsPersistentDAO;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DataStore;
import com.squid.kraken.v4.persistence.DataStoreEvent;
import com.squid.kraken.v4.persistence.DataStoreEventObserver;
import com.squid.kraken.v4.persistence.MongoDBHelper;

public class StateDAO extends
		AccessRightsPersistentDAO<State, StatePK> implements
		DataStoreEventObserver, ExpirableDAO<State> {

	private final Cache<CustomerPK, List<StatePK>> findByParentCache;

	public StateDAO(DataStore ds) {
		super(State.class, ds);
		findByParentCache = CacheFactoryEHCache.getCacheFactory()
				.getCollectionsCache(StatePK.class, "findByClient");
	}
	
    public List<State> findAllExpired(long expDate) {
        Query<State> q = MongoDBHelper.getDatastore().createQuery(type);
        q.field("creationTime").lessThan(expDate);
        q.field("shortcutId").doesNotExist();
        return q.asList();
    }
    
    public List<State> findByParent(AppContext app, CustomerPK parentId) {
        Query<State> q = MongoDBHelper.getDatastore().createQuery(type);
        q.field("shortcutId").exists();
        return q.asList();
    }

    /**
     * No Role is required to perform the operation.<br>
     */
	@Override
	public Optional<State> read(AppContext ctx, StatePK id) {
		return ds.read(ctx, type, id);
	}

    /**
     * No Role is required to perform the operation.<br>
     */
	@Override
	public State readNotNull(AppContext ctx, StatePK id) {
		return ds.readNotNull(ctx, type, id);
	}

	/**
     * No Role is required to perform a creation operation<br>
     */
	@Override
	public State create(AppContext ctx, State newInstance) {
		if (newInstance.getCreationTime() == null) {
			newInstance.setCreationTime(System.currentTimeMillis());
		}
		// create
		Persistent<? extends GenericPK> parent = newInstance
				.getParentObject(ServiceUtils.getInstance().getRootUserContext(ctx));
		// set the access rights
		AccessRightsUtils.getInstance().setAccessRights(ctx, newInstance, parent);
		return ds.create(ctx, newInstance);
	}
	
    /**
     * No update is supported<br>
     */
	@Override
	public void update(AppContext ctx, State newInstance) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void notifyEvent(DataStoreEvent event) {
		StatePK id = null;
		if (event.getSource() instanceof StatePK) {
			// deletion
			id = (StatePK) event.getSource();
			instanceCache.remove(id);
		}
		if (event.getSource() instanceof State) {
			// creation or update
			State source = (State) event.getSource();
			id = source.getId();
			instanceCache.put(id, source);
		}
		if (id != null) {
			// finder cache invalidation
			findByParentCache.remove(new CustomerPK(id.getCustomerId()));
		}
	}

}
