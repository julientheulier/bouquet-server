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
import java.util.List;

import org.mongodb.morphia.query.Query;

import com.google.common.base.Optional;
import com.squid.kraken.v4.model.AccessToken;
import com.squid.kraken.v4.model.AccessTokenPK;
import com.squid.kraken.v4.model.UserPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DataStore;
import com.squid.kraken.v4.persistence.DataStoreEvent;
import com.squid.kraken.v4.persistence.DataStoreEventBus;
import com.squid.kraken.v4.persistence.DataStoreQueryField;
import com.squid.kraken.v4.persistence.MongoDBHelper;
import com.squid.kraken.v4.persistence.PersistentDAO;

public class AccessTokenDAO extends PersistentDAO<AccessToken, AccessTokenPK>
		implements ExpirableDAO<AccessToken> {

	public AccessTokenDAO(DataStore ds) {
		super(AccessToken.class, ds);
	}
	
	public Optional<AccessToken> findRefreshToken(String customerId, String clientId, String userId) {
		Query<AccessToken> q = MongoDBHelper.getDatastore().createQuery(type);
		q.field("customerId").equal(customerId);
		q.field("clientId").equal(clientId);
		q.field("userId").equal(userId);
		q.field("type").equal(AccessToken.Type.REFRESH);
		return Optional.fromNullable(q.get());
	}
	
	public Optional<AccessToken> findRefreshToken(String tokenId) {
		Query<AccessToken> q = MongoDBHelper.getDatastore().createQuery(type);
		q.field("_id").equal(tokenId);
		q.field("type").equal(AccessToken.Type.REFRESH);
		return Optional.fromNullable(q.get());
	}

	public List<AccessToken> findAllExpired(long expDate) {
		Query<AccessToken> q = MongoDBHelper.getDatastore().createQuery(type);
		q.field("expirationDateMillis").exists();
		q.field("expirationDateMillis").lessThan(expDate);
		return q.asList();
	}

	public Optional<AccessToken> read(AppContext app, AccessTokenPK id) {
		return read(id);
	}

	@Override
	public AccessToken readNotNull(AppContext ctx, AccessTokenPK id) {
		// TODO Auto-generated method stub
		return super.readNotNull(ctx, id);
	}

	public Optional<AccessToken> read(AccessTokenPK id) {
		Optional<AccessToken> object = instanceCache.getOptional(id);
		if (object == null) {
			object = ds.read(null, type, id);
			instanceCache.put(id, object.orNull());
		}
		return object;
	}

	public AccessToken create(AppContext ctx, AccessToken newInstance) {
		if (!ctx.isDryRun()) {
			ds.create(ctx, newInstance);
			// publish
			DataStoreEventBus.getInstance().publishEvent(
					new DataStoreEvent(ctx, newInstance,
							DataStoreEvent.Type.CREATION));
		}
		return newInstance;
	}

	public void delete(AppContext ctx, AccessTokenPK id) {
		if (!ctx.isDryRun()) {
			ds.delete(ctx, type, id);
			// publish
			DataStoreEventBus.getInstance().publishEvent(
					new DataStoreEvent(ctx, id, DataStoreEvent.Type.DELETE));
		}
	}

	@Override
	public boolean exists(AppContext ctx, AccessTokenPK id) {
		return read(id).isPresent();
	}

	@Override
	public Class<AccessToken> getType() {
		return AccessToken.class;
	}

	@Override
	public void update(AppContext ctx, AccessToken transientObject) {
		throw new UnsupportedOperationException();
	}

	public List<AccessToken> findByUser(AppContext app, UserPK userId) {
		List<DataStoreQueryField> queryFields = new ArrayList<DataStoreQueryField>(2);
		queryFields.add(new DataStoreQueryField("customerId", app
				.getCustomerId()));
		queryFields.add(new DataStoreQueryField("userId", userId.getUserId()));
		List<AccessToken> find = ds.find(app, type, queryFields, null);
		return find;
	}

	@Override
	public void notifyEvent(DataStoreEvent event) {
		AccessTokenPK sourceId = null;
		if (event.getSource() instanceof AccessTokenPK) {
			// deletion
			sourceId = (AccessTokenPK) event.getSource();
			instanceCache.remove(sourceId);
		}
		if (event.getSource() instanceof AccessToken) {
			// creation or update
			AccessToken source = (AccessToken) event.getSource();
			sourceId = source.getId();
			instanceCache.put(sourceId, source);
		}
	}

}
