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
import com.squid.kraken.v4.caching.Cache;
import com.squid.kraken.v4.caching.CacheFactoryEHCache;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Client;
import com.squid.kraken.v4.model.ClientPK;
import com.squid.kraken.v4.model.CustomerPK;
import com.squid.kraken.v4.persistence.AccessRightsPersistentDAO;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.DataStore;
import com.squid.kraken.v4.persistence.DataStoreEvent;

public class ClientDAO extends AccessRightsPersistentDAO<Client, ClientPK> {

	private Cache<CustomerPK, List<ClientPK>> findByCustomerCache;

	public ClientDAO(DataStore ds) {
		super(Client.class, ds);
		this.findByCustomerCache = CacheFactoryEHCache.getCacheFactory()
				.getCollectionsCache(ClientPK.class, "findByCustomer");
	}

	public List<Client> findAll(AppContext ctx) {
		return super.find(ctx, ctx.getCustomerPk(), null, findByCustomerCache);
	}

	@Override
	public void notifyEvent(DataStoreEvent event) {
		ClientPK id = null;
		if (event.getSource() instanceof ClientPK) {
			// deletion
			id = (ClientPK) event.getSource();
			instanceCache.remove(id);
		}
		if (event.getSource() instanceof Client) {
			// creation or update
			Client source = (Client) event.getSource();
			id = source.getId();
			instanceCache.put(id, source);
		}
		if (id != null) {
			// finder cache invalidation
			findByCustomerCache.remove(new CustomerPK(id.getCustomerId()));
		}
	}

	@Override
	public void update(AppContext ctx, Client transientObject) {
		Client existingClient = DAOFactory.getDAOFactory().getDAO(Client.class)
				.readNotNull(ctx, transientObject.getId());
		// force the secret
		transientObject.setSecret(existingClient.getSecret());
		transientObject.setJWTKeyPrivate(existingClient.getJWTKeyPrivate());
		transientObject.setJWTKeyPublic(existingClient.getJWTKeyPublic());
		super.update(ctx, transientObject);
	}

	/**
	 * Role {@link Role#READ} is required to perform the operation but
	 * {@link Role#WRITE} is required to view some attributes.<br>
	 */
	@Override
	public Optional<Client> read(AppContext ctx, ClientPK id) {
		Optional<Client> object = ds.read(ctx, type, id);
		if (object.isPresent()) {
			// check the access rights
			AccessRightsUtils.getInstance().checkRole(ctx, object.get(), Role.READ);
			applyFilters(ctx, object.get());
		}
		return object;
	}

	/**
	 * Role {@link Role#READ} is required to perform the operation but
	 * {@link Role#WRITE} is required to view some attributes.<br>
	 */
	@Override
	public Client readNotNull(AppContext ctx, ClientPK id) {
		Client object = ds.readNotNull(ctx, type, id);
		// check the access rights
		AccessRightsUtils.getInstance().checkRole(ctx, object, Role.READ);
		applyFilters(ctx, object);
		return object;
	}

	private void applyFilters(AppContext ctx, Client object) {
		boolean hasWriteRole = AccessRightsUtils.getInstance().hasRole(
				ctx.getUser(), object.getAccessRights(), Role.WRITE);
		if (!hasWriteRole) {
			// prevent from exposing some secret attributes
			object.setSecret(null);
			object.setUrls(null);
			object.setJWTKeyPrivate(null);
			object.setJWTKeyPublic(null);
		}
	}

}
