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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Customer;
import com.squid.kraken.v4.model.CustomerPK;
import com.squid.kraken.v4.persistence.AccessRightsPersistentDAO;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DataStore;
import com.squid.kraken.v4.persistence.DataStoreEvent;

public class CustomerDAO extends
		AccessRightsPersistentDAO<Customer, CustomerPK> {
	
    private List<Customer> findAllCache = Collections.synchronizedList(new ArrayList<Customer>());

	public CustomerDAO(DataStore ds) {
		super(Customer.class, ds);
	}
	
    public List<Customer> findAll(AppContext app) {
    	if (findAllCache.isEmpty()) {
    		List<Customer> find = ds.find(app, type, null, null);
    		this.findAllCache.addAll(find);
    	}
    	return findAllCache;
    }

	@Override
	public Customer create(AppContext ctx, Customer newInstance) {
		// set the access rights
		Set<AccessRight> newAccessRights = AccessRightsUtils.getInstance()
				.applyAccessRights(ctx, new HashSet<AccessRight>(),
						newInstance.getAccessRights());
		newInstance.setAccessRights(newAccessRights);

		return ds.create(ctx, newInstance);
	}

	/**
	 * Role {@link Role#OWNER} on the object is required to perform the operation.<br>
	 */
	@Override
	public void update(AppContext ctx, Customer newInstance) {
		Customer toUpdate = ds.read(ctx, type, newInstance.getId()).get();

		// check the access rights
		AccessRightsUtils.getInstance().checkRole(ctx, toUpdate, Role.OWNER);

		// set the access rights
		Set<AccessRight> newAccessRights = AccessRightsUtils.getInstance()
				.applyAccessRights(ctx, toUpdate.getAccessRights(),
						newInstance.getAccessRights());
		newInstance.setAccessRights(newAccessRights);

		ds.update(ctx, newInstance);
	}

    /**
     * Role {@link Role#OWNER} on the object is required to perform the operation.<br>
     */
	@Override
	public void delete(AppContext ctx, CustomerPK id) {
		Customer object = readNotNull(ctx, id);
		AccessRightsUtils.getInstance().checkRole(ctx, object, Role.OWNER);
		ds.delete(ctx, type, id);
	}

	@Override
	public void notifyEvent(DataStoreEvent event) {
		CustomerPK id = null;
		if (event.getSource().getClass().equals(CustomerPK.class)) {
			// deletion
			id = (CustomerPK) event.getSource();
			instanceCache.remove(id);
			findAllCache.clear();
		}
		if (event.getSource() instanceof Customer) {
			// creation or update
			Customer source = (Customer) event.getSource();
			id = source.getId();
			instanceCache.put(id, source);
			findAllCache.clear();
		}
	}

}
