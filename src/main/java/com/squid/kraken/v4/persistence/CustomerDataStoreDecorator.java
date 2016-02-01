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

import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Optional;
import com.squid.kraken.v4.model.CustomerPK;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;

/**
 * Customer decorator for a DataStore<br>
 * Forces the Object PK with the Customer id found in the {@link AppContext}.
 */
public class CustomerDataStoreDecorator implements DataStore {

    private final DataStore ds;

    public CustomerDataStoreDecorator(DataStore ds) {
        this.ds = ds;
    }

    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> T create(AppContext ctx, T newInstance) {
        // force the customerId
        newInstance.setCustomerId(ctx.getCustomerId());
        return ds.create(ctx, newInstance);
    }

    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> void delete(AppContext ctx, Class<T> type, PK id) {
        // set the customerId
        ((CustomerPK) id).setCustomerId(ctx.getCustomerId());
        ds.delete(ctx, type, id);
    }

    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> Optional<T> read(AppContext ctx, Class<T> type, PK id) {
        // set the customerId
        ((CustomerPK) id).setCustomerId(ctx.getCustomerId());
        return ds.read(ctx, type, id);
    }

    @Override
    final public <T extends Persistent<PK>, PK extends GenericPK> T readNotNull(AppContext ctx, Class<T> type, PK id) {
        // set the customerId
        ((CustomerPK) id).setCustomerId(ctx.getCustomerId());
        return ds.readNotNull(ctx, type, id);
    }

    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> void update(AppContext ctx, T transientObject) {
        // force the customerId
        transientObject.setCustomerId(ctx.getCustomerId());

        ds.update(ctx, transientObject);

    }

    public <T extends Persistent<PK>, PK extends GenericPK> boolean exists(AppContext ctx, Class<T> type, PK objectId) {
        ((CustomerPK) objectId).setCustomerId(ctx.getCustomerId());
        return ds.exists(ctx, type, objectId);
    }

    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> List<T> find(AppContext app, Class<T> type,
            List<DataStoreQueryField> queryFields, List<DataStoreFilterOperator> filterOperators) {
        return find(app, type, queryFields, filterOperators, null);
    }

    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> List<T> find(AppContext app, Class<T> type,
            List<DataStoreQueryField> queryFields, List<DataStoreFilterOperator> filterOperators, String orderBy) {
        if (queryFields == null) {
            queryFields = new LinkedList<DataStoreQueryField>();
        }
        if (filterOperators != null) {
            filterOperators.add(0, DataStoreFilterOperator.EQUAL);
        }
        queryFields.add(0, new DataStoreQueryField("id.customerId", app.getCustomerId()));
        return ds.find(app, type, queryFields, filterOperators, orderBy);
    }
}
