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

import java.util.List;

import org.bson.types.ObjectId;

import com.google.common.base.Optional;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.caching.Cache;
import com.squid.kraken.v4.caching.CacheFactoryEHCache;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;

/**
 * Base decorator for a DataStore<br>
 * Implements automated instances caching and update notifications through the {@link DataStoreEventBus}.<br>
 * Implements {@link AppContext#isDryRun()} option.
 */
public class GenericDataStoreDecorator implements DataStore {

    private final DataStore ds;

    /**
     * Constructor
     * 
     * @param ds
     *            the DataStore to decorate.
     */
    public GenericDataStoreDecorator(DataStore ds) {
        this.ds = ds;
    }

    /**
     * Create a new DB record.<br>
     * Will check if the newInstance id is syntaxically valid.<br>
     * If the id's objectId is <tt>null</tt> a new uuid will be generated.<br>
     * If the AppContext is {@link AppContext#isDryRun()} then the DB update will not occur.<br>
     * The persistent cache will be updated.
     */
    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> T create(AppContext ctx, T newInstance) {
        // check the id
    	PK id = newInstance.getId();
        if (id.getObjectId() == null) {
            // generate a new oid
            id.setObjectId(ObjectId.get().toString());
            newInstance.setId(id);
        } else if (!id.isValid()) {
            throw new APIException("Object Id "+id+" doesn't match pattern : " + ServiceUtils.ID_VALIDATION_REGEX, ctx
                    .isNoError());
        }
        
        // save
        if (!ctx.isDryRun()) {
            // in DB
            ds.create(ctx, newInstance);
            // publish
            DataStoreEventBus.getInstance().publishEvent(new DataStoreEvent(newInstance, DataStoreEvent.Type.CREATION));
        }
        return newInstance;
    }

    /**
     * Delete a DB record.<br>
     * If the AppContext is {@link AppContext#isDryRun()} then the DB update will not occur.<br>
     * The persistent cache will be updated.
     */
    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> void delete(AppContext ctx, Class<T> type, PK id) {
        if (!ctx.isDryRun()) {
            // delete
            // in DB
            ds.delete(ctx, type, id);
            // publish
            DataStoreEventBus.getInstance().publishEvent(new DataStoreEvent(id, DataStoreEvent.Type.DELETE));
        }
    }

    /**
     * Read a DB record.<br>
     * The persistent cache will be updated if the object could be first read from it.
     * 
     * @return the object wrapped as Optional to handle not found (null) case.
     */
    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> Optional<T> read(AppContext ctx, Class<T> type, PK objectId) {
        T object = null;
        if (objectId != null) {
            Cache<PK, T> cache = CacheFactoryEHCache.getCacheFactory().getCache(type);
            object = cache.get(objectId);
            if (object == null) {
                Optional<T> daoObject = ds.read(ctx, type, objectId);
                if (daoObject.isPresent()) {
                    object = daoObject.get();
                    cache.put(objectId, object);
                }
            }
        }
        if (object != null) {
        	// set the Role
        	AccessRightsUtils.getInstance().setRole(ctx, object);
            return Optional.of(object);
        } else {
            return Optional.absent();
        }
    }

    /**
     * Read a DB record (non <tt>null</tt> version).<br>
     * The persistent cache will be updated if the object could be first read from it.
     * 
     * @return the object found or throws a {@link ObjectNotFoundAPIException}
     */
    @Override
    final public <T extends Persistent<PK>, PK extends GenericPK> T readNotNull(AppContext ctx, Class<T> type, PK objectId) {
        T object = null;
        if (objectId != null) {
            Cache<PK, T> cache = CacheFactoryEHCache.getCacheFactory().getCache(type);
            object = cache.get(objectId);
            if (object == null) {
                object = ds.readNotNull(ctx, type, objectId);
                cache.put(objectId, object);
            }
        }
        // set the Role
    	AccessRightsUtils.getInstance().setRole(ctx, object);
        return object;
    }

    /**
     * Update a DB record.<br>
     * If the AppContext is {@link AppContext#isDryRun()} then the DB update will not occur.<br>
     * The persistent cache will be updated.
     */
    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> void update(AppContext ctx, T newInstance) {
        // save
        if (!ctx.isDryRun()) {
            // in DB
            ds.update(ctx, newInstance);
            // publish
            DataStoreEventBus.getInstance().publishEvent(new DataStoreEvent(newInstance, DataStoreEvent.Type.UPDATE));
        }
    }

    public <T extends Persistent<PK>, PK extends GenericPK> boolean exists(AppContext ctx, Class<T> type, PK objectId) {
        boolean exists;
        Cache<PK, T> cache = CacheFactoryEHCache.getCacheFactory().getCache(type);
        // check exists
        exists = cache.contains(objectId);
        if (!exists) {
            exists = ds.exists(ctx, type, objectId);
        }
        return exists;
    }

    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> List<T> find(AppContext app, Class<T> type,
            List<DataStoreQueryField> queryFields, List<DataStoreFilterOperator> filterOperators) {
        return find(app, type, queryFields, filterOperators, null);
    }

    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> List<T> find(AppContext app, Class<T> type,
            List<DataStoreQueryField> queryFields, List<DataStoreFilterOperator> filterOperators, String orderBy) {
        return ds.find(app, type, queryFields, filterOperators, orderBy);
    }

}
