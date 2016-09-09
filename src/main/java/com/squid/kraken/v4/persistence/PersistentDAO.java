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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Optional;
import com.squid.kraken.v4.api.core.InvalidCredentialsAPIException;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.caching.Cache;
import com.squid.kraken.v4.caching.CacheFactoryEHCache;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.CustomerPK;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.model.visitor.DeepReadVisitor;

/**
 * DAO base abstract implementation.<br>
 * Handles instances caching and invalidation through the {@link DataStoreEventBus}.
 * 
 * @param <T>
 *            Object Type
 * @param <PK>
 *            Object PK
 */
public abstract class PersistentDAO<T extends Persistent<PK>, PK extends GenericPK> implements GenericDAO<T, PK>, DataStoreEventObserver {

    protected final Class<T> type;

    protected final DataStore ds;
    
    protected final Cache<PK, T> instanceCache;

    public PersistentDAO(Class<T> type, DataStore ds) {
        this.type = type;
        this.ds = ds;
        DataStoreEventBus.getInstance().subscribe(this);
        instanceCache = CacheFactoryEHCache.getCacheFactory().getCache(type);
    }

    @Override
    public T create(AppContext ctx, T newInstance) {
        ds.create(ctx, newInstance);
        return newInstance;
    }

    @Override
    public void delete(AppContext ctx, PK id) {
        ds.delete(ctx, type, id);
    }

    /**
     * Read a DB record.<br>
     * The customerId will be forced to AppContext's customerId.<br>
     * Role {@link Role#READ} is required to perform the operation.<br>
     * The persistent cache will be updated if the object could be first read from it.
     * 
     * @return the object wrapped as Optional to handle not found (null) case.
     */
    @Override
    public Optional<T> read(AppContext ctx, PK id) {
        return ds.read(ctx, type, id);
    }

    @Override
    public boolean exists(AppContext ctx, PK id) {
        return ds.exists(ctx, type, id);
    }

    /**
     * Read a DB record (non <tt>null</tt> version).<br>
     * 
     * @see PersistentDAO#read(AppContext, CustomerPK)
     * 
     * @return the object found or throws a {@link ObjectNotFoundAPIException}
     */
    @Override
    public T readNotNull(AppContext ctx, PK id) {
        return ds.readNotNull(ctx, type, id);
    }

    /**
     * Update a DB record.<br>
     * The customerId will be forced to AppContext's customerId.<br>
     * Role {@link Role#WRITE} on the object is required to perform the operation.<br>
     * If the AppContext is {@link AppContext#isDryRun()} then the DB update will not occur.<br>
     * The persistent cache will be updated.
     */
    @Override
    public void update(AppContext ctx, T transientObject) {
        ds.update(ctx, transientObject);
    }

    /**
     * Generic finder handling caching.<br>
     * Performs DB find query, caches the results if cache parameter is not null.
     * 
     * @param <K>
     *            the cache key type
     * @param app
     *            context
     * @param objectId
     *            the cache key
     * @param queryFields
     *            the fields to build the query
     * @param cache
     *            a cache to put result list in
     * @return a filtered list
     */
    public <K extends GenericPK> List<T> find(AppContext app, K objectId,
            List<DataStoreQueryField> queryFields, Cache<K, List<PK>> cache) {
        return find(app, objectId, queryFields, null, cache);
    }
    
    public <K extends GenericPK> List<T> find(AppContext app, K objectId,
            List<DataStoreQueryField> queryFields, Cache<K, List<PK>> cache, String orderBy) {
        return find(app, objectId, queryFields, null, cache, orderBy);
    }
    
    public <K extends GenericPK> List<T> find(AppContext app, K objectId, List<DataStoreQueryField> queryFields,
            List<DataStoreFilterOperator> filterOperators, Cache<K, List<PK>> cache) {
        return find(app, objectId, queryFields, filterOperators, cache, null);
    }

    /**
     * Generic finder handling caching.<br>
     * Performs DB find query, caches the results if cache parameter is not null.
     * 
     * @param <K>
     *            the cache key type
     * @param app
     *            context
     * @param objectId
     *            the cache key
     * @param queryFields
     *            the fields to build the query
     * @param filterOperators
     *            a list of filter operators ("=", ">", ">=", "<", "<=", "<>", "in", "not in", "all")
     * @param cache
     *            a cache to put result list in
     * @param orderBy
     *            order the result. Example:
     *            <ul>
     *            <li>age</li>
     *            <li>-age (descending order)</li>
     *            <li>age,date</li>
     *            <li>age,-date (age ascending, date descending)</li>
     *            </ul>
     * @return a filtered list
     */
	public <K extends GenericPK> List<T> find(AppContext app, K objectId, List<DataStoreQueryField> queryFields,
			List<DataStoreFilterOperator> filterOperators, Cache<K, List<PK>> cache, String orderBy) {
		
        List<PK> pkList = null;
        if (cache != null) {
        	pkList = cache.get(objectId);
        }
        if (pkList == null) {
            // execute the query
        	List<T> list = ds.find(app, type, queryFields, filterOperators, orderBy);
            // put in cache
            pkList = toPKList(list);
            if (cache != null) {
            	cache.put(objectId, pkList);
            }
        }
        // get persistent objects
        List<T> list;
        list = toPersistentList(app, pkList);
        return list;
    }    

    private List<PK> toPKList(Collection<T> list) {
        List<PK> pkList = new ArrayList<PK>();
        for (T o : list) {
            pkList.add(o.getId());
        }
        return pkList;
    }

    private List<T> toPersistentList(AppContext ctx, Collection<PK> list) {
        List<T> pList = new ArrayList<T>();
        for (PK o : list) {
            try {
                Optional<T> read = read(ctx, o);
                if (read.isPresent()) {
                	T object = read.get();
                	if (ctx.isDeepRead()) {
                		DeepReadVisitor v1 = new DeepReadVisitor(ctx);
                		object.accept(v1);
                	}
                    pList.add(object);
                }
            } catch (InvalidCredentialsAPIException e) {
                // no read right, ignore this item
            }
        }
        return pList;
    }

    @Override
    public Class<T> getType() {
        return type;
    }

}
