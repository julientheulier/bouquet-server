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

import org.mongodb.morphia.Datastore;

import com.google.common.base.Optional;
import com.squid.kraken.v4.caching.Cache;
import com.squid.kraken.v4.caching.CacheFactoryEHCache;
import com.squid.kraken.v4.model.LString;
import com.squid.kraken.v4.model.LStringPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.MongoDBHelper;

public class LStringDAO {
    
    static private LStringDAO instance;

    protected Cache<LStringPK, LString> cache;
    
    static synchronized public LStringDAO getInstance() {
        if (instance == null) {
            instance = new LStringDAO();
        }
        return instance;
    }

    private LStringDAO() {
        this.cache = CacheFactoryEHCache.getCacheFactory().getCache(LString.class);
    }

    public Optional<LString> read(AppContext app, LStringPK id) {
        return read(id);
    }

    public Optional<LString> read(LStringPK id) {
        Optional<LString> object = cache.getOptional(id);
        if (object == null) {
            LString dbobject = MongoDBHelper.getDatastore().get(LString.class, id.toUUID());
            cache.put(id, dbobject);
            object = Optional.fromNullable(dbobject);
        }
        return object;
    }

    public LString create(AppContext ctx, LString newInstance) {
        if (!ctx.isDryRun()) {
            MongoDBHelper.getDatastore().save(newInstance);
            cache.put(newInstance.getId(), newInstance);
        }
        return newInstance;
    }

    public void delete(AppContext ctx, LStringPK id) {
        Datastore ds = MongoDBHelper.getDatastore();
        if (!ctx.isDryRun()) {
            ds.delete(read(id));
            cache.remove(id);
        }
    }
}
