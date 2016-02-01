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
package com.squid.kraken.v4.caching;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.squid.kraken.v4.model.GenericPK;

public abstract class CacheFactory {

    // List of types supported by the factory
    public static final int EHCACHE = 1;

    private static Map<Integer, CacheFactory> factories;

    public static CacheFactory getCacheFactory() {
        return getCacheFactory(EHCACHE);
    }

    public synchronized static CacheFactory getCacheFactory(int whichFactory) {
        if (factories == null) {
            // may happen when tomcat is reloading the webapp
            factories = new HashMap<Integer, CacheFactory>();
        }
        CacheFactory f = factories.get(whichFactory);
        if (f == null) {
            switch (whichFactory) {
            case EHCACHE:
                f = new CacheFactoryEHCache();
                factories.put(whichFactory, f);
            }
        }
        return f;
    }
    
    /////////////////
    
    /**
     * Generic cache for Persistent objects.
     * 
     * @param <K>
     *            the Key type
     * @param <V>
     *            the Value type
     * @param valueType
     * @return
     */
    public abstract <K extends GenericPK, V> Cache<K, V> getCache(Class<V> valueType);

    /**
     * Generic cache for Persistent objects collections.
     * 
     * @param <K>
     *            the Key type
     * @param <V>
     *            the Value type
     * @param valueType
     * @param collectionName
     *            the collection name
     * @return
     */
    public abstract <K extends GenericPK, V> Cache<K, List<V>> getCollectionsCache(Class<V> valueType,
            String collectionName);

    public abstract void shutdown();
    
    public abstract void clearAll();
}