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

import com.google.common.base.Optional;

/**
 * A persistent object cache.
 * 
 * @param <K>
 *            key
 * @param <V>
 *            value
 */
public interface Cache<K, V> {

    /**
     * Get an object from the cache given a Key.
     * 
     * @param key
     * @return the object found or <tt>null</tt> if the object wasn't found or its value was <tt>null</tt>.
     */
    public V get(K key);

    /**
     * Getter version returning an Optional object if the object found has a null value.
     * 
     * @param key
     * @return the object found as an Optional or <tt>null</tt> if the object wasn't found.
     */
    public Optional<V> getOptional(K key);

    public boolean contains(K key);

    public void remove(K key);

    public void removeAll();

    public void put(K key, V value);

    public String getStatistics();

}