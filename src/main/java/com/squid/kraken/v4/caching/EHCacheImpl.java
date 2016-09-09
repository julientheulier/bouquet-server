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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.ObjectExistsException;
import net.sf.ehcache.config.CacheConfiguration;

public class EHCacheImpl<K, V> implements Cache<K, V> {

	final Logger log = LoggerFactory.getLogger(EHCacheImpl.class);

	protected net.sf.ehcache.Cache cache;

	public EHCacheImpl(CacheManager cacheManager, String name) {
		this(cacheManager, name, false);
	}

	public EHCacheImpl(CacheManager cacheManager, String name,
			boolean copyOnReadAndWrite) {
		cache = cacheManager.getCache(name);
		if (cache == null) {
			try {
				cacheManager.addCache(name);
			} catch (ObjectExistsException e) {
				// cache alread exists.. ignore
				log.info("Cache " + name + " already exists ... ignoring");
			}
			cache = cacheManager.getCache(name);
		}
		if (copyOnReadAndWrite
				&& ((!cache.getCacheConfiguration().isCopyOnRead()) || (!cache
						.getCacheConfiguration().isCopyOnWrite()))) {
			log.warn("Forcing copyOnReadAndWrite for cache : " + name);
			cache.getCacheConfiguration().setCopyOnRead(true);
			cache.getCacheConfiguration().setCopyOnWrite(true);
			net.sf.ehcache.Cache clone = new net.sf.ehcache.Cache(
					cache.getCacheConfiguration());
			// recreate the cache
			cacheManager.removeCache(name);
			cacheManager.addCache(clone);
			cache = clone;
		}
		log.info("Created cache : " + name + " with isCopyOnRead: "
				+ cache.getCacheConfiguration().isCopyOnRead()
				+ " with isCopyOnWrite: "
				+ cache.getCacheConfiguration().isCopyOnWrite());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.squid.kraken.v4.caching.Cache#get(java.lang.String)
	 */
	@SuppressWarnings("unchecked")
	public V get(K key) {
		if (log.isDebugEnabled()) {
//			log.debug("get : " + key);
		}
		Element e = cache.get(key);
		if (e != null) {
			return (V) e.getObjectValue();
		} else {
			// key not found
			return null;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.squid.kraken.v4.caching.Cache#get(java.lang.String)
	 */
	@SuppressWarnings("unchecked")
	public Optional<V> getOptional(K key) {
		if (log.isDebugEnabled()) {
			log.debug("get : " + key);
		}
		Element e = cache.get(key);
		if (e != null) {
			return Optional.fromNullable((V) e.getObjectValue());
		} else {
			// key not found
			return null;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.squid.kraken.v4.caching.Cache#contains(java.lang.String)
	 */
	public boolean contains(K key) {
		return cache.isKeyInCache(key);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.squid.kraken.v4.caching.Cache#remove(java.lang.String)
	 */
	public void remove(K key) {
		if (log.isDebugEnabled()) {
			log.debug("remove : " + key);
		}
		cache.remove(key);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.squid.kraken.v4.caching.Cache#removeAll()
	 */
	public void removeAll() {
		if (log.isDebugEnabled()) {
			log.debug("removeAll");
		}
		cache.removeAll();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.squid.kraken.v4.caching.Cache#put(java.lang.String, V)
	 */
	public void put(K key, V value) {
		put(new Element(key, value));
	}

	private void put(Element element) {
		if (log.isDebugEnabled()) {
			log.debug("put : " + element);
		}
		cache.put(element);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.squid.kraken.v4.caching.Cache#getStatistics()
	 */
	public String getStatistics() {
		return cache.getStatistics().toString();
	}

	public CacheConfiguration getConfiguration() {
		CacheConfiguration cf = cache.getCacheConfiguration();
		cf.copyOnRead(true);
		cf.copyOnWrite(true);
		return cf;
	}

}
