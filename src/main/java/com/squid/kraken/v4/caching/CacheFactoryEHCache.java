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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.model.GenericPK;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.Configuration.Monitoring;
import net.sf.ehcache.config.DiskStoreConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.config.SizeOfPolicyConfiguration;
import net.sf.ehcache.management.ManagementService;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

public class CacheFactoryEHCache extends CacheFactory {

	private static final Logger log = LoggerFactory
			.getLogger(CacheFactoryEHCache.class);

//	public final String CACHE_CONF_FILENAME;

	private final CacheManager cacheManager;

	private final Map<Class<?>, Cache<?, ?>> caches;

	private final Map<String, Cache<?, ?>> collectionsCaches;

	public CacheFactoryEHCache() {
		super();
		String confFile = System.getProperty("kraken.ehcache.config");
		if (confFile == null){
			log.info("No configuration file for EHCache. Using defaut configuration") ;
			cacheManager = CacheManager.create(this.createDefaultConfiguration());
		}else{
			String customConfPath = System.getProperty("user.home")
					+ File.separatorChar + confFile;

			InputStream cacheConfig;
			File test = new File(customConfPath);
			if (test.exists()){
				try {
					cacheConfig = new FileInputStream(customConfPath);					
					cacheManager = CacheManager.create(cacheConfig);
				} catch (FileNotFoundException e) {
					throw new RuntimeException(e);
				}
			
			}else{
				log.info("File " + customConfPath + " does not exists. Using defaut EHCache configuration") ;
				cacheManager = CacheManager.create(this.createDefaultConfiguration());
			}
		}
		
		try {
			MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
	        ManagementService.registerMBeans(cacheManager, mBeanServer, false, false, false, true);
			log.info("CacheManager DiskStorePath : "
					+ cacheManager.getConfiguration()
							.getDiskStoreConfiguration().getPath());
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 

		// init the caches
		caches = new HashMap<Class<?>, Cache<?, ?>>();
		collectionsCaches = new HashMap<String, Cache<?, ?>>();
        
	}

	@Override
	public void shutdown() {
		cacheManager.shutdown();
	}
	
	@Override
	public void clearAll() {
		for (Cache<?, ?> c : caches.values()) {
			c.removeAll();
		}
		for (Cache<?, ?> c : collectionsCaches.values()) {
			c.removeAll();
		}
	}

	public <K extends GenericPK, V> Cache<K, V> getCache(
			Class<V> valueType) {
		Cache<?, ?> cache;
		synchronized (caches) {
			cache = caches.get(valueType);
			if (cache == null) {
				cache = new EHCacheImpl<K, V>(cacheManager,
						valueType.getName(), true);
				caches.put(valueType, cache);
			}
		}
		// cache can be cast safely
		@SuppressWarnings("unchecked")
		Cache<K, V> cacheToReturn = (Cache<K, V>) cache;
		return cacheToReturn;
	}

	public <K extends GenericPK, V> Cache<K, List<V>> getCollectionsCache(
			Class<V> valueType, String collectionName) {
		String cacheKey = valueType.getName() + "_" + collectionName;
		Cache<?, ?> cache;
		synchronized (collectionsCaches) {
			cache = collectionsCaches.get(cacheKey);
			if (cache == null) {
				cache = new EHCacheImpl<K, V>(cacheManager, cacheKey, true);
				collectionsCaches.put(cacheKey, cache);
			}
		}
		// cache can be cast safely
		@SuppressWarnings("unchecked")
		Cache<K, List<V>> cacheToReturn = (Cache<K, List<V>>) cache;
		return cacheToReturn;
	}

	private Configuration createDefaultConfiguration(){
		Configuration defConf = new Configuration();
		defConf.updateCheck(true)
		.monitoring(Monitoring.AUTODETECT)
		.dynamicConfig(true);
		
		SizeOfPolicyConfiguration sizeofConf = new SizeOfPolicyConfiguration();
		defConf.sizeOfPolicy(sizeofConf.maxDepth(100).maxDepthExceededBehavior("abort"));
		
		DiskStoreConfiguration diskConf = new DiskStoreConfiguration() ;
		defConf.diskStore(diskConf.path("user.home/caches/"));
		
//		CacheConfiguration cacheConf = defConf.getDefaultCacheConfiguration().clone();
		CacheConfiguration cacheConf = new CacheConfiguration();
		cacheConf.maxEntriesLocalHeap(10000)
			.eternal(false)
			.diskSpoolBufferSizeMB(30)
			.maxElementsOnDisk(10000)
			.diskExpiryThreadIntervalSeconds(120)
			.memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU)
			.copyOnRead(true)
			.statistics(true)
			.copyOnWrite(true);
		
		PersistenceConfiguration persistConf= new PersistenceConfiguration();
		cacheConf.persistence(persistConf.strategy(PersistenceConfiguration.Strategy.LOCALTEMPSWAP)); 
		defConf.defaultCache(cacheConf);

		
		return defConf; 
	}
	
}
