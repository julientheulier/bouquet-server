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
package com.squid.kraken.v4.runtime;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.ESIndexFacade.ESIndexFacadeConfiguration;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.caching.redis.CacheInitPoint;
import com.squid.kraken.v4.caching.redis.RedisCacheConfig;
import com.squid.kraken.v4.caching.redis.RedisCacheManager;
import com.squid.kraken.v4.core.analysis.engine.index.DimensionStoreManagerFactory;

/**
 * RuntimeService provides static methods to start/stop kraken
 * @author sergefantino
 *
 */
public class RuntimeService {
    
    private static final Logger logger = LoggerFactory
            .getLogger(RuntimeService.class);
    
    public static void startup(String version) {

        // init Velocity Engine
        //VelocityTemplateManager.initEngine(getServletContext().getRealPath(""));


        HashSet<String> facets = new HashSet<String>();

        String facetsStr = System.getProperty("kraken.facet");
        if (facetsStr == null) {
            facetsStr = "front,queries,keysserver,queryworker,cachemanager";
        }
        facets.addAll(Arrays.asList(facetsStr.split(",")));
        
        RedisCacheConfig conf;

        try {
            String configFile = System.getProperty("kraken.cache.config.json");
            if (configFile != null) {
                logger.info(configFile);
                conf = RedisCacheConfig.loadFromjson(System
                        .getProperty("kraken.cache.config.json"));
                logger.info(conf.getAppName());
            } else {
                conf = RedisCacheConfig.getDefault();
            }
        } catch (IOException e) {
            e.printStackTrace();
            conf = RedisCacheConfig.getDefault();
        }

        if (facets.contains("front")) {
            // init the API
            ServiceUtils.getInstance().initAPI(version, 10, 3600*24);
            // initialize RedisCacheManager
            RedisCacheManager.getInstance().setConfig(conf);
            RedisCacheManager.getInstance().startCacheManager();
        }
        CacheInitPoint cache = CacheInitPoint.INSTANCE;
        cache.start(conf, facets);

        // DimensionStoreManagerFactory initialization
        try {
			String embeddedValue = KrakenConfig.getProperty("elastic.local", "true");
			boolean embedded = embeddedValue.equals("true");
			ESIndexFacadeConfiguration esConfig = new ESIndexFacadeConfiguration(embedded, null);
            DimensionStoreManagerFactory.init(esConfig);
        } catch (Exception e) {
            logger.error("Failed to initialized DImensionStore with error: "+e.toString());
            throw new IllegalStateException("Failed to initialized DImensionStore", e);
        }
    }

}
