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

import java.util.Enumeration;
import java.util.Properties;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.concurrent.ExecutionManager;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.core.analysis.engine.index.DimensionStoreManagerFactory;
import com.squid.kraken.v4.core.database.impl.DatabaseServiceImpl;
import com.squid.kraken.v4.persistence.MongoDBHelper;

import net.sf.ehcache.CacheManager;

public class ServletContextListenerImpl implements ServletContextListener {
    
    private static final Logger logger = LoggerFactory.getLogger(ServletContextListenerImpl.class);

    @Override
    public void contextDestroyed(ServletContextEvent arg0) {
        logger.info("Stopping Indexing...");
		DimensionStoreManagerFactory.stop();
        logger.info("Stopping Execution...");
		ExecutionManager.INSTANCE.shutdownJobsExecutor();
        logger.info("Stopping Kraken server...");
        ServiceUtils.getInstance().shutdownAPI();
        logger.info("Stopping MongoDB pool...");
        MongoDBHelper.getDatastore().getMongo().close();
        logger.info("Stopping DatabaseService...");
		DatabaseServiceImpl.INSTANCE.shutdownJobsExecutor();
        logger.info("Stopping EHCache...");
        CacheManager.getInstance().shutdown();
        logger.info("Kraken server stopped.");
    }

    @Override
    public void contextInitialized(ServletContextEvent arg0) {
        // debug properties
        Properties p = System.getProperties();
        Enumeration<Object> keys = p.keys();
        while (keys.hasMoreElements()) {
          String key = (String)keys.nextElement();
          String value = (String)p.get(key);
          logger.info("System.property "+key + ": " + value);
        }
    }

}
