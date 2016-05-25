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
package com.squid.kraken.v4.ESIndexFacade;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple factory to initialize ES services
 * @author sergefantino
 *
 */
public class ESIndexFactory {

    static final Logger logger = LoggerFactory.getLogger(ESIndexFactory.class);
    
    private static ESIndexFacade INSTANCE = null;
    
    public static ESIndexFacade getInstance() {
        if (INSTANCE==null) {
            throw new IllegalStateException("ES not initialized, must call init() first");
        }
        return INSTANCE;
    }
  
    public static synchronized void init(ESIndexFacadeConfiguration config) {
        if (INSTANCE==null) {
            ESIndexFacade elastic = new ESIndexFacade();
            NodeBuilder node = nodeBuilder();
            try {
            	if (config.isEmbedded()) {
            		logger.warn("Starting embedded ES node");
            		node = node.local(true);
    	
            	} else {
            		logger.info("Starting client ES node");
            		// node may be configured via elasticsearch.yml
            		node = node.client(true);
            		if (config.getClustername() != null) {
            			node = node.clusterName(config.getClustername());
            		}
            	}
            	elastic.start(node.node());
            	try {
            		// wait a bit to make sure node is built
            		Thread.sleep(1000);
            	} catch (InterruptedException e) {
            		e.printStackTrace();
            	}
                INSTANCE=elastic;
            } catch (Exception e) {
                logger.error(e.toString());
                throw new IllegalStateException("failed to initialize ES", e);
            }
        }
    }
    
    public static synchronized void stop() {
        if (INSTANCE!=null) {
            INSTANCE.stop();
            INSTANCE=null;
        }
    }

}
