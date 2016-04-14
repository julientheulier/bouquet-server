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
package com.squid.kraken.v4.caching.redis.queryworkerserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.caching.redis.RedisCacheConfig;
import com.squid.kraken.v4.caching.redis.ServerID;


public class QueryWorkerFactory {
	
	public static final QueryWorkerFactory INSTANCE = new QueryWorkerFactory();
	
    static final Logger logger = LoggerFactory.getLogger(QueryWorkerFactory.class);
    
    private static IQueryWorkerServer instServ;
    
    public IQueryWorkerServer getQueryWorkerServer(){
    	logger.info("getting query worker");
    	return instServ;
    }
    public IQueryWorkerServer getNewQueryWorkerServer(RedisCacheConfig conf, ServerID id, boolean onLocalhost){
    	if(instServ == null){
	    	if (conf.getDebug()){
	    		if (onLocalhost || id.port==-1 ){
	    			instServ= new MockQueryWorker(id, conf.getRedisID());
	       		}else{
	    			instServ= new MockQueryWorkerStub(id, conf.getAppName());
	    		}	
	    	}else{
	    		if (onLocalhost ||  id.port==-1 ){
	    			instServ= new QueryWorkerServer(conf);
	    		}else{
	    			instServ= new QueryWorkerServerStub(id, conf.getAppName());
	    		}	    		
	    	}
    	}
    	return instServ;
    }
}
