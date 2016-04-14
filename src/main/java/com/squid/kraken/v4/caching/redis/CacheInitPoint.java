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
package com.squid.kraken.v4.caching.redis;

import java.util.HashSet;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.caching.redis.generationalkeysserver.GenerationalKeysRestService;
import com.squid.kraken.v4.caching.redis.generationalkeysserver.GenerationalKeysServer;
import com.squid.kraken.v4.caching.redis.generationalkeysserver.GenerationalKeysServerFactory;
import com.squid.kraken.v4.caching.redis.generationalkeysserver.IGenerationalKeysServer;
import com.squid.kraken.v4.caching.redis.queriesserver.IQueriesServer;
import com.squid.kraken.v4.caching.redis.queriesserver.QueriesServer;
import com.squid.kraken.v4.caching.redis.queriesserver.QueriesServerFactory;
import com.squid.kraken.v4.caching.redis.queriesserver.QueriesServerRestService;
import com.squid.kraken.v4.caching.redis.queryworkerserver.IQueryWorkerServer;
import com.squid.kraken.v4.caching.redis.queryworkerserver.MockQueryWorker;
import com.squid.kraken.v4.caching.redis.queryworkerserver.QueryWorkerFactory;
import com.squid.kraken.v4.caching.redis.queryworkerserver.QueryWorkerRestService;
import com.squid.kraken.v4.caching.redis.queryworkerserver.QueryWorkerServer;

@Path("/cache")
public class CacheInitPoint {
	
    static final Logger logger = LoggerFactory.getLogger(CacheInitPoint.class);

	public static final CacheInitPoint INSTANCE = new CacheInitPoint();

	public CacheInitPoint(){
	}
	
	private static boolean isQueries  = false;
	private static boolean isGenKey = false;
	private static boolean isWorker = false;
	private static boolean isManager = false;
	
	public void start(RedisCacheConfig conf, HashSet<String> facets) {
		logger.info("CACHE INITALIZATION 15-01-2015");
        try{
	        if (facets.contains("queries")){        		
	        	logger.info(" Facet : Queries");
	        	CacheInitPoint.isQueries = true;
	        	IQueriesServer queries = QueriesServerFactory.INSTANCE.getNewQueriesServer(conf, true); 
	        	queries.start();
	        }
	        if (facets.contains("queryworker")){  
	            logger.info("Facet : Queryworker");
	            CacheInitPoint.isWorker= true; 
	    		IQueryWorkerServer  worker = QueryWorkerFactory.INSTANCE.getNewQueryWorkerServer(conf, new ServerID("localhost", -1), true);
	    		worker.start();
	       	}
	        if (facets.contains("keysserver")){  
	        	logger.info("Facet : keys server");
	        	CacheInitPoint.isGenKey = true;
	    		IGenerationalKeysServer genKey = GenerationalKeysServerFactory.INSTANCE.getNewGenerationalKeysServer(conf,true);
	    		genKey.start();
	     	}
	        if (facets.contains("cachemanager")){  
	        	//debug only
	    		logger.info("Facet :  cache manager");
	    		CacheInitPoint.isManager = true;
				IRedisCacheManager cache= RedisCacheManager.getInstance();
				cache.setConfig(conf);
				cache.startCacheManager();
	        } 
	  
    	} catch (Exception e) {
    		logger.error("failed to init cache distribution layout", e);
    		throw new IllegalStateException(e);
    	}
    } 
		

	@GET
	@Path("/hello")
	public String sayHello(){
		return "Hi cache";
	}

	@Path("/generationalkeys")
	public GenerationalKeysRestService getKeysServer(){
		IGenerationalKeysServer serv= GenerationalKeysServerFactory.INSTANCE.getGenerationalKeysServer();
		if ( CacheInitPoint.isGenKey && (serv!=null) && (serv instanceof GenerationalKeysServer))
			return new  GenerationalKeysRestService((GenerationalKeysServer) serv);
		else 
			return null;
	}
	
	@Path("/queries")
	public QueriesServerRestService getQueriesServer(){
		IQueriesServer serv = QueriesServerFactory.INSTANCE.getQueriesServer();
		if (CacheInitPoint.isQueries && (serv!=null) && (serv instanceof QueriesServer))
			return new QueriesServerRestService((QueriesServer) serv);
		else
			return null;
	}
	
	@Path("/queryworker")
	public QueryWorkerRestService getQueryWorker(){
		logger.info("query worker");
		IQueryWorkerServer serv = QueryWorkerFactory.INSTANCE.getQueryWorkerServer();
		if (CacheInitPoint.isWorker && (serv!=null) && ( serv instanceof QueryWorkerServer || serv instanceof MockQueryWorker ))

			return new	QueryWorkerRestService(serv);
		else{
			if(logger.isDebugEnabled()){logger.debug((serv.getClass().toString()));}
			return  null;
		}
	} 

	@Path("/manager")
	public RedisCacheManagerRestService getCacheManager(){
		if (CacheInitPoint.isManager)
			return new RedisCacheManagerRestService();
		else
			return null;
	}


}
