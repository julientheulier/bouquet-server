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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

//import org.junit.Test;

public class AWSRedisCacheConfig {
	
	static final Logger logger = LoggerFactory
			.getLogger(AWSRedisCacheConfig.class);

	
	private ServerID redisID = new ServerID();
	
	private ServerID generationalKeysServerID = new ServerID();
	
	private ServerID queriesServerID = new ServerID();
	
	private ArrayList<ServerID> workers;
	
	private boolean debug;

	private String appName="";

	private int querieServerThreadPoolSize = 5;

	private static final int MAX_RECORD_DEFAULT = -1;// by default turn off maxRecord for now - need to provide control from the API first

	private int maxRecord = MAX_RECORD_DEFAULT;
	
	private int ttlInSecond = 24*60*60;

	public AWSRedisCacheConfig(){
		
	}

	public static AWSRedisCacheConfig getDefault(){
		AWSRedisCacheConfig defconf = new AWSRedisCacheConfig();
		ServerID defServ = new ServerID("localhost", -1);

		defconf.setRedisID(new ServerID("localhost", 6379));
		defconf.setGenerationalKeysServerID(defServ);
		defconf.setQueriesServerID(defServ);
		defconf.appName="v4-kraken";
		defconf.querieServerThreadPoolSize = 5;
		defconf.maxRecord = MAX_RECORD_DEFAULT;
		return defconf;
	}
	
	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}


	public boolean getDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	
	public ServerID getGenerationalKeysServerID() {
		return generationalKeysServerID;
	}
	public void setGenerationalKeysServerID(ServerID generationalKeysServerID) {
		this.generationalKeysServerID = generationalKeysServerID;
	}

	public ServerID getQueriesServerID() {
		return queriesServerID;
	}
	public void setQueriesServerID(ServerID queriesServerID) {
		this.queriesServerID = queriesServerID;
	}
	
	public ServerID getRedisID() {
		return redisID;
	}
	public void setRedisID(ServerID redisID) {
		this.redisID = redisID;
	}

	public ArrayList<ServerID> getWorkers() {
		return workers;
	}
	public void setWorkers(ArrayList<ServerID> workers) {
		this.workers = workers;
	}

	public int getQuerieServerThreadPoolSize() {
		return querieServerThreadPoolSize;
	}

	public void setQuerieServerThreadPoolSize(int querieServerThreadPoolSize) {
		this.querieServerThreadPoolSize = querieServerThreadPoolSize;
	}
	
	public int getTtlInSecond() {
		return ttlInSecond;
	}

	public void setTtlInSecond(int ttlInSecond) {
		this.ttlInSecond = ttlInSecond;
	}

	public int getMaxRecord() {
		return maxRecord;
	}

	public void setMaxRecord(int maxRecord) {
		this.maxRecord = maxRecord;
	}

	public static AWSRedisCacheConfig loadFromjson(String filename) throws IOException{
	
        File file = new File(filename);
        if (!file.exists() || !file.canRead()) {        	
        	logger.info("can't read config file " + filename);
        	logger.info("can't read config file " + file.getAbsolutePath());
        	logger.info("exists? " +file.exists());
        	logger.info("canRead? " +file.canRead());        	
        	return null;
        } 
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper.readValue(file, AWSRedisCacheConfig.class);
	}
	
/*	@Test
	public void Test(){
		
		AWSRedisCacheConfig cache= new AWSRedisCacheConfig();
		cache.redisID= new ServerID("localhost", -1);
		cache.generationalKeysServerID = new ServerID("localhost", -1);
		cache.queriesServerID = new ServerID("localhost", -1);
		cache.RSpassword = "abc";
		cache.RSURL="def";
		cache.RSusername ="jhi";
		
		ArrayList<ServerID> w = new ArrayList<ServerID>();
		w.add(new ServerID("localhost", -1)); 
		cache.workers = w;
		
		try {
			ObjectMapper mapper = new ObjectMapper();
		    String json = mapper.enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(cache);
		    System.out.println(json);	
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		try {
			String configFile = "/Users/hoahaduong/Documents/cache.config.json";
			AWSRedisCacheConfig conf = loadFromjson(configFile);
			ObjectMapper mapper = new ObjectMapper();
		    String json = mapper.enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(conf);
		    System.out.println(json);	
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	} */

/*	@org.junit.Test
	public void Test(){
		try {
			String configFile = "/Users/hoahaduong/Documents/conf-tomcatssquibox02/conf/cacheconfig.json";
			AWSRedisCacheConfig conf = loadFromjson(configFile);
			ObjectMapper mapper = new ObjectMapper();
		    String json = mapper.enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(conf);
		    System.out.println(json);	
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} */
		
	
}
