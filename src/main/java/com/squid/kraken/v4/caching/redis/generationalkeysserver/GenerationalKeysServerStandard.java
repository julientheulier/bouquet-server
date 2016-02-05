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
package com.squid.kraken.v4.caching.redis.generationalkeysserver;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.squid.kraken.v4.caching.redis.IRedisCacheProxy;
import com.squid.kraken.v4.caching.redis.ServerID;

/**
 * abstract version to be used by both actual and mockup
 * @author sergefantino
 *
 */
public abstract class GenerationalKeysServerStandard implements IGenerationalKeysServer {

    static final Logger logger = LoggerFactory
            .getLogger(GenerationalKeysServerStandard.class);

	public static final String REDISKEY_VERSION = "";// use it as a kill switch to clear any cache when releasing a new version
    public static final String REDISKEY_PREFIX = "rediskey" + REDISKEY_VERSION + "-";

    private IRedisCacheProxy redis;

    private ConcurrentHashMap<String, RedisKey> keys;

    public GenerationalKeysServerStandard(ServerID redisID) {
        this.keys = new ConcurrentHashMap<String, RedisKey>();
        this.redis = initRedisService(redisID);
    }
    
    protected abstract IRedisCacheProxy initRedisService(ServerID redisID);

    public void start() {
        logger.info("starting  Generational Keys Server V2");
    }

    public String hello() {
        return "Hello Generational Keys Server V2";
    }

    public RedisKey getKey(String id, Collection<String> dependencies) {
        try {
            RedisKey k = this.getExistingKey(id);
            if (k == null) {
            	k = this.createKey(id, dependencies);
                return k;
            }
            if (isDependenciesChanged(k,dependencies)) {
            	// create a new key
            	k = this.createKey(id, dependencies);
                return k;
            }
            if (!this.isFresh(k)) {
                k = this.updateKey(k);
            }
            return k;
        } catch (GenKeyException e) {
            throw new RuntimeException(e);
        }
    }
    
    private boolean isDependenciesChanged(RedisKey k, Collection<String> dependencies) {
    	if (k.getDepGen()==null || k.getDepGen().isEmpty()) {
    		return !(dependencies==null || dependencies.isEmpty());
    	}
    	if (dependencies==null || k.getDepGen().size()!=dependencies.size()) {
    		return true;
    	}
    	if (dependencies instanceof Set<?>) {
    		Set<String> mykeys = k.getDepGen().keySet();
        	if (mykeys.equals(dependencies)) {
        		return false;
        	} else {
        		return true;
        	}
    	} else {
        	if (k.getDepGen().keySet().equals(new HashSet<String>(dependencies))) {
        		return false;
        	} else {
        		return true;
        	}
    	}
    }
    
    @Override
    public boolean refresh(Collection<String> dependencies) {
        for (String name : dependencies) {
        	RedisKey k = this.getExistingKey(name);
        	if (k ==null){
        		try {
					k = this.createKey(name, null);
				} catch (GenKeyException e) {
					logger.error("failed to refresh key for " + name +" with error:" + e.toString());
					continue;
				}
            } else {
                k.increaseVersion();;
            }
            this.saveKeyToRedis(k);
            //logger.info("refreshed key for " + name + ": " + k.getStringKey());
        }
        return true;
    }
    
    private RedisKey getExistingKey(String name){
    	RedisKey k = this.keys.get(name);
    	if (k!=null) {// got a key
    		if (!k.getName().equals(name)) {
    			// alert, it's not valid
    			logger.error("inconsistent genkey found for key=" + name);
    			return null;
    		} else {
    			// the normal way
    			return k;
    		}
    	} else if (redis.inCache(REDISKEY_PREFIX + name)) { // k==null
    		// the key is in Redis
    		try {
    			// got it
				return this.retrieveKeyFromRedis(name);
			} catch (GenKeyException e) {
				// some error
				logger.error("cannot retrieve genkey found for key=" + name, e);
				return null;
			}
    	} else {
    		// unknown key
    		return null;
    	}
    }
    
    private HashMap<String, Integer> getGenDeps(Collection<String> names) throws GenKeyException {
    	if (names == null)
    		return null;
        
    	HashMap<String, Integer> res = new HashMap<String, Integer>();
        
    	if (names!=null) {
	        HashSet<String> doneDeps = new HashSet<String>(names);
	        for (String name: names) {
	        	doneDeps.add(name);
	        	RedisKey dep = this.getExistingKey(name);
	        	if (dep == null)
	        	{
	                dep = this.createKey(name, null) ;
	            }
	        	res.put(name, new Integer(dep.getVersion())); 
	        }
    	}
        
        return res;
    }

    private void saveKeyToRedis(RedisKey toSave) {
        String key = REDISKEY_PREFIX + toSave.getName();
        try {
            this.redis.put(key, toSave.toJson());
        } catch (JsonProcessingException e) {
            logger.error("failed to save key to redis: " + e.getMessage());
            throw new RuntimeException(e);
        }

    }
    
    //private ConcurrentHashMap<String, RedisKey> reverseCheck = new ConcurrentHashMap<>();

    private RedisKey retrieveKeyFromRedis(String name) throws GenKeyException {
        String key = REDISKEY_PREFIX + name;
        byte[] b = this.redis.get(key);
        if (b != null) {
        	RedisKey k = RedisKey.fromJson(new String(b)) ;
        	this.keys.put(name, k);
        	/*
        	RedisKey check = reverseCheck.get(k.getStringKey());
        	if (check!=null && !check.getName().equals(name)) {
        	    logger.error("inconsistent genKey: we have a collision with " + check.getName());
        	} else {
        	    reverseCheck.put(k.getStringKey(), k);
        	    logger.info("create genKey=" + k.getStringKey());
        	}
        	*/
            return k;
        } else {
            throw new GenKeyException("cannot find redisKey '" + name +"'");
        }
    }

    public boolean isFresh(RedisKey k) throws GenKeyException {
    	//System.out.println("to check " +k.toString());
    	RedisKey local = this.getExistingKey(k.getName());
    	if (local == null){// SFA: can we ever go there?
    		this.keys.put(k.getName(), k);
    		return true;
    	}else{
    		//System.out.println("local copy " + local.toString());
    		if (k.getVersion() != local.getVersion()){
    			return false;
    		}else{
    			if (k.getDepGen() == null){
    				return true;
    			}else{
    				for (String name : k.getDepGen().keySet()) {
    					RedisKey dep = this.getExistingKey(name);
    					if (dep == null){
    						//throw new GenKeyException("dependency " + name + " cannot be found");
    					    return false;
    					}else{
    						//System.out.println(dep.toString());
    						if( ! k.getDepGen().get(name).equals(dep.getVersion())) {
    							return false;
    						}		
    					}
    				}
    				return true;
    			}
    		}
    	}
    }

    private RedisKey updateKey(RedisKey old) throws GenKeyException {
    	HashMap<String, Integer> depVersions = this.getGenDeps(old.getDepGen().keySet());
  
        RedisKey newKey = new RedisKey(old.getName(), old.getUniqueID(), old.getVersion() + 1, depVersions);
        this.keys.put(old.getName(), newKey);
        this.saveKeyToRedis(newKey);
        return newKey;
    }

    private RedisKey createKey(String id, Collection<String> dependencies) throws GenKeyException {
        UUID uniqueID = UUID.randomUUID();
        HashMap<String, Integer> depVersions = this.getGenDeps(dependencies);
        RedisKey newKey = new RedisKey(id, uniqueID, 0, depVersions);
        this.keys.put(id, newKey);
        this.saveKeyToRedis(newKey);
        return newKey;
    }

}
