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
package com.squid.kraken.v4.core.analysis.engine.index;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.ESIndexFacade.ESIndexFacadeConfiguration;
import com.squid.kraken.v4.ESIndexFacade.ESIndexFacadeException;
import com.squid.kraken.v4.ESIndexFacade.ESIndexFactory;
import com.squid.kraken.v4.caching.awsredis.RedisCacheManager;
import com.squid.kraken.v4.caching.awsredis.generationalkeysserver.RedisKey;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.model.DomainPK;

/**
 * manage the sub index
 * @author sergefantino
 *
 */
public class DimensionStoreManagerES implements IDimensionStoreManager {

    private static final Logger logger = LoggerFactory.getLogger(DimensionStoreManagerES.class);
    
    public static final String ES_VERSION = "V5"; // this is a constant that identify the ES mapping version; it can be updated to force ES index invalidation when upgrading the mapping.
 
    
    private static final String indexKeyPrefix = "IDX/";
    
    private ConcurrentHashMap<DomainPK, Lock> domains = new ConcurrentHashMap<>();

    protected DimensionStoreManagerES(ESIndexFacadeConfiguration config) {
        ESIndexFactory.init(config);
    }
    
    @Override
    public void stop() {
        ESIndexFactory.stop();
    }

    @Override
    public void invalidate(DomainPK domain) throws InterruptedException {
    	logger.info("Invalidating index for domain " +domain.toString()) ;
        Lock lock = domains.get(domain);
        if (lock!=null) {
            lock.lockInterruptibly();
            try {
                clearDomain(domain);
            } finally {
                lock.unlock();
            }
        } else {
            Lock myself = new ReentrantLock();
            try {
                myself.lockInterruptibly();
                Lock check = domains.putIfAbsent(domain, myself);
                if (check!=null) {
                    try {
                        check.lock();
                        clearDomain(domain);
                    } finally {
                        check.unlock();
                    }
                } else {
                    clearDomain(domain);
                }
            } finally {
                myself.unlock();
            }
        }
    }
    
    /**
     * unsafe to call directly, must be synchronized => use invalidate
     * @param project
     */
    private void clearDomain(DomainPK domain) {
        try {
            ESIndexFactory.getInstance().removeDomain(getIndexNameForDomain(domain));
            refreshIndexNameForDomain(domain);
        } catch (ESIndexFacadeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // remove the lock
        domains.remove(domain);
    }

    @Override
    public IDimensionStore createIndexStore(DimensionIndex index) throws InterruptedException, DimensionStoreException {
        logger.info("create index store ES for " +index.getDimensionName());
    	DomainPK domainPK = index.getDimension().getId().getParent() ;
    	try {
            checkDomain(index);
        } catch (ESIndexFacadeException e) {
            logger.error("Unable to initialize ES Store for " + domainPK + "/" + index.getDimensionName() +" : " , e);
            throw new RuntimeException(e);
        }
        Lock lock = domains.get(domainPK);
        try {
            lock.lock();
            logger.info("creating dimension store ES for " +index.getDimensionName());
            return new DimensionStoreES(ESIndexFactory.getInstance(), getIndexNameForDomain(domainPK ), index);
        } catch (ESIndexFacadeException e) {
            throw new DimensionStoreException("failed creating dimension store for " + domainPK+ "/" + index.getDimensionName(), e);
        } finally {
            lock.unlock();
        }
    }
    
    
    private void refreshIndexNameForDomain(DomainPK domain) {
        if (domain==null ) throw new RuntimeException("invalid project name");
        
        RedisCacheManager.getInstance().refresh(indexKeyPrefix + domain.toUUID() + ":" + ES_VERSION);
    }
    
    

    private String getIndexNameForDomain(DomainPK domain){
    	  if (domain==null){
    		  throw new RuntimeException("Invalid domain name");
    	  }
          RedisKey key = RedisCacheManager.getInstance().getKey(indexKeyPrefix + domain.toUUID() + ":" + ES_VERSION);
          return (key.getStringKey()).toLowerCase();
    }
    
    private void checkDomain(DimensionIndex index) throws ESIndexFacadeException{
    	DomainPK domainPK = index.getDimension().getId().getParent() ;
    	String indexName = getIndexNameForDomain(domainPK);

    	if (!domains.contains(domainPK)) {
            Lock myself = new ReentrantLock();
            try {
                myself.lock();
                Lock check = domains.putIfAbsent(domainPK, myself);
                if (check==null) {
                    // I own the lock
                	boolean existingProject = ESIndexFactory.getInstance().domainInES(indexName);
                    logger.info("does index for domain " + indexName + " exists ? "+ existingProject);

                    if (!existingProject) {                    	
                    	ESIndexFactory.getInstance().addDomain(indexName);
                    }
                } else {
                    // someone is already creating the index
                    check.lock();// wait
                    check.unlock();
                }
            } finally {
                myself.unlock();
            }
        }	
    	
    }


}
