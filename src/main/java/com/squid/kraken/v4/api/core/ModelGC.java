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
package com.squid.kraken.v4.api.core;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.ExpirableDAO;

/**
 * Garbage Collector task.<br>
 * Delete all expired model object.
 */
public class ModelGC<T extends Persistent<PK>, PK extends GenericPK> implements Runnable {

    static private final Logger logger = LoggerFactory.getLogger(ModelGC.class);

    private final int temporaryMaxAgeInSeconds;
    
    private GenericService<T, PK> service;
    
    private Class<T> type;

    public ModelGC(int temporaryMaxAgeInSeconds, GenericService<T, PK> service, Class<T> type) {
        super();
        this.temporaryMaxAgeInSeconds = temporaryMaxAgeInSeconds;
        this.service = service;
        this.type = type;
    }

    @Override
    public void run() {
        logger.info("Running JobGC for type : "+type.getSimpleName());
        try {
            DAOFactory factory = DAOFactory.getDAOFactory();
            ExpirableDAO<T> dao = ((ExpirableDAO<T>) factory.getDAO(type));
            long maxCreationDate = System.currentTimeMillis() - (temporaryMaxAgeInSeconds * 1000);
            List<T> findAllExpired = dao.findAllExpired(maxCreationDate);
            logger.info("Deleting " + findAllExpired.size() + " expired "+type.getName()+" instances");
            for (T job : findAllExpired) {
                try {
                    AppContext ctx = ServiceUtils.getInstance().getRootUserContext(job.getCustomerId());
                    service.delete(ctx, job.getId());
                } catch (Exception e) {
                    logger.warn("Error deleting objectId : " +job.getId());
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}