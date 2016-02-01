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
package com.squid.kraken.v4.persistence.dao;

import java.util.ArrayList;
import java.util.List;

import com.squid.kraken.v4.caching.Cache;
import com.squid.kraken.v4.caching.CacheFactoryEHCache;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.DimensionPK;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.persistence.AccessRightsPersistentDAO;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DataStore;
import com.squid.kraken.v4.persistence.DataStoreEvent;
import com.squid.kraken.v4.persistence.DataStoreQueryField;

public class DimensionDAO extends AccessRightsPersistentDAO<Dimension, DimensionPK> {

    private final Cache<DomainPK, List<DimensionPK>> findByDomainCache;

    private final Cache<DimensionPK, List<DimensionPK>> findByParentCache;
    
    // parentIndex holds parent relations to invalidate findByParentCache properly
    private final Cache<DimensionPK, DimensionPK> parentIndex;

    public DimensionDAO(DataStore ds) {
        super(Dimension.class, ds);
        findByDomainCache = CacheFactoryEHCache.getCacheFactory()
                .getCollectionsCache(DimensionPK.class, "findByDomain");
        findByParentCache = CacheFactoryEHCache.getCacheFactory()
                .getCollectionsCache(DimensionPK.class, "findByParent");
        parentIndex = CacheFactoryEHCache.getCacheFactory().getCache(DimensionPK.class);
    }

    public List<Dimension> findByDomain(AppContext app, DomainPK domainId) {
        List<DataStoreQueryField> queryFields = new ArrayList<DataStoreQueryField>(2);
        queryFields.add(new DataStoreQueryField("id.projectId", domainId.getProjectId()));
        queryFields.add(new DataStoreQueryField("id.domainId", domainId.getDomainId()));
        List<Dimension> find = super.find(app, domainId, queryFields, findByDomainCache);
        return find;
    }

    public List<Dimension> findByParent(AppContext app, Dimension dimension) {
        List<DataStoreQueryField> queryFields = new ArrayList<DataStoreQueryField>(3);
        queryFields.add(new DataStoreQueryField("id.projectId", dimension.getId().getProjectId()));
        queryFields.add(new DataStoreQueryField("id.domainId", dimension.getId().getDomainId()));
        queryFields.add(new DataStoreQueryField("parentId", dimension.getId()));
        List<Dimension> find = super.find(app, dimension.getId(), queryFields, findByParentCache);
        return find;
    }

    @Override
    public void notifyEvent(DataStoreEvent event) {
        DimensionPK sourceId = null;
        if (event.getSource().getClass().equals(DimensionPK.class)) {
            // deletion
            sourceId = (DimensionPK) event.getSource();
            instanceCache.remove(sourceId);
            parentIndex.remove(sourceId);
        }
        if (event.getSource() instanceof Dimension) {
            // creation or update
            Dimension source = (Dimension) event.getSource();
            sourceId = source.getId();
            instanceCache.put(sourceId, source);
            // deal with parent
            DimensionPK oldParentId = parentIndex.get(sourceId);
            DimensionPK newParentId = source.getParentId();
            if (newParentId != null) {
            	if (oldParentId != null) {
            		if (!oldParentId.equals(newParentId)) {
            			// parent has changed
            			findByParentCache.remove(oldParentId);
            			parentIndex.put(sourceId, newParentId);
            		}
            	} else {
            		// new parent
            		parentIndex.put(sourceId, newParentId);
            	}
            } else {
            	if (oldParentId != null) {
            		// parent has been removed
        			findByParentCache.remove(oldParentId);
        			parentIndex.remove(sourceId);
            	}
            }
        }
        if (sourceId != null) {
            // finder cache invalidation
            DomainPK domainId = new DomainPK(sourceId.getCustomerId(), sourceId.getProjectId(), sourceId.getDomainId());
            findByDomainCache.remove(domainId);
        }
    }
    
    @Override
    public Dimension create(AppContext ctx, Dimension dimension) {
        forceCustomerId(ctx.getCustomerId(), dimension);
        return super.create(ctx, dimension);
    }
    
    @Override
    public void update(AppContext ctx, Dimension dimension) {
        forceCustomerId(ctx.getCustomerId(), dimension);
        super.update(ctx, dimension);
    }

    /**
     * Force the referenced parent PK to have the right CustomerId.
     * @param customerId
     * @param dimension
     */
    private void forceCustomerId(String customerId, Dimension dimension) {
    	DimensionPK id = dimension.getParentId();
        if (id != null) {
        	id.setCustomerId(customerId);
        }
    }


}
