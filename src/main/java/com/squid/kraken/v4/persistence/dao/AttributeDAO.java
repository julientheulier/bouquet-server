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
import com.squid.kraken.v4.model.Attribute;
import com.squid.kraken.v4.model.AttributePK;
import com.squid.kraken.v4.model.DimensionPK;
import com.squid.kraken.v4.persistence.AccessRightsPersistentDAO;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DataStore;
import com.squid.kraken.v4.persistence.DataStoreEvent;
import com.squid.kraken.v4.persistence.DataStoreQueryField;

public class AttributeDAO extends AccessRightsPersistentDAO<Attribute, AttributePK> {

    private final Cache<DimensionPK, List<AttributePK>> findByDimensionCache;

    public AttributeDAO(DataStore ds) {
        super(Attribute.class, ds);
        findByDimensionCache = CacheFactoryEHCache.getCacheFactory().getCollectionsCache(AttributePK.class,
                "findByDimension");
    }

    public List<Attribute> findByDimension(AppContext app, DimensionPK dimensionId) {
        List<DataStoreQueryField> queryFields = new ArrayList<DataStoreQueryField>(3);
        queryFields.add(new DataStoreQueryField("id.projectId", dimensionId.getProjectId()));
        queryFields.add(new DataStoreQueryField("id.domainId", dimensionId.getDomainId()));
        queryFields.add(new DataStoreQueryField("id.dimensionId", dimensionId.getDimensionId()));
        List<Attribute> find = super.find(app, dimensionId, queryFields, findByDimensionCache);
        return find;
    }

    @Override
    public void notifyEvent(DataStoreEvent event) {
        AttributePK id = null;
        if (event.getSource() instanceof AttributePK) {
            // deletion
            id = (AttributePK) event.getSource();
            instanceCache.remove(id);
        }
        if (event.getSource() instanceof Attribute) {
            // creation or update
            Attribute source = (Attribute) event.getSource();
            id = source.getId();
            instanceCache.put(id, source);
        }
        if (id != null) {
            // finder cache invalidation
            findByDimensionCache.remove(new DimensionPK(id.getCustomerId(), id.getProjectId(), id.getDomainId(), id
                    .getDimensionId()));
        }
    }

}
