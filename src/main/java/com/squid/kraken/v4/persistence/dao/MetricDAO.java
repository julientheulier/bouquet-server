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
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.MetricPK;
import com.squid.kraken.v4.persistence.AccessRightsPersistentDAO;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DataStore;
import com.squid.kraken.v4.persistence.DataStoreEvent;
import com.squid.kraken.v4.persistence.DataStoreQueryField;

public class MetricDAO extends AccessRightsPersistentDAO<Metric, MetricPK> {

    private final Cache<DomainPK, List<MetricPK>> findByDomainCache;

    public MetricDAO(DataStore ds) {
        super(Metric.class, ds);
        findByDomainCache = CacheFactoryEHCache.getCacheFactory().getCollectionsCache(MetricPK.class, "findByDomain");
    }

    public List<Metric> findByDomain(AppContext app, DomainPK domainId) {
        List<DataStoreQueryField> queryFields = new ArrayList<DataStoreQueryField>(2);
        queryFields.add(new DataStoreQueryField("id.projectId", domainId.getProjectId()));
        queryFields.add(new DataStoreQueryField("id.domainId", domainId.getDomainId()));
        List<Metric> find = super.find(app, domainId, queryFields, findByDomainCache);
        return find;
    }

    @Override
    public void notifyEvent(DataStoreEvent event) {
        MetricPK id = null;
        if (event.getSource() instanceof MetricPK) {
            // deletion
            id = (MetricPK) event.getSource();
            instanceCache.remove(id);
        }
        if (event.getSource() instanceof Metric) {
            // creation or update
            Metric source = (Metric) event.getSource();
            id = source.getId();
            instanceCache.put(id, source);
        }
        if (id != null) {
            // finder cache invalidation
            findByDomainCache.remove(new DomainPK(id.getCustomerId(), id.getProjectId(), id.getDomainId()));
        }
    }

}
