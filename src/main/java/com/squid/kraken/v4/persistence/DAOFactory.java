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
package com.squid.kraken.v4.persistence;

import java.util.HashMap;
import java.util.Map;

import com.squid.kraken.v4.model.AccessToken;
import com.squid.kraken.v4.model.Annotation;
import com.squid.kraken.v4.model.Attribute;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.Client;
import com.squid.kraken.v4.model.Customer;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectAnalysisJob;
import com.squid.kraken.v4.model.ProjectFacetJob;
import com.squid.kraken.v4.model.ProjectUser;
import com.squid.kraken.v4.model.Relation;
import com.squid.kraken.v4.model.Shortcut;
import com.squid.kraken.v4.model.State;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserGroup;
import com.squid.kraken.v4.persistence.dao.AccessTokenDAO;
import com.squid.kraken.v4.persistence.dao.AnnotationDAO;
import com.squid.kraken.v4.persistence.dao.AttributeDAO;
import com.squid.kraken.v4.persistence.dao.BookmarkDAO;
import com.squid.kraken.v4.persistence.dao.ClientDAO;
import com.squid.kraken.v4.persistence.dao.CustomerDAO;
import com.squid.kraken.v4.persistence.dao.DimensionDAO;
import com.squid.kraken.v4.persistence.dao.DomainDAO;
import com.squid.kraken.v4.persistence.dao.MetricDAO;
import com.squid.kraken.v4.persistence.dao.ProjectAnalysisJobDAO;
import com.squid.kraken.v4.persistence.dao.ProjectDAO;
import com.squid.kraken.v4.persistence.dao.ProjectFacetJobDAO;
import com.squid.kraken.v4.persistence.dao.ProjectUserDAO;
import com.squid.kraken.v4.persistence.dao.RelationDAO;
import com.squid.kraken.v4.persistence.dao.ShortcutDAO;
import com.squid.kraken.v4.persistence.dao.StateDAO;
import com.squid.kraken.v4.persistence.dao.UserDAO;
import com.squid.kraken.v4.persistence.dao.UserGroupDAO;

/**
 * Dynamic DAO Factory.
 */
public class DAOFactory {

    private static DAOFactory mongoDBDAOFactory;

    /**
     * Default singleton getter
     * 
     * @return a MongoDB factory.
     */
    public static DAOFactory getDAOFactory() {
        if (mongoDBDAOFactory == null) {
            DataStore ds = new MongoDBDataStore();
            mongoDBDAOFactory = new DAOFactory(ds);
        }
        return mongoDBDAOFactory;
    }

    private DataStore defaultDataStore;
    private DataStore baseDataStore;
    private DataStore rootDataStore;

    private static Map<Class<?>, GenericDAO<?, ? extends GenericPK>> daoCache;

    private DAOFactory(DataStore baseDataStore) {
        if (daoCache == null) {
            daoCache = new HashMap<Class<?>, GenericDAO<?, ? extends GenericPK>>();
        }
        this.baseDataStore = new GenericDataStoreDecorator(baseDataStore);
        this.rootDataStore = new LocalizationDataStoreDecorator(
                this.baseDataStore);
        // decorated DS
        this.defaultDataStore = new CustomerDataStoreDecorator(rootDataStore);
    }

    public <T extends Persistent<PK>, PK extends GenericPK> GenericDAO<T, PK> getDAO(Class<T> type) {
        GenericDAO<?, ? extends GenericPK> dao = daoCache.get(type);
        if (dao == null) {
            String typeName = type.getName();
            if (typeName.equals(Customer.class.getName())) {
                dao = new CustomerDAO(rootDataStore);
            } else if (typeName.equals(Domain.class.getName())) {
                dao = new DomainDAO(defaultDataStore);
            } else if (typeName.equals(Project.class.getName())) {
                dao = new ProjectDAO(defaultDataStore);
            } else if (typeName.equals(Dimension.class.getName())) {
                dao = new DimensionDAO(defaultDataStore);
            } else if (typeName.equals(Metric.class.getName())) {
                dao = new MetricDAO(defaultDataStore);
            } else if (typeName.equals(Attribute.class.getName())) {
                dao = new AttributeDAO(defaultDataStore);
            } else if (typeName.equals(ProjectAnalysisJob.class.getName())) {
                dao = new ProjectAnalysisJobDAO(defaultDataStore);
            } else if (typeName.equals(ProjectFacetJob.class.getName())) {
                dao = new ProjectFacetJobDAO(defaultDataStore);
            } else if (typeName.equals(User.class.getName())) {
                dao = new UserDAO(defaultDataStore);
            } else if (typeName.equals(AccessToken.class.getName())) {
                dao = new AccessTokenDAO(baseDataStore);
            } else if (typeName.equals(UserGroup.class.getName())) {
                dao = new UserGroupDAO(defaultDataStore);
            } else if (typeName.equals(Client.class.getName())) {
                dao = new ClientDAO(defaultDataStore);
            } else if (typeName.equals(Annotation.class.getName())) {
                dao = new AnnotationDAO(defaultDataStore);
            } else if (typeName.equals(ProjectUser.class.getName())) {
                dao = new ProjectUserDAO(defaultDataStore);
            } else if (typeName.equals(Relation.class.getName())) {
                dao = new RelationDAO(defaultDataStore);
            } else if (typeName.equals(State.class.getName())) {
                dao = new StateDAO(defaultDataStore);
            } else if (typeName.equals(Shortcut.class.getName())) {
                dao = new ShortcutDAO(defaultDataStore);
            } else if (typeName.equals(Bookmark.class.getName())) {
                dao = new BookmarkDAO(defaultDataStore);
            }
            daoCache.put(type, dao);
        }
        // yes we can cast
        @SuppressWarnings("unchecked")
        GenericDAO<T, PK> genericDAO = (GenericDAO<T, PK>) dao;
        return genericDAO;
    }

	public DataStore getBaseDataStore() {
		return baseDataStore;
	}  
    

}
