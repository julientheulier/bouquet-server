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

import java.util.List;

import com.google.common.base.Optional;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.HasLocalizedName;
import com.squid.kraken.v4.model.LString;
import com.squid.kraken.v4.model.LStringPK;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.persistence.dao.LStringDAO;

/**
 * A DataStore decorator which handles Localization of {@link HasLocalizedName} instances.
 */
public class LocalizationDataStoreDecorator implements DataStore {

    private final DataStore ds;

    /**
     * Constructor
     * 
     * @param ds
     *            the DataStore to decorate.
     */
    public LocalizationDataStoreDecorator(DataStore ds) {
        super();
        this.ds = ds;
    }

    public <T extends Persistent<PK>, PK extends GenericPK> T create(AppContext ctx, T newInstance) {
        if (newInstance instanceof HasLocalizedName) {
            // localization
            HasLocalizedName lobject = (HasLocalizedName) newInstance;
            if (ServiceUtils.getInstance().isDefaultLocale(ctx) && (lobject.getLName() != null)) {
                // set the internal name to default locale value
                lobject.setName(lobject.getLName());
            }
        }

        // perform the create
        T created = ds.create(ctx, newInstance);

        if (newInstance instanceof HasLocalizedName) {
            HasLocalizedName lobject = (HasLocalizedName) newInstance;
            // localization
            if (!ServiceUtils.getInstance().isDefaultLocale(ctx)) {
                // persist the localized name if not default locale
                LStringPK pk = new LStringPK(ctx.getCustomerId(), created.getObjectType(), "name", created.getId()
                        .toUUID(), ctx.getLocale());
                LStringDAO.getInstance().create(ctx, new LString(pk, lobject.getLName()));
            }
        }

        return created;
    }

    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> void delete(AppContext ctx, Class<T> type, PK id) {
        // TODO delete any LString
        ds.delete(ctx, type, id);
    }

    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> boolean exists(AppContext ctx, Class<T> type, PK id) {
        return ds.exists(ctx, type, id);
    }

    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> Optional<T> read(AppContext ctx, Class<T> type, PK id) {
        Optional<T> read = ds.read(ctx, type, id);
        if (read.isPresent()) {
            T object = read.get();
            localize(ctx, object);
        }
        return read;
    }
    
    private <T extends Persistent<PK>, PK extends GenericPK> void localize(AppContext ctx, T object) {
        if (object instanceof HasLocalizedName) {
            // localization
            HasLocalizedName lobject = (HasLocalizedName) object;
            String localizedName;
            if (ServiceUtils.getInstance().isDefaultLocale(ctx)) {
                localizedName = lobject.getName();
            } else {
                // get the localized name
                LStringPK pk = new LStringPK(ctx.getCustomerId(), object.getObjectType(), "name", object.getId()
                        .toUUID(), ctx.getLocale());
                Optional<LString> ls = LStringDAO.getInstance().read(ctx, pk);
                if (ls.isPresent()) {
                    localizedName = ls.get().getValue();
                } else {
                    // fallback to default
                    if (lobject.getName() != null) {
                        localizedName = lobject.getName();
                    } else {
                        localizedName = lobject.getLName();
                    }
                }
            }
            lobject.setLName(localizedName);
        }
    }

    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> T readNotNull(AppContext ctx, Class<T> type, PK id) {
        T object = ds.readNotNull(ctx, type, id);
        localize(ctx, object);
        return object;
    }

    
    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> void update(AppContext ctx, T transientObject) {
        if (transientObject instanceof HasLocalizedName) {
            // localization
            HasLocalizedName lobject = (HasLocalizedName) transientObject;
            if (ServiceUtils.getInstance().isDefaultLocale(ctx) && (lobject.getLName() != null)) {
                // set the internal name to locale value
                lobject.setName(lobject.getLName());
            }
            if ((!ServiceUtils.getInstance().isDefaultLocale(ctx)) && (lobject.getName() == null)) {
                // force previous name as Rest API ignores it
                @SuppressWarnings("unchecked")
                GenericDAO dao = DAOFactory.getDAOFactory().getDAO(transientObject.getClass());
                @SuppressWarnings("unchecked")
                HasLocalizedName object = (HasLocalizedName) dao.readNotNull(ctx, transientObject.getId());
                lobject.setName(object.getName());
            }
        }

        // perform the update
        ds.update(ctx, transientObject);

        if (transientObject instanceof HasLocalizedName) {
            // localization
            HasLocalizedName lobject = (HasLocalizedName) transientObject;
            if (!ServiceUtils.getInstance().isDefaultLocale(ctx) && (lobject.getLName() != null)) {
                // persist the localized name
                LStringPK pk = new LStringPK(ctx.getCustomerId(), transientObject.getObjectType(), "name",
                        transientObject.getId().toUUID(), ctx.getLocale());
                LStringDAO.getInstance().create(ctx, new LString(pk, lobject.getLName()));
            }
        }
    }

    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> List<T> find(AppContext app, Class<T> type,
            List<DataStoreQueryField> queryFields, List<DataStoreFilterOperator> filterOperators) {
        return find(app, type, queryFields, filterOperators, null);
    }

    @Override
    public <T extends Persistent<PK>, PK extends GenericPK> List<T> find(AppContext app, Class<T> type,
            List<DataStoreQueryField> queryFields, List<DataStoreFilterOperator> filterOperators, String orderBy) {
        List<T> list = ds.find(app, type, queryFields, filterOperators, orderBy);
        for (T object : list) {
            localize(app, object);
        }
        return list;
    }
}
