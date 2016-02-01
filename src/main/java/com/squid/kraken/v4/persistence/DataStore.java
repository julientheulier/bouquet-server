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
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;

/**
 * DataStore is an abstraction of an underlying storage.<br>
 * It is used by DAOs.
 */
public interface DataStore {

    /**
     * Persist the newInstance object into database
     */
    public <T extends Persistent<PK>, PK extends GenericPK> T create(AppContext ctx, T object);

    /**
     * Retrieve an object that was previously persisted to the database using the indicated id as primary key
     * 
     * @return the object wrapped as Optional to handle not found (null) case.
     */
    public <T extends Persistent<PK>, PK extends GenericPK> Optional<T> read(AppContext ctx, Class<T> type, PK id);

    /**
     * Check if an object exists.
     */
    public <T extends Persistent<PK>, PK extends GenericPK> boolean exists(AppContext ctx, Class<T> type, PK id);

    /**
     * Retrieve an object that was previously persisted to the database using the indicated id as primary key
     * 
     * @return the object found or throws a {@link ObjectNotFoundAPIException}
     */
    public <T extends Persistent<PK>, PK extends GenericPK> T readNotNull(AppContext ctx, Class<T> type, PK id);

    /** Save changes made to a persistent object. */
    public <T extends Persistent<PK>, PK extends GenericPK> void update(AppContext ctx, T transientObject);

    /** Remove an object from persistent storage in the database */
    public <T extends Persistent<PK>, PK extends GenericPK> void delete(AppContext ctx, Class<T> type, PK id);
    
    /**
     * Generic finder
     * 
     * @param <T>
     *            type of returned objects
     * @param <PK>
     *            type of returned objects' PK
     * @param app
     *            context
     * @param type
     *            type of returned objects
     * @param queryFields
     *            a List of key/values to express queries.
     * @param filterOperators
     * 			  a List of filter operators ("=", ">", ">=", "<", "<=", "<>", "in", "not in", "all")          
     * @return
     */
    public <T extends Persistent<PK>, PK extends GenericPK> List<T> find(AppContext app, Class<T> type,
            List<DataStoreQueryField> queryFields, List<DataStoreFilterOperator> filterOperators);
    
    /**
     * Generic finder
     * 
     * @param <T>
     *            type of returned objects
     * @param <PK>
     *            type of returned objects' PK
     * @param app
     *            context
     * @param type
     *            type of returned objects
     * @param queryFields
     *            a List of key/values to express queries.
     * @param filterOperators
     *            a List of filter operators ("=", ">", ">=", "<", "<=", "<>", "in", "not in", "all")
     * @param orderBy
     *            order the result. Example:
     *            <ul>
     *            <li>age</li>
     *            <li>-age (descending order)</li>
     *            <li>age,date</li>
     *            <li>age,-date (age ascending, date descending)</li>
     *            </ul>
     * @return
     */
    public <T extends Persistent<PK>, PK extends GenericPK> List<T> find(AppContext app, Class<T> type,
            List<DataStoreQueryField> queryFields, List<DataStoreFilterOperator> filterOperators, String orderBy);
}
