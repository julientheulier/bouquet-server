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

import com.google.common.base.Optional;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;

/**
 * Generic Data Access Object.
 * 
 * @param <T>
 *            Object Type
 * @param <PK>
 *            Primary Key type
 */
public interface GenericDAO<T, PK> {

    /**
     * Persist the newInstance object into database
     */
    T create(AppContext ctx, T newInstance);

    /**
     * Retrieve an object that was previously persisted to the database using the indicated id as primary key
     * 
     * @return the object wrapped as Optional to handle not found (null) case.
     */
    Optional<T> read(AppContext ctx, PK id);
    
    /**
     * Check if an object exists.
     */
    boolean exists(AppContext ctx, PK id);

    /**
     * Retrieve an object that was previously persisted to the database using the indicated id as primary key
     * 
     * @return the object found or throws a {@link ObjectNotFoundAPIException}
     */
    T readNotNull(AppContext ctx, PK id);

    /** Save changes made to a persistent object. */
    void update(AppContext ctx, T transientObject);

    /** Remove an object from persistent storage in the database */
    void delete(AppContext ctx, PK id);
    
    /**
     * Used by decorators.
     * @return
     */
    public Class<T> getType();

}