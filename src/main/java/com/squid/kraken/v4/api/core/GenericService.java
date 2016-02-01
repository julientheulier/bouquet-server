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

import java.util.Set;

import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.persistence.AppContext;

/**
 * Generic Service interface exposing a public API.<br>
 * The customerId is always passed to check the caller is not accessing unauthorized objects.<br>
 * If such a method is called on objects belonging to another Customer, a {@link RuntimeException} will be raised.
 * 
 * @param <T>
 *            the Service target object type.
 * @param <PK>
 *            the Service target object PK type.
 */
public interface GenericService<T, PK> {

    /**
     * Reads an object.<br>
     * If the AppContext contains a 'refresh' command, and the User has WRITE rights on the object, the object will be
     * refreshed.
     * 
     * @param customerId
     * @param objectId
     *            The id of the object to read.
     * @return the object
     * @throws a
     *             ObjectNotFoundAPIException if the object is not found.
     */
    public T read(AppContext ctx, PK objectId);
    
    /**
     * Reads an object (in Deep-Read mode).<br>
     * If the AppContext contains a 'refresh' command, and the User has WRITE rights on the object, the object will be
     * refreshed.
     * 
     * @param customerId
     * @param objectId
     *            The id of the object to read.
     * @param deepRead
     *            if true the complete object graph will be read.
     * @return the object
     * @throws a
     *             ObjectNotFoundAPIException if the object is not found.
     */
    public T read(AppContext ctx, PK objectId, boolean deepRead);

    /**
     * Delete an object.
     * 
     * @param customerId
     * @param objectId
     *            The id of the object to delete.
     * @return Whether the object was successfully deleted.
     */
    public boolean delete(AppContext ctx, PK objectId);

    /**
     * Invalidate an object.
     * 
     * @param customerId
     * @param objectId
     *            The id of the object to invalidate.
     * @return Whether the object was successfully invalidated.
     */
    public boolean invalidate(AppContext ctx, PK objectId);

    public T store(AppContext ctx, T object);

    /**
     * Read the object's access rights.<br>
     * Access Rights rules :
     * <ul>
     * <li>Every object has a set of {@link AccessRight} defined and describing allowed user operations</li>
     * <li>All operations performed on an object must include an {@link AppContext} which states the user performing the
     * call</li>
     * <li>Every object must have at least one OWNER right defined</li>
     * <li>All new objects will be initialized with their parent's rights</li>
     * <li>Creator of an object will have OWNER rights</li>
     * </ul>
     * Rights definitions :
     * <ul>
     * <li>{@link Role#READ}</li>
     * <ul>
     * <li>Can read the object and its connections</li>
     * <li>Can run Analysis and Facet jobs</li>
     * </ul>
     * <li>{@link Role#WRITE}</li>
     * <ul>
     * <li>Have all READ rights
     * <li>Can update the object and add child objects (to the object's connections)</li>
     * <li>Can update object's READ and WRITE access rights</li>
     * </ul>
     * <li>{@link Role#OWNER}</li>
     * <ul>
     * <li>Have all WRITE rights
     * <li>Can delete the object</li>
     * <li>Can update object's OWNER access rights</li>
     * </ul>
     * </ul>
     * 
     * @param ctx
     * @param objectId
     * @return
     */
    public Set<AccessRight> readAccessRights(AppContext ctx, PK objectId);

    /**
     * Update an object's access rights.
     * 
     * @param ctx
     *            an AppContext whom user has {@link Role#OWNER}
     * @param objectId
     *            the object to update
     * @param accessRights
     *            the new access rights to apply (must at least have one {@link Role#OWNER})
     * @return the updated access rights
     */
    public Set<AccessRight> storeAccessRights(AppContext ctx, PK objectId, Set<AccessRight> accessRights);

}
