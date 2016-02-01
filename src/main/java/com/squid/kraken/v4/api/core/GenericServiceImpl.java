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


import com.google.common.base.Optional;
import com.squid.kraken.v4.caching.awsredis.RedisCacheManager;
import com.squid.kraken.v4.caching.awsredis.generationalkeysserver.RedisKey;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.model.visitor.CreateOrUpdateVisitor;
import com.squid.kraken.v4.model.visitor.DeepReadVisitor;
import com.squid.kraken.v4.model.visitor.DeletionVisitor;
import com.squid.kraken.v4.model.visitor.InvalidationVisitor;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;

/**
 * Base implementation for {@link GenericService}.
 * 
 */
public abstract class GenericServiceImpl<T extends Persistent<PK>, PK extends GenericPK> implements
        GenericService<T, PK> {

    protected DAOFactory factory = DAOFactory.getDAOFactory();

    private final Class<T> clazz;

    public GenericServiceImpl(Class<T> clazz) {
        super();
        this.clazz = clazz;
    }

    @Override
    public boolean delete(AppContext ctx, PK objectId) {
        Optional<T> object = factory.getDAO(clazz).read(ctx, objectId);
        if (object.isPresent()) {
        	DeepReadVisitor v1 = new DeepReadVisitor(ctx);
            object.get().accept(v1);
            DeletionVisitor deletionVisitor = new DeletionVisitor(ctx);
            object.get().accept(deletionVisitor);
            deletionVisitor.commit();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean invalidate(AppContext ctx, PK objectId) {
    	T object = factory.getDAO(clazz).readNotNull(ctx, objectId);
        // perform recursive invalidation
    	DeepReadVisitor v1 = new DeepReadVisitor(ctx);
        object.accept(v1);
        InvalidationVisitor invalidationVisitor = new InvalidationVisitor(ctx, object);
        object.accept(invalidationVisitor);
        invalidationVisitor.commit();
        return true;
    }

    @Override
    public T read(AppContext ctx, PK objectId) {
        return read(ctx, objectId, ctx.isDeepRead());
    }

    @Override
    public T read(AppContext ctx, PK objectId, boolean deepRead) {
        T object = factory.getDAO(clazz).readNotNull(ctx, objectId);
        if (ctx.isRefresh()) {
            // check for owner role
            AccessRightsUtils.getInstance().checkRole(ctx, object, Role.WRITE);
            // perform recursive invalidation
            DeepReadVisitor v1 = new DeepReadVisitor(ctx);
            object.accept(v1);
            InvalidationVisitor invalidationVisitor = new InvalidationVisitor(ctx, object);
            object.accept(invalidationVisitor);
            invalidationVisitor.commit();
        } else if (deepRead) {
        	DeepReadVisitor v1 = new DeepReadVisitor(ctx);
            object.accept(v1);
        }
        return object;
    }

    /**
     * Create/Update a Persistent object (performing deep creation/update) .<br>
     * 
     * @return the instance.
     */
    @Override
    public T store(AppContext ctx, T object) {
        // check the id
        if (object.getId() == null) {
            throw new APIException("Object should not have a null id", ctx.isNoError());
        }
        CreateOrUpdateVisitor createOrUpdateVisitor = new CreateOrUpdateVisitor(ctx);
        object.accept(createOrUpdateVisitor);
        
        return object;
    }

    /**
     * Update a Persistent object.<br>
     * 
     * @return the updated instance.
     */
    protected T create(AppContext ctx, T object) {
        // perform the create
        factory.getDAO(clazz).create(ctx, object);
        return object;
    }

    @Override
    public Set<AccessRight> readAccessRights(AppContext ctx, PK objectId) {
        T object = read(ctx, objectId);
        return object.getAccessRights();
    }

    @Override
    public Set<AccessRight> storeAccessRights(AppContext ctx, PK objectId, Set<AccessRight> accessRights) {
        T object = factory.getDAO(clazz).readNotNull(ctx, objectId);
        object.setAccessRights(accessRights);
        factory.getDAO(clazz).update(ctx, object);
        return accessRights;
    }
    
    // cache extension
    
    public Object readCacheInfo(AppContext ctx, PK projectPK) {
		//
        T object = read(ctx, projectPK);
		if (object!=null) {
			RedisKey key = RedisCacheManager.getInstance().getKey(object.getId().toUUID());
			return key;
		} else {
			return "undefined project";
		}
	}
    
    public Object refreshCache(AppContext ctx, PK projectPK) {
		//
        T object = read(ctx, projectPK);
		if (object!=null) {
			RedisCacheManager.getInstance().refresh(object.getId().toUUID());
			RedisKey key = RedisCacheManager.getInstance().getKey(object.getId().toUUID());
			return key;
		} else {
			return "undefined project";
		}
	}
    
}
