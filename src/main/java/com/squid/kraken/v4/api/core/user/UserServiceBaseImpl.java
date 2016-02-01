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
package com.squid.kraken.v4.api.core.user;

import java.util.List;

import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.query.Query;

import com.squid.kraken.v4.api.core.GenericServiceImpl;
import com.squid.kraken.v4.model.ProjectUser;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.MongoDBHelper;
import com.squid.kraken.v4.persistence.dao.UserDAO;

public class UserServiceBaseImpl extends GenericServiceImpl<User, UserPK> {

    private static UserServiceBaseImpl instance;

    public static UserServiceBaseImpl getInstance() {
        if (instance == null) {
            instance = new UserServiceBaseImpl();
        }
        return instance;
    }

    private UserServiceBaseImpl() {
        // made private for singleton access
        super(User.class);
    }

    @Override
    public User read(AppContext ctx, UserPK objectId) {
        return super.read(ctx, objectId);
    }
    
	public List<User> readAll(AppContext ctx) {
		UserDAO userDAO = (UserDAO) factory.getDAO(User.class);
		return userDAO.findByCustomer(ctx, ctx.getCustomerPk());
	}

    @Override
    public boolean delete(AppContext ctx, UserPK userPk) {
        try {
            Datastore ds = MongoDBHelper.getDatastore();
            Query<ProjectUser> query = ds.createQuery(ProjectUser.class).filter("id.userId =", userPk.getUserId());
            ds.findAndDelete(query);
        } catch (Exception e) {
            return false;
        }

        return super.delete(ctx, userPk);
    }
    
    @Override
    public User store(AppContext ctx, User user) {
        user.setLogin(user.getLogin().toLowerCase());
        return super.store(ctx, user);
    }
}
