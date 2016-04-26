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

import org.mongodb.morphia.query.Query;

import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.ComputationJob;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.FacetSelection;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.persistence.AccessRightsPersistentDAO;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DataStore;
import com.squid.kraken.v4.persistence.MongoDBHelper;

public abstract class JobDAO<T extends Persistent<PK>, PK extends GenericPK> extends AccessRightsPersistentDAO<T, PK> implements ExpirableDAO<T> {

    public JobDAO(Class<T> type, DataStore ds) {
        super(type, ds);
    }
    
    public List<T> findAllNotDone() {
        Query<T> q = MongoDBHelper.getDatastore().createQuery(type);
        q.field("status").equal(ComputationJob.Status.RUNNING);
        return q.asList();
    }
    
    public List<T> findAllExpired(long expDate) {
        Query<T> q = MongoDBHelper.getDatastore().createQuery(type);
        q.field("temporary").notEqual(false);
        q.field("creationTime").lessThan(expDate);
        return q.asList();
    }

    /**
     * Role {@link Role#WRITE} on the parent object is required to perform the operation except for temporary
     * {@link ComputationJob}s which can be created only with parent read access.<br>
     * Access rights are inherited from its parent (see
     * {@link AccessRightsUtils#setAccessRights(AppContext, Persistent, Persistent)}).<br>
     */
	@Override
	public T create(AppContext ctx, T newInstance) {
		// check the access rights
        Persistent<? extends GenericPK> parent = newInstance.getParentObject(ctx);
        ComputationJob<?,?> job = (ComputationJob<?,?>) newInstance;
        if (job.getTemporary()) {
            // temporary jobs can be created/updated with read access on parent
            AccessRightsUtils.getInstance().checkRole(ctx, parent, Role.READ);
        } else {
            // non temporary jobs need write role on parent
            AccessRightsUtils.getInstance().checkRole(ctx, parent, Role.WRITE);
        }

        AccessRightsUtils.getInstance().setAccessRights(ctx, newInstance, parent);
        job.setCreationTime(System.currentTimeMillis());
        return ds.create(ctx, newInstance);
	}

	protected FacetSelection getCleanFacetSelection(FacetSelection selection) {
    	if (selection == null) {
    		return null;
    	} else {
	    	List<Facet> facets = new ArrayList<Facet>();
	    	for (Facet facet : selection.getFacets()) {
	    		if (!facet.getSelectedItems().isEmpty()) {
	    			Facet f = Facet.createSelectionfacet(facet);
	    			facets.add(f);
	    		}
	    	}
	    	selection.setFacets(facets);
	    	return selection;
    	}
    }

}
