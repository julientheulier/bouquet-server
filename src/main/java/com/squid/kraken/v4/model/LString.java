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
package com.squid.kraken.v4.model;

import java.util.Set;

import com.squid.kraken.v4.model.visitor.ModelVisitor;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;

/**
 * Specific persistent object used to handle Localization.
 * @see HasLocalizedName
 */
@SuppressWarnings("serial")
public class LString extends PersistentBaseImpl<LStringPK> {

    private String value;
    
    public LString() {
        super(null);
    }

    public LString(LStringPK id, String value) {
        super(id);
        this.value = value;
    }

    public LStringPK getId() {
        return super.getId();
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
    
    /**
     * Visitor.
     */
    public void accept(ModelVisitor visitor) {
        visitor.visit(this);
    }

    public Set<AccessRight> getAccessRights() {
        // not applicable
        return null;
    }

    public void setAccessRights(Set<AccessRight> accessRights) {
        // not applicable
    }

    public Persistent<?> getParentObject(AppContext ctx) {
        return DAOFactory.getDAOFactory().getDAO(Customer.class).readNotNull(ctx, new CustomerPK(id.getCustomerId()));
    }
    
}
