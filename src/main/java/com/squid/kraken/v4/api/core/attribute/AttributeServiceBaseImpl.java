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
package com.squid.kraken.v4.api.core.attribute;

import java.util.List;

import com.squid.kraken.v4.api.core.GenericServiceImpl;
import com.squid.kraken.v4.model.Attribute;
import com.squid.kraken.v4.model.AttributePK;
import com.squid.kraken.v4.model.DimensionPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.dao.AttributeDAO;

public class AttributeServiceBaseImpl extends GenericServiceImpl<Attribute, AttributePK> {

    private static AttributeServiceBaseImpl instance;

    public static AttributeServiceBaseImpl getInstance() {
        if (instance == null) {
            instance = new AttributeServiceBaseImpl();
        }
        return instance;
    }

    public AttributeServiceBaseImpl() {
        // made private for singleton access
        super(Attribute.class);
    }
    
	public List<Attribute> readAll(AppContext ctx,
			DimensionPK dimensionId) {
		List<Attribute> attributes = ((AttributeDAO) factory
				.getDAO(Attribute.class)).findByDimension(ctx, dimensionId);
		return attributes;
	}


}
