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
package com.squid.kraken.v4.core.model.domain;

import com.squid.core.domain.DomainObject;
import com.squid.core.domain.IDomain;
import com.squid.kraken.v4.model.Domain;

/**
 * The generic DomainDomain implementation, using a singleton pattern; this is the parent of all specific DomainDomain
 * @author sfantino
 *
 */
public class DomainDomainImp 
extends DomainObject 
implements DomainDomain
{
	
	/**
	 * return the Domain or null if not applicable
	 * @param source
	 * @return
	 */
	public static Domain getDomain(IDomain source) {
		if (source.isInstanceOf(DomainDomain.DOMAIN)) {
			Object adapt = source.getAdapter(Domain.class);
			if (adapt!=null && adapt instanceof Domain) {
				return (Domain)adapt;
			}
		}
		// else
		return null;
	}

    /**
     * 
     */
    public DomainDomainImp() {
        this(IDomain.OBJECT);
    }

    /**
     * @param parent
     */
    protected DomainDomainImp(IDomain parent) {
        super(parent);
        setName("Domain");
    }

    /*
    public Image getIcon() {
        return getParentDomain().getIcon();
    }
    */

    public IDomain getSingleton() {
        return DomainDomainImp.DOMAIN;
    }
    
    @Override
    public Domain getDomain() {
    	return null;
    }

}
