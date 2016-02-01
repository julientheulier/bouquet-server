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
package com.squid.kraken.v4.core.expression.reference;

import com.squid.core.database.model.Column;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.model.domain.ProxyDomainDomain;
import com.squid.kraken.v4.model.Domain;

/**
 * this is a reference to a column object, in the context of a given Domain.
 * We need to keep the reference to the domain in order to compute the correct source-domain.
 * @author sfantino
 *
 */
public class ColumnDomainReference extends com.squid.core.expression.reference.ColumnReference {
	
	public ColumnDomainReference(Universe universe, Domain domain, Column reference) {
		super(reference, new ProxyDomainDomain(universe, domain));
	}

    public ColumnDomainReference(Space space, Column reference) {
        super(reference, new ProxyDomainDomain(space.getUniverse(), space.getDomain()));
    }
}
