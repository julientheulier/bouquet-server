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
package com.squid.kraken.v4.core.analysis.scope;

import com.squid.core.domain.DomainBase;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.Domain;

@Deprecated
public class UniverseDomainImp 
extends DomainBase
implements UniverseDomain {
	
	private Space space;

	public UniverseDomainImp(Space space) {
		super(UniverseDomain.DOMAIN);
		this.space = space;
	}
	
	@Override
	public Space getSpace() {
		return space;
	}

	@Override
	public Object getAdapter(Class<?> adapter) {
		if (adapter.equals(Domain.class)) {
			return this.space.getRoot();
		} else if (adapter.equals(Space.class)) {
			return this.space;
		} else {
			return null;
		}
	}
	
	@Override
	public String toString() {
	    return space!=null?"S("+space.prettyPrint()+")":"S(undefined)";
	}

}
