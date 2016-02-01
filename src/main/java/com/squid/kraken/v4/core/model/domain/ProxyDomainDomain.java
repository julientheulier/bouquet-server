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

import com.squid.core.database.domain.TableDomain;
import com.squid.core.database.model.Table;
import com.squid.core.domain.DomainObject;
import com.squid.core.domain.IDomain;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.Domain;

/**
 * This is a DomainDomain associated with a single Domain object.
 *
 */
public class ProxyDomainDomain
extends DomainObject
implements DomainDomain
{
	
	private Universe universe;
	private Domain domain;
	
	public ProxyDomainDomain() {
		super(DomainDomain.DOMAIN);
	}
	
	public ProxyDomainDomain(Universe universe, Domain domain) {
		this();
		this.universe = universe;
		this.domain = domain;
		setName("ProxyDomain["+domain.getName()+"]");
	}

	@Override
	public Domain getDomain() {
		return domain;
	}
	
	@Override
	public Object getAdapter(Class<?> adapter) {
		if (adapter.equals(Domain.class)) {
			return this.domain;
		}
		return super.getAdapter(adapter);
	}
	
	@Override
	public boolean isInstanceOf(IDomain domain) {
		if (this==domain) {
			return true;
		} else if (domain==DomainDomain.DOMAIN) {
			return true;
		} else if (domain instanceof ProxyDomainDomain) {
			ProxyDomainDomain that = (ProxyDomainDomain)domain;
			return this.domain.equals(that.domain);
		} else if (domain instanceof TableDomain) {
			TableDomain that = (TableDomain)domain;
			if (that.getTable()!=null) {
				Table table = getTable();
				if (table!=null && table.equals(that.getTable())) {
					return true;
				}
			}
		}
		// else
		return getParentDomain().isInstanceOf(domain);
	}
	
	/**
	 * return the table associated with the domain from the underlying physics, or null if none defined
	 * @return
	 */
	protected Table getTable() {
		try {
			return universe.getTable(this.domain);
		} catch (ScopeException e) {
			return null;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((domain == null) ? 0 : domain.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (getClass() != obj.getClass())
			return false;
		ProxyDomainDomain other = (ProxyDomainDomain) obj;
		if (domain == null) {
			if (other.domain != null)
				return false;
		} else if (!domain.equals(other.domain))
			return false;
		return true;
	}
	

}
