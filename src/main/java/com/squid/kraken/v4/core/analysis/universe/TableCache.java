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
package com.squid.kraken.v4.core.analysis.universe;

import java.util.HashMap;

import com.squid.core.database.model.Table;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;

/**
 * a very simple cache for table - DON'T USE IT, cache is evil.
 * 
 * @author sergefantino
 *
 */
@Deprecated
public class TableCache {
	
	class TableEntry {
		
		public Table table;
		public String expression;
		public TableEntry(Table table, String expression) {
			super();
			this.table = table;
			this.expression = expression;
		}

	}
	
	private HashMap<DomainPK, TableEntry> cache = null;
	
	private TableCache() {
		cache = new HashMap<DomainPK, TableCache.TableEntry>();
	}
	
	public Table get(Domain domain) {
		TableEntry entry = cache.get(domain.getId());
		if (entry!=null && domain.getSubject()!= null && entry.expression.equals(domain.getSubject().getValue()) && !entry.table.isStale()) {
			return entry.table;
		} else {
			// remove from cache
			cache.remove(domain.getId());
			return null;
		}
	}
	
	public void put(Domain domain, Table table) {
		synchronized (table) {
			cache.put(domain.getId(), new TableEntry(table, domain.getSubject().getValue()));
		}
	}

}
