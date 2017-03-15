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
package com.squid.kraken.v4.core.sql.script;

import com.squid.core.sql.render.RenderingException;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.QueryMapper;
import com.squid.kraken.v4.core.sql.SelectUniversal;

/**
 * a SQLScript is a way to execute complex SQL script including preliminary task
 * 
 * A SelectUniversal can be added as a final step. The idea here is to be able to transform that final step further.
 * 
 * @author sergefantino
 *
 */
public class SQLScript {
	
	private String sql = null;
	private SelectUniversal select = null;
	
	// T2033: because we can have side effects into the mapping, must be part of the result
	private QueryMapper mapper = null;
	
	/**
	 * create a very simple SQLScript which will only execute the given select
	 * @param select
	 */
	public SQLScript(SelectUniversal select, QueryMapper mapper) {
		this.select = select;
		this.mapper = mapper;
	}
	
	public SQLScript(String sql, QueryMapper mapper) {
		this.sql = sql;
		this.mapper = mapper;
	}
	
	public SelectUniversal getSelect() {
		return this.select;
	}
	
	public String render() throws RenderingException {
		String render = "";
		if (select!=null) {
			render += select.render();
		} else if (sql!=null) {
			render += sql;
		} else {
			throw new RenderingException("Empty SQL script");
		}
		return render;
	}
	
	/**
	 * @return the mapper
	 */
	public QueryMapper getMapper() {
		return mapper;
	}

}
