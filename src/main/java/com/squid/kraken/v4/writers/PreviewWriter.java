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
package com.squid.kraken.v4.writers;

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.caching.redis.RedisCacheManager;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;

/**
 * transform a RawMatrix into a DataMatrix
 * @author hoa
 *
 */
public class PreviewWriter extends QueryWriter {

	private DataMatrix dm = null;

	public PreviewWriter() {
	}

	@Override
	public void write() throws ScopeException {
		if (val != null) {
			if (val instanceof RawMatrix) {
				this.dm = new DataMatrix(db, (RawMatrix) val, mapper);
			} else {
				if (val instanceof RedisCacheValuesList) {
					RawMatrix raw = RedisCacheManager.getInstance().getRawMatrix(val.getRedisKey());
					this.dm = new DataMatrix(db, raw, mapper);
				}
			}
		}
	}

	public DataMatrix getDataMatrix() {
		return dm;
	};
	
}
