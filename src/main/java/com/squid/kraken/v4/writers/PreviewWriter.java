package com.squid.kraken.v4.writers;

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.caching.redis.RedisCacheManager;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;

public class PreviewWriter extends QueryWriter {

	private DataMatrix dm = null;

	public PreviewWriter() {
	}

	@Override
	public void write() throws ScopeException {
		if (val != null) {
			if (val instanceof RawMatrix) {
				this.dm = new DataMatrix(db, (RawMatrix) val, mapper.getMeasureMapping(), mapper.getAxisMapping());
			} else {
				if (val instanceof RedisCacheValuesList) {
					RawMatrix raw = RedisCacheManager.getInstance().getRawMatrix(val.getRedisKey());
					this.dm = new DataMatrix(db, raw, mapper.getMeasureMapping(), mapper.getAxisMapping());
				}
			}
		}
	}

	public DataMatrix getDataMatrix() {
		return dm;
	};
	
}
