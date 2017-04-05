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
package com.squid.kraken.v4.core.analysis.engine.query.rollup;

import java.util.List;

import com.squid.core.sql.db.features.IRollupStrategySupport;
import com.squid.core.sql.db.templates.SkinFactory;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.ISkinFeatureSupport;
import com.squid.core.sql.render.SQLSkin;
import com.squid.kraken.v4.core.analysis.engine.query.SimpleQuery;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.QueryMapper;
import com.squid.kraken.v4.core.analysis.model.GroupByAxis;
import com.squid.kraken.v4.core.sql.SelectUniversal;

public class RollupStrategySelector {
	
	public static IRollupStrategy selectStrategy(SimpleQuery query, SelectUniversal select,
			List<GroupByAxis> rollup, boolean grandTotal, QueryMapper mapper) throws SQLScopeException {
		//
		// T2033 - make a mapper copy to avoid side effects
    	QueryMapper copy = new QueryMapper(mapper);// T2033
		SQLSkin skin = SkinFactory.INSTANCE.createSkin(select.getDatabase());
		//
		// using the skin feature support to select the strategy
		ISkinFeatureSupport support = skin.getFeatureSupport(IRollupStrategySupport.ID);
		if (support.equals(ISkinFeatureSupport.IS_NOT_SUPPORTED) || !(support instanceof IRollupStrategySupport)) {
			throw new SQLScopeException("this database does not support the ROLLUP feature");
		} else {
			IRollupStrategySupport strategy = (IRollupStrategySupport)support;
			if (query.isAssociative()) {
				if (strategy.getStrategy().equals(IRollupStrategySupport.Strategy.USE_BUILTIN_SUPPORT)) {
					return new NativeRollupStrategy(query, skin, select, rollup, grandTotal, copy);
				} else if (strategy.getStrategy().equals(IRollupStrategySupport.Strategy.DO_NOT_OPTIMIZE)) {
					return new NonAssociativeRollupStrategy(query, skin, select, rollup, grandTotal, copy);
				} else if (strategy.getStrategy().equals(IRollupStrategySupport.Strategy.OPTIMIZE_USING_WITH)) {
					return new AssociativeRollupStrategy(query, skin, select, rollup, grandTotal, copy);
				} else if (strategy.getStrategy().equals(IRollupStrategySupport.Strategy.OPTIMIZE_USING_TEMPORARY)) {
					return new AssociativeTemporaryRollupStrategy(query, skin, select, rollup, grandTotal, copy);
				} else {
					// default to not optimize
					return new NonAssociativeRollupStrategy(query, skin, select, rollup, grandTotal, copy);
				}
			} else {
				if (strategy.getStrategy().equals(IRollupStrategySupport.Strategy.USE_BUILTIN_SUPPORT)) {
					return new NativeRollupStrategy(query, skin, select, rollup, grandTotal, copy);
				} else {
					// do not optimize
					return new NonAssociativeRollupStrategy(query, skin, select, rollup, grandTotal, copy);
				}
			}
		}
	}

}
