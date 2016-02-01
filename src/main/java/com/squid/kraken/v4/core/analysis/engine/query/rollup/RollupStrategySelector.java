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

import com.squid.core.database.impl.DataSourceReliable;
import com.squid.core.sql.db.features.IRollUpSupport;
import com.squid.core.sql.db.templates.SkinFactory;
import com.squid.core.sql.render.ISkinFeatureSupport;
import com.squid.core.sql.render.ISkinPref;
import com.squid.core.sql.render.SQLSkin;
import com.squid.kraken.v4.core.analysis.engine.query.SimpleQuery;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.QueryMapper;
import com.squid.kraken.v4.core.analysis.model.GroupByAxis;
import com.squid.kraken.v4.core.sql.SelectUniversal;

public class RollupStrategySelector {
	
	public static IRollupStrategy selectStrategy(SimpleQuery query, SelectUniversal select,
			List<GroupByAxis> rollup, boolean grandTotal, QueryMapper mapper) {
		//
		SQLSkin skin = SkinFactory.INSTANCE.createSkin(select.getDatabase());
		if (false && skin.getFeatureSupport(IRollUpSupport.ID)==ISkinFeatureSupport.IS_SUPPORTED) {
			// we still need to add the grouping ID
			return new NativeRollupStrategy(query, skin, select, rollup, grandTotal, mapper);
		} else {
			if (query.isAssociative()) {
				if (skin.getPreferences(DataSourceReliable.FeatureSupport.ROLLUP) == ISkinPref.TEMP) {
					return new AssociativeTemporaryRollupStrategy(query, skin, select, rollup, grandTotal, mapper);
				} else {
					return new AssociativeRollupStrategy(query, skin, select, rollup, grandTotal, mapper);
				}
			} else {
				return new NonAssociativeRollupStrategy(query, skin, select, rollup, grandTotal, mapper);
			}
		}
	}

}
