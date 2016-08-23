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

import org.apache.commons.codec.digest.DigestUtils;

import com.squid.core.database.impl.DataSourceReliable.FeatureSupport;
import com.squid.core.sql.render.ISkinFeatureSupport;
import com.squid.core.sql.render.RenderingException;
import com.squid.core.sql.render.SQLSkin;
import com.squid.kraken.v4.core.analysis.engine.query.SimpleQuery;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.QueryMapper;
import com.squid.kraken.v4.core.analysis.model.GroupByAxis;
import com.squid.kraken.v4.core.sql.SelectUniversal;

/**
 * compute the ROLLUP using a SELECT...INTO statement to prepare raw level aggregation, then apply union for each level
 * @author sergefantino
 *
 */
public class AssociativeTemporaryRollupStrategy extends AssociativeRollupStrategy {

	public AssociativeTemporaryRollupStrategy(SimpleQuery query, SQLSkin skin, SelectUniversal select,
			List<GroupByAxis> rollup, boolean grandTotal, QueryMapper mapper) {
		super(query, skin, select, rollup, grandTotal, mapper);
	}
	
	@Override
	protected void renderInitialStep(SQLSkin skin, StringBuilder sql) {
        sql.append(skin.quoteComment("ROLLUP / associative TEMPORARY statement strategy")).append("\n");
	}
	
	@Override
	protected String getSubQueryIdentifierName() throws RenderingException {
        String original = "rollup:";
        if (hasGrandTotal()) original += "/grandTotal";
        for (GroupByAxis ax : getRollup()) {
        	original += "/" + ax.getAxis().getId();
        }
        original += "sql:";
        original += getSelect().render();
        return "T_"+DigestUtils.sha256Hex(original);
	}
	
	@Override
	protected void renderSubQuery(SQLSkin skin, SelectUniversal main,
			StringBuilder sql, String tempTableName) throws RenderingException {
        // add INTO temp table
        main.getStatement().setIntoTemporaryTableName(tempTableName);
        //
        sql.append(main.render());
        sql.append(";\n");
	}
	
	@Override
	protected void renderFinalStep(SQLSkin skin, StringBuilder sql, String tempTableName) {
        // drop temporary table
		// T1196: if not autocommit=true, cannot drop the table before we read the resultset
		if(skin.getFeatureSupport(FeatureSupport.AUTOCOMMIT) == ISkinFeatureSupport.IS_SUPPORTED) {
			sql.append("\nDROP TABLE "+skin.quoteTableIdentifier(tempTableName)+";\n");
		}
	}

}
