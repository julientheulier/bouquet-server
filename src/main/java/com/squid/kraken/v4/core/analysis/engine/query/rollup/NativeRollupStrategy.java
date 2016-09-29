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

import java.util.ArrayList;
import java.util.List;

import com.squid.core.domain.aggregate.GroupingIDOperatorDefinition;
import com.squid.core.domain.aggregate.GroupingOperatorDefinition;
import com.squid.core.domain.operators.OperatorDefinition;
import com.squid.core.domain.operators.OperatorScope;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.ISelectPiece;
import com.squid.core.sql.render.RenderingException;
import com.squid.core.sql.render.SQLSkin;
import com.squid.kraken.v4.core.analysis.engine.query.SimpleQuery;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.OrderByMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.QueryMapper;
import com.squid.kraken.v4.core.analysis.model.GroupByAxis;
import com.squid.kraken.v4.core.sql.SelectUniversal;
import com.squid.kraken.v4.core.sql.script.SQLScript;

/**
 * rollup strategy for database supporting Rollup natively (Oracle, SQLServer).
 * We basically add the grouping_id() and grouping() for each level
 * @author sergefantino
 *
 */
public class NativeRollupStrategy extends BaseRollupStrategy {

	public NativeRollupStrategy(SimpleQuery query, SQLSkin skin,
			SelectUniversal select, List<GroupByAxis> rollup, boolean grandTotal, QueryMapper mapper) {
		super(query, skin, select, rollup, grandTotal, mapper);
	}
	
	@Override
	public SQLScript generateScript() throws SQLScopeException {
		try {
			OperatorDefinition grouping_id = OperatorScope.getDefault().lookupByID(GroupingIDOperatorDefinition.ID);
			OperatorDefinition grouping = OperatorScope.getDefault().lookupByExtendedID(GroupingOperatorDefinition.ID);	   
			if (grouping_id==null) {
				throw new RenderingException("undefined function GROUPING_ID");
			}  
			if (grouping==null) {
				throw new RenderingException("undefined function GROUPING");
			}
			// create the main copy
	        SelectUniversal main = createSelect();
	        addAxis(main);// add all
	        // add orderBy
	        List<OrderByMapping> orderByMapping = inlineOrderBy(main);
	        // add levels to order by rollup
	        List<ISelectPiece> levels = new ArrayList<>();
	        List<ExpressionAST> rollex = new ArrayList<ExpressionAST>(getRollup().size());
	        for (int idx=1;idx<=getRollup().size();idx++) {
	        	GroupByAxis axis = getRollup().get(idx-1);
	        	ExpressionAST expr = axis.getAxis().getDefinitionSafe();
		        // add actual rollup
		        main.getGrouping().addRollup(expr);
		        // add level_ID
	        	rollex.add(expr);
	            levels.add(main.select(ExpressionMaker.op(grouping, expr), "level_"+idx));
	        }
	        ISelectPiece levelIDPiece = main.select(ExpressionMaker.op(grouping_id, rollex), "levelID");
	        if (hasGrandTotal()) {
	        	// case(grouping_id(rollup...)=2^n,1,0)
	        	ExpressionAST exp1 = ExpressionMaker.op(grouping_id, rollex);
	        	ExpressionAST exp2 = ExpressionMaker.CASE(ExpressionMaker.EQUAL(exp1, ExpressionMaker.CONSTANT(1>>rollex.size())),ExpressionMaker.CONSTANT(1),ExpressionMaker.CONSTANT(0));
	            levels.add(main.select(exp2, "level_0"));
	        }
	        //
	        // pretty-print
	        StringBuilder sql = new StringBuilder();
	        sql.append(getSkin().quoteComment("ROLLUP / Native strategy")).append("\n");
	        sql.append(main.render());
            // prepare the order by
	        sql.append("\n");
            sql.append(renderOrderBy(main, levels, orderByMapping));
            // add the regular limit
            if (getSelect().getStatement().hasLimitValue()) {
                sql.append("\nLIMIT "+getSelect().getStatement().getLimitValue());
            }
	        // add rollup metadata mapping (at the end to avoid side-effect)
	        addLevelMapping(levelIDPiece);
			return new SQLScript(sql.toString(), getMapper());
	    } catch (Exception e) {
	        throw new SQLScopeException("cannot create a rollup statement", e);
	    }
	}

}
