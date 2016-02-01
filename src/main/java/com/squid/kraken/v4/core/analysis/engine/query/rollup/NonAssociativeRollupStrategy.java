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

import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.ISelectPiece;
import com.squid.core.sql.render.SQLSkin;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.query.SimpleQuery;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.OrderByMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.QueryMapper;
import com.squid.kraken.v4.core.analysis.model.GroupByAxis;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.sql.SelectUniversal;
import com.squid.kraken.v4.core.sql.script.SQLScript;

/**
 * Support a non-associative rollup strategy
 * @author sergefantino
 *
 */
public class NonAssociativeRollupStrategy extends BaseRollupStrategy {
	
	public NonAssociativeRollupStrategy(SimpleQuery query, SQLSkin skin, SelectUniversal select, List<GroupByAxis> rollup, boolean grandTotal, QueryMapper mapper) {
		super(query,skin,select,rollup,grandTotal,mapper);
	}
	
	@Override
	public SQLScript generateScript() throws SQLScopeException {
        // handle the rollup
        // V1: use UNION for any database, even if it supports native rollup
        ArrayList<SelectUniversal> union = new ArrayList<>();
        ArrayList<Axis> totals = new ArrayList<Axis>();
        boolean topLevelRollup = hasGrandTotal();
        try {
            // create the main copy
            SelectUniversal main = createSelect();
            addAxis(main);// add all
            List<OrderByMapping> orderByMapper = inlineOrderBy(main);
            // add levels to order by rollup
            List<ISelectPiece> levels = new ArrayList<>();
            if (topLevelRollup) {
                levels.add(main.select(ExpressionMaker.CONSTANT(0), "level_0"));
            }
            for (int idx=1;idx<=getRollup().size();idx++) {
                levels.add(main.select(ExpressionMaker.CONSTANT(0), "level_"+idx));
            }
            ISelectPiece levelIDPiece = main.select(ExpressionMaker.NULL(), "levelID");
            main.getStatement().addComment("raw data");
            union.add(main);
            int level = 0;
            // add the grand total
            if (topLevelRollup) {
            	union.add(renderRollupSelect(orderByMapper, topLevelRollup, level, totals));
            	/*
                SelectUniversal grandTotal = createSelect();
                addAxis(grandTotal, totals);// ignore require==false
                // add levels to order by rollup
                if (topLevelRollup) {
                    grandTotal.select(ExpressionMaker.CONSTANT(1), "level_0");
                }
                for (int idx=1;idx<=getRollup().size();idx++) {
                    grandTotal.select(ExpressionMaker.CONSTANT(1), "level_"+idx);
                }
                grandTotal.select(ExpressionMaker.CONSTANT(level), "levelID");
                // no need to add orderBy explicitly
                // (...)
                grandTotal.getStatement().setComment("grand total");
                union.add(grandTotal);
                */
            }
            // for each level, create a new statement
            for (GroupByAxis gpTotal : getRollup()) {
                level++;
            	Axis total = gpTotal.getAxis();
                // add the total
                totals.add(total);
                union.add(renderRollupSelect(orderByMapper, topLevelRollup, level, totals));
            }
            //
            // perform the UNION by hand...
            StringBuilder sql = new StringBuilder();
            for (SelectUniversal select : union) {
                if (sql.length()>0) {
                	sql.append("\nUNION ALL\n");
                } else {
                    sql.append(getSkin().quoteComment("ROLLUP / UNION STRATEGY (non associative)"));
                }
                sql.append(select.render());
            }
            // prepare the order by
            sql.append("\n");
            sql.append(renderOrderBy(main, levels, orderByMapper));
            // add the regular limit
            if (getSelect().getStatement().hasLimitValue()) {
                sql.append("\nLIMIT "+getSelect().getStatement().getLimitValue());
            }
            // add rollup metadata mapping (at the end to avoid side-effect)
            addLevelMapping(levelIDPiece);
            // closing
            sql.append("\n");
            return new SQLScript(sql.toString());
        } catch (Exception e) {
            throw new SQLScopeException("cannot create a rollup statement", e);
        }
    }
    
    protected SelectUniversal renderRollupSelect(List<OrderByMapping> orderByMapper, boolean topLevelRollup, int level, ArrayList<Axis> totals) throws ScopeException, SQLScopeException, ComputingException, InterruptedException {
        SelectUniversal subselect = createSelect();
        // iter through mapping
        // axis
        addAxis(subselect, totals);
        // orderBy
        //List<ISelectPiece> moreStuffToAddInTheGroupBy = new ArrayList<ISelectPiece>();
        for (OrderByMapping ox : orderByMapper) {
        	if (getMapper().find(ox.getOrderBy().getExpression())==null) {
            	if (includeOrderBy(ox.getOrderBy(),totals)) {
            		//moreStuffToAddInTheGroupBy.add(ox.getPiece());
                    subselect.select(ox.getOrderBy().getExpression(),ox.getPiece().getAlias());
            	} else {
                    // else replace the expression with a null value
                    subselect.select(ExpressionMaker.NULL(),ox.getPiece().getAlias());
            	}
        	}
        }
        // add levels to order by rollup
        if (topLevelRollup) {
            subselect.select(ExpressionMaker.CONSTANT(0>=level?1:0), "level_0");
        }
        for (int idx=1;idx<=getRollup().size();idx++) {
            subselect.select(ExpressionMaker.CONSTANT(idx>=level?1:0), "level_"+idx);
        }
        subselect.select(ExpressionMaker.CONSTANT(level), "levelID");
        subselect.getStatement().addComment("rollup at level #"+level);
        return subselect;
    }

}
