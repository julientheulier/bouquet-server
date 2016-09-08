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

import com.squid.core.domain.associative.AssociativeDomainInformation;
import com.squid.core.domain.operators.OperatorDefinition;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.model.IAlias;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.IPiece;
import com.squid.core.sql.render.ISelectPiece;
import com.squid.core.sql.render.OperatorPiece;
import com.squid.core.sql.render.RenderingException;
import com.squid.core.sql.render.SQLSkin;
import com.squid.core.sql.render.SelectPieceReference;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.query.SimpleQuery;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.AxisMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.MeasureMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.OrderByMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.QueryMapper;
import com.squid.kraken.v4.core.analysis.model.GroupByAxis;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.sql.SelectUniversal;
import com.squid.kraken.v4.core.sql.script.SQLScript;

public class AssociativeRollupStrategy extends BaseRollupStrategy {

	public AssociativeRollupStrategy(SimpleQuery query, SQLSkin skin, SelectUniversal select,
			List<GroupByAxis> rollup, boolean grandTotal, QueryMapper mapper) {
		super(query, skin, select, rollup, grandTotal, mapper);
	}
	
	@Override
	public SQLScript generateScript() throws SQLScopeException {
		try {
			SQLSkin skin = getSkin();
	        // create the main copy
	        SelectUniversal main = createSelect();
	        addAxis(main);// add all
	        // T130: inline orderBy if required
	        List<OrderByMapping> orderByMapper = inlineOrderBy(main);
            // add levels to order by rollup
            List<ISelectPiece> levels = new ArrayList<>();
            if (hasGrandTotal()) {
                levels.add(main.select(ExpressionMaker.CONSTANT(0), "level_0"));
            }
            for (int idx=1;idx<=getRollup().size();idx++) {
                levels.add(main.select(ExpressionMaker.CONSTANT(0), "level_"+idx));
            }
            ISelectPiece levelIDPiece = main.select(ExpressionMaker.CONSTANT(0), "levelID");
	        //
            final String tempTableName = getSubQueryIdentifierName();
            IAlias tempTableAlias = new IAlias() {
				@Override
				public String getAlias() {
					return tempTableName;
				}
			};
            //
            StringBuilder sql = new StringBuilder();
            renderInitialStep(skin, sql);
            //
            renderSubQuery(skin, main, sql, tempTableName);
            //
            // copy raw level
            sql.append("SELECT\n");
            boolean first = true;
            // measures
            for (MeasureMapping mx : getMapper().getMeasureMapping()) {
            	if (first) first = false; else sql.append(",\n");
            	sql.append("\n\t"+skin.quoteComment(mx.getMapping().getName()+" (Metric)"));
            	sql.append("\t"+skin.quoteIdentifier(mx.getPiece().getAlias()));
            }
            // axis
            for (AxisMapping ax : getMapper().getAxisMapping()) {
            	if (first) first = false; else sql.append(",\n");
            	sql.append("\n\t"+skin.quoteComment(ax.getAxis().getName()+" (Dimension)"));
            	sql.append("\t"+skin.quoteIdentifier(ax.getPiece().getAlias()));
            }
            // orderBy
            for (OrderByMapping ox : orderByMapper) {
            	if (getMapper().find(ox.getOrderBy().getExpression())==null) {
            		if (first) first = false; else sql.append(",\n");
            		sql.append("\t"+skin.quoteIdentifier(ox.getPiece().getAlias()));
            	}
            }
            // levels
            if (hasGrandTotal()) {
            	sql.append(",\n\t0 as "+skin.quoteIdentifier("level_0"));
            }
            for (int idx=1;idx<=getRollup().size();idx++) {
            	sql.append(",\n\t0 as "+skin.quoteIdentifier("level_"+idx));
            }
            sql.append(",\n\tNULL as "+skin.quoteIdentifier(levelIDPiece.getAlias()));
            sql.append("\nFROM ").append(skin.quoteTableIdentifier(tempTableName)).append(" \n");
            //
            // rollup
            int level = 0;
            ArrayList<Axis> totals = new ArrayList<Axis>();
            if (hasGrandTotal()) {
            	sql.append("UNION\n");
                sql.append(renderRollupSelect(skin, tempTableAlias, orderByMapper, 0, levelIDPiece, totals));
            }
            for (GroupByAxis gpTotal : getRollup()) {
                level++;
            	Axis total = gpTotal.getAxis();
            	sql.append("UNION\n");
                // add the total
                totals.add(total);
                //
                sql.append(renderRollupSelect(skin, tempTableAlias, orderByMapper, level, levelIDPiece, totals));
            }
            // add order by
            sql.append("\n");
            sql.append(renderOrderBy(prepareOrderBy(main, levels, orderByMapper)));
            // add the regular limit
            if (getSelect().getStatement().hasLimitValue()) {
                sql.append("\nLIMIT "+getSelect().getStatement().getLimitValue());
            }
            
			sql.append(skin.quoteEndOfStatement("\n"));
            renderFinalStep(skin, sql, tempTableName);


			//
            // add rollup metadata mapping (at the end to avoid side-effect)
            addLevelMapping(levelIDPiece);
			return new SQLScript(sql.toString());
		} catch (Exception e) {
            throw new SQLScopeException("cannot create a rollup statement", e);
		}
	}
	
	protected String renderRollupSelect(SQLSkin skin, IAlias tempTableAlias, List<OrderByMapping> orderByMapper, int level, ISelectPiece levelIDPiece, List<Axis> totals) throws RenderingException, ScopeException, ComputingException, InterruptedException {
		StringBuilder sql = new StringBuilder();
        sql.append("SELECT\n");
        // iter through mapping
        //
        boolean first = true;
        // measures
        for (MeasureMapping mx : getMapper().getMeasureMapping()) {
        	if (first) first = false; else sql.append(",\n");
        	OperatorDefinition opdef = getAssociativeOperator(mx.getMapping().getDefinitionSafe());
        	IPiece[] args = new IPiece[]{new SelectPieceReference(tempTableAlias, mx.getPiece())};
        	IPiece piece = new OperatorPiece(opdef, args);
        	sql.append("\t"+piece.render(skin));
        }
        // axis
        for (AxisMapping ax : getMapper().getAxisMapping()) {
        	if (first) first = false; else sql.append(",\n");
        	if (totals.contains(ax.getAxis())) {
        		sql.append("\t"+skin.quoteIdentifier(ax.getPiece().getAlias()));
        	} else {
        		sql.append("\tNULL as "+skin.quoteIdentifier(ax.getPiece().getAlias()));
        	}
        }
        // orderBy
        List<ISelectPiece> moreStuffToAddInTheGroupBy = new ArrayList<ISelectPiece>();
        for (OrderByMapping ox : orderByMapper) {
        	if (getMapper().find(ox.getOrderBy().getExpression())==null) {
        		if (first) first = false; else sql.append(",\n");
            	if (includeOrderBy(ox.getOrderBy(),totals)) {
            		moreStuffToAddInTheGroupBy.add(ox.getPiece());
            		sql.append("\t"+skin.quoteIdentifier(ox.getPiece().getAlias()));
            	} else {
            		sql.append("\tNULL as "+skin.quoteIdentifier(ox.getPiece().getAlias()));
            	}
        	}
        }
        // levels
        if (hasGrandTotal()) {
        	sql.append(",\n\t"+(0>=level?1:0)+" as "+skin.quoteIdentifier("level_0"));
        }
        for (int idx=1;idx<=getRollup().size();idx++) {
        	sql.append(",\n\t"+(idx>=level?1:0)+" as "+skin.quoteIdentifier("level_"+idx));
        }
        sql.append(",\n\t"+level+" as "+skin.quoteIdentifier(levelIDPiece.getAlias()));
        //subselect.select(ExpressionMaker.CONSTANT(levelID--), "levelID");
        sql.append("\nFROM ").append(skin.quoteTableIdentifier(tempTableAlias.getAlias())).append(" \n");
        first = true;
        for (Axis axis : totals) {
        	if (first) {
        		sql.append("\nGROUP BY ");
        		first = false;
        	} else sql.append(",\n");
        	AxisMapping ax = getMapper().find(axis);
        	sql.append("\t"+skin.quoteIdentifier(ax.getPiece().getAlias()));
        }
        for (ISelectPiece piece : moreStuffToAddInTheGroupBy) {
        	if (first) {
        		sql.append("\nGROUP BY ");
        		first = false;
        	} else sql.append(",\n");
        	sql.append("\t"+skin.quoteIdentifier(piece.getAlias()));
        }
        //
        return sql.toString();
	}

	protected void renderInitialStep(SQLSkin skin, StringBuilder sql) {
        sql.append(skin.quoteComment("ROLLUP / associative WITH statement strategy")).append("\n");
	}
	
	protected OperatorDefinition getAssociativeOperator(ExpressionAST expr) throws RenderingException {
		OperatorDefinition op = AssociativeDomainInformation.getAssociativeOperator(expr.getImageDomain());
		if (op!=null) {
			return op;
		} else {
			throw new RenderingException("unable to aggregate measure: "+expr.prettyPrint());
		}
	}
	
	protected void renderFinalStep(SQLSkin skin, StringBuilder sql, String tempTableName) {
		// TODO Auto-generated method stub
	}

	protected void renderSubQuery(SQLSkin skin, SelectUniversal main, StringBuilder sql, String tempTableName) throws RenderingException {
        sql.append("WITH ").append(skin.quoteTableIdentifier(tempTableName)).append(" AS (\n");
        sql.append(main.render());
        sql.append("\n)\n");
	}

	protected String getSubQueryIdentifierName() throws RenderingException {
		return "PRIMARY_DATA";
	}

}
