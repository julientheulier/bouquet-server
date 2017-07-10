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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.IOrderByPiece.ORDERING;
import com.squid.core.sql.render.ISelectPiece;
import com.squid.core.sql.render.OrderByPiece;
import com.squid.core.sql.render.RenderingException;
import com.squid.core.sql.render.SQLSkin;
import com.squid.core.sql.render.SelectPieceReference;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.query.BaseQuery.Filter;
import com.squid.kraken.v4.core.analysis.engine.query.SimpleQuery;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.AxisMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.MeasureMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.OrderByMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.QueryMapper;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.SimpleMapping;
import com.squid.kraken.v4.core.analysis.model.GroupByAxis;
import com.squid.kraken.v4.core.analysis.model.OrderBy;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.sql.SelectUniversal;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.ProjectAnalysisJob.Position;

public abstract class BaseRollupStrategy implements IRollupStrategy {

	private SimpleQuery query;
	private SQLSkin skin;
	private SelectUniversal select;
    private List<GroupByAxis> rollup = null;
    private boolean grandTotal = false;
	private QueryMapper mapper;
	
	
	public BaseRollupStrategy(SimpleQuery query, SQLSkin skin, SelectUniversal select, List<GroupByAxis> rollup, boolean grandTotal, QueryMapper mapper) {
		this.query = query;
		this.skin = skin;
		this.select = select;
		this.rollup = rollup;
		this.grandTotal = grandTotal;
		this.mapper = mapper;
	}

	@Override
	public String render() throws RenderingException {
		try {
			return generateScript().render();
		} catch (SQLScopeException e) {
			throw new RenderingException(e);
		}
	}
	
	public boolean hasGrandTotal() {
		return grandTotal;
	}
	
	protected SimpleQuery getQuery() {
		return query;
	}
	
	public SQLSkin getSkin() {
		return skin;
	}
	
	protected Universe getUniverse() {
		return query.getUniverse();
	}
	
	protected Domain getSubject() {
		return query.getSubject();
	}
	
	protected QueryMapper getMapper() {
		return mapper;
	}
	
	protected SelectUniversal getSelect() {
		return select;
	}
	
	protected List<GroupByAxis> getRollup() {
		return rollup != null? rollup: Collections.emptyList();
	}

	// add all
    protected void addAxis(SelectUniversal subselect) throws ScopeException, SQLScopeException {
    	addAxis(subselect, null);
    }
    
    protected void addAxis(SelectUniversal subselect, List<Axis> levels) throws ScopeException, SQLScopeException {
        for (AxisMapping ax : getMapper().getAxisMapping()) {
            Axis axis = ax.getAxis();
            if (levels==null || levels.contains(axis)) {// levels==null => add all
                // ok, add it too
                ISelectPiece piece = subselect.select(axis.getDefinition(),ax.getPiece().getAlias());
        		piece.addComment(axis.getName()+" (Dimension)");
            } else {
                // else replace the expression with a null value
                ISelectPiece piece = subselect.select(ExpressionMaker.NULL(),ax.getPiece().getAlias());
        		piece.addComment(axis.getName()+" (Dimension)");
            }
        }
    }

    /**
     * create a base selectUniversal with from, measures, conditions & filters
     * missing: select & orderby
     * @return
     * @throws SQLScopeException
     * @throws ScopeException
     */
    protected SelectUniversal createSelect() throws SQLScopeException, ScopeException {
        SelectUniversal subselect = new SelectUniversal(getUniverse());
        // from
        subselect.from(getUniverse().S(getSubject()));
        // measures
        for (MeasureMapping mx : getMapper().getMeasureMapping()) {
            ISelectPiece piece = subselect.select(mx.getMapping().getDefinition(),mx.getPiece().getAlias());
    		piece.addComment(mx.getMapping().getName()+" (Metric)");
        }
        // conditions
        for (ExpressionAST condition : getQuery().getConditions()) {
            subselect.where(condition);
        }
        // filters
        for (Filter filter : getQuery().getFilters()) {
            filter.applyFilter(subselect);
        }
        //
        return subselect;
    }
    
    /**
     * explicitly select any orderBy expression which is not already in the scope
     * 
     * T130: we want to be able to sort on that expression thought it is not selected, but the orderBy will be implemented in an enclosing Select statement.
     * @throws SQLScopeException 
     * @throws ScopeException 
     * 
     */
    protected List<OrderByMapping> inlineOrderBy(SelectUniversal subselect) throws ScopeException, SQLScopeException {
    	List<OrderByMapping> orderByMapper = new ArrayList<OrderByMapping>();
        for (OrderBy order : getQuery().getOrderBy()) {
        	SimpleMapping m = mapper.find(order.getExpression());
            if (m!=null) {
                // it's in the select we can ignore for now
            	orderByMapper.add(new OrderByMapping(m.getPiece(), order));
            } else {
            	// the order expression is not yet in the scope, add it in the select so we can access it latter
            	ISelectPiece piece = subselect.select(order.getExpression());
            	orderByMapper.add(new OrderByMapping(piece, order));
            }
        }
        return orderByMapper;
    }
	
    /**
     * check if we need to add the orderBy for that rollup levels
     * @param orderBy
     * @param totals
     * @return
     * @throws ScopeException
     * @throws ComputingException
     * @throws InterruptedException
     */
	protected boolean includeOrderBy(OrderBy orderBy, List<Axis> totals) throws ScopeException, ComputingException, InterruptedException {
		Axis axis = getUniverse().asAxis(orderBy.getExpression());
		if (axis!=null) {
			// check if the orderBy is a parent of a rollup
			for (Axis check : totals) {
				if (axis.isParentDimension(check)) {
					return true;
				}
			}
			// else
			return false;
		} else {
			return true;
		}
	}

    protected void addLevelMapping(ISelectPiece levelIDPiece) throws ScopeException {
        Axis groupingAxis = getUniverse().axis("'"+getSubject().getName()+"'.(grouping_id())").withId("GROUPING_ID");
        AxisMapping levelMapping = new AxisMapping(levelIDPiece, groupingAxis);
        getMapper().add(0,levelMapping);// add in first place
    }

    /**
     * add rollup sort inside the original orderBy statement
     * @param subselect
     * @param sql
     * @param levels
     * @return
     * @throws RenderingException
     * @throws ScopeException 
     * @throws InterruptedException 
     * @throws ComputingException 
     */
    protected List<OrderByPiece> prepareOrderBy(SelectUniversal subselect, List<ISelectPiece> levels, List<OrderByMapping> OrderByMapping) throws RenderingException, ScopeException, ComputingException, InterruptedException {
    	List<OrderByPiece> orderBy = new ArrayList<OrderByPiece>();
    	//
        Iterator<ISelectPiece> iter_levels = levels.iterator();
        if (hasGrandTotal()) {
        	ISelectPiece level_top = iter_levels.next();
        	orderBy.add(new OrderByPiece(new SelectPieceReference(null, level_top),ORDERING.DESCENT));
        }
        Iterator<GroupByAxis> iter_rollup = getRollup().iterator();
        GroupByAxis next_rollup = null;
        ISelectPiece next_level = null;
		if (iter_levels.hasNext() && iter_rollup.hasNext()) {
        	next_rollup = iter_rollup.next();
        	next_level = iter_levels.next();
        }
        Iterator<OrderByMapping> iter_mapping = OrderByMapping.iterator();
        OrderByMapping next_orderBy = null;
        if (iter_mapping.hasNext()) {
        	next_orderBy = iter_mapping.next();
        }
        while (next_rollup!=null && next_orderBy!=null) {
        	ExpressionAST expr = next_orderBy.getOrderBy().getExpression();
        	Axis axis = getUniverse().asAxis(expr);
        	if (axis==null || !axis.isParentDimension(next_rollup.getAxis())) {
        		// add the rollup
            	orderBy.add(new OrderByPiece(new SelectPieceReference(null, next_level),ordering(next_rollup.getRollupPosition())));
				if (iter_levels.hasNext() && iter_rollup.hasNext()) {
                	next_rollup = iter_rollup.next();
                	next_level = iter_levels.next();
                } else {
                	next_rollup = null;
                	next_level = null;
                }
        	} else {
        		// add the orderBy
        		SelectPieceReference alias = new SelectPieceReference(null, next_orderBy.getPiece());
        		orderBy.add(new OrderByPiece(alias, next_orderBy.getOrderBy().getOrdering()));
                if (iter_mapping.hasNext()) {
                	next_orderBy = iter_mapping.next();
                } else {
                	next_orderBy = null;
                }
        	}
        }
        // it will be one or another, not both
        while  (next_rollup!=null) {
        	orderBy.add(new OrderByPiece(new SelectPieceReference(null, next_level),ordering(next_rollup.getRollupPosition())));
			if (iter_levels.hasNext() && iter_rollup.hasNext()) {
            	next_rollup = iter_rollup.next();
            	next_level = iter_levels.next();
            } else {
            	next_rollup = null;
            	next_level = null;
            }
        } 
        while (next_orderBy!=null) {
    		SelectPieceReference alias = new SelectPieceReference(null, next_orderBy.getPiece());
    		orderBy.add(new OrderByPiece(alias, next_orderBy.getOrderBy().getOrdering()));
            if (iter_mapping.hasNext()) {
            	next_orderBy = iter_mapping.next();
            } else {
            	next_orderBy = null;
            }
        }
        //
        return orderBy;
    }
    
    protected ORDERING ordering(Position position) {
    	if (position==Position.FIRST) {
    		return ORDERING.DESCENT;
    	} else {
    		return ORDERING.ASCENT;
    	}
    }
    
    protected String renderOrderBy(List<OrderByPiece> orderBy) throws RenderingException {
    	StringBuilder sql = new StringBuilder("ORDER BY ");
    	boolean first = true;
    	for (OrderByPiece piece : orderBy) {
            if (first) {
                first = false;
            } else {
                sql.append(", ");
            }
            sql.append(piece.render(skin));
    	}
    	return sql.toString();
    }
        
	protected String renderOrderBy(SelectUniversal subselect, List<ISelectPiece> levels, List<OrderByMapping> OrderByMapping) throws RenderingException, ScopeException, ComputingException, InterruptedException {
        return renderOrderBy(prepareOrderBy(subselect, levels, OrderByMapping));
	}

}
