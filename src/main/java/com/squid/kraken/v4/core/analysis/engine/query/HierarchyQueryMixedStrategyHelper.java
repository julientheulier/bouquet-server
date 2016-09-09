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
package com.squid.kraken.v4.core.analysis.engine.query;

import java.util.HashMap;

import com.squid.core.domain.IDomain;
import com.squid.core.domain.operators.OperatorDefinition;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.Operator;
import com.squid.core.sql.GroupingInterface.GroupingSet;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.IPiece;
import com.squid.core.sql.render.ISelectPiece;
import com.squid.core.sql.render.OperatorPiece;
import com.squid.core.sql.render.SubSelectReferencePiece;
import com.squid.kraken.v4.core.sql.FromSelectUniversal;
import com.squid.kraken.v4.core.sql.SelectUniversal;

/**
 * ticket:2994
 * This is a helper class that ease the construction of a the SQL query for computing the hierarchy with mixed strategy.
 * Mixed strategy uses groupBy and groupingSets to reduce resultset size.
 * 
 * Example of SQL code generated:
 * 
 * SELECT
 * country,city,
 * age,
 * min(date),max(date)
 * from
 * (select country,city,age,min(date),max(date) from DATA group by country,city,age)
 * group by grouping sets ((country,city),(age))
 * 
 * Note that it won't work if the KPI is not associative!
 * 
 * @author sergefantino
 *
 */
public class HierarchyQueryMixedStrategyHelper {
	
	private SelectUniversal mixedSelect;
	private FromSelectUniversal from;
	
	private HashMap<Object, GroupingSet> groupingSets = new HashMap<Object, GroupingSet>();

	public HierarchyQueryMixedStrategyHelper(SelectUniversal inner) throws SQLScopeException {
		mixedSelect = new SelectUniversal(inner.getUniverse());
		from = mixedSelect.from(inner);// from select
	}

	public SelectUniversal getSelect() {
		return mixedSelect;
	}

	/**
	 * select the piece from the inner select in the outer select
	 * @param piece
	 * @return
	 */
	public ISelectPiece select(ISelectPiece piece) {
		SubSelectReferencePiece ref = new SubSelectReferencePiece(from, piece);
		return mixedSelect.select(ref);
	}

	/**
	 * ok, that one is tricky... we will select the piece, but may rewrite the expression to give invariant result
	 * @param piece
	 * @param expr
	 * @return
	 */ 
	public ISelectPiece select(ISelectPiece piece, ExpressionAST expr) {
		SubSelectReferencePiece ref = new SubSelectReferencePiece(from, piece);
		IDomain image = expr.getImageDomain();
		if (image.isInstanceOf(IDomain.AGGREGATE)) {
			// ok, in that case we expect the expression to be OPERATOR(x)
			if (expr instanceof Operator) {
				Operator op = (Operator)expr;
				OperatorDefinition def = op.getOperatorDefinition();
				OperatorPiece wrapper = new OperatorPiece(def, new IPiece[]{ref});
				return mixedSelect.select(wrapper);
			}
		}
		// else
		// ok, do the standard stuff
		return mixedSelect.select(ref);
	}

	/**
	 * add the piece in a groupingset identified by key
	 * @param root
	 * @param piece
	 */
	public void groupingSets(Object key, ISelectPiece piece) {
		GroupingSet set = groupingSets.get(key);
		if (set==null) {
        	set = mixedSelect.getGrouping().addGroupingSet();
        	groupingSets.put(key, set);
		}
    	set.add(piece.getSelect());
	}

}
