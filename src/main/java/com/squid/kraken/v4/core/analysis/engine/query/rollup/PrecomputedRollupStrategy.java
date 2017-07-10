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

import com.squid.core.domain.operators.ExtendedType;
import com.squid.core.domain.operators.Operators;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.Context;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.IPiece;
import com.squid.core.sql.render.ISelectPiece;
import com.squid.core.sql.render.OperatorPiece;
import com.squid.core.sql.render.SQLSkin;
import com.squid.core.sql.render.SimpleConstantValuePiece;
import com.squid.kraken.v4.api.core.attribute.AttributeServiceBaseImpl;
import com.squid.kraken.v4.core.analysis.engine.query.SimpleQuery;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.AxisMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.OrderByMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.QueryMapper;
import com.squid.kraken.v4.core.analysis.model.GroupByAxis;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.sql.SelectUniversal;
import com.squid.kraken.v4.core.sql.script.SQLScript;
import com.squid.kraken.v4.model.Attribute;

public class PrecomputedRollupStrategy extends BaseRollupStrategy {

	private GroupByAxis precomputedRollupAxis = null;

	public PrecomputedRollupStrategy(SimpleQuery query, SQLSkin skin, SelectUniversal select,
			List<GroupByAxis> rollup, boolean grandTotal, QueryMapper mapper) {
		super(query, skin, select, rollup, grandTotal, mapper);
		precomputedRollupAxis = query.getPrecomputedRollupAxis();
	}

	@Override
	public SQLScript generateScript() throws SQLScopeException {
		try {
			// create the main copy
			SelectUniversal main = createSelect();
			addAxis(main);// add all
			// add orderBy
			List<OrderByMapping> orderByMapping = inlineOrderBy(main);
			// add levels to order by rollup
			List<ISelectPiece> levels = new ArrayList<>();
			ISelectPiece levelIDPiece = null;
			if (precomputedRollupAxis != null) {
				List<Attribute> attributes = AttributeServiceBaseImpl.getInstance().readAll(getQuery().getUniverse().getContext(), precomputedRollupAxis.getAxis().getDimension().getId());
				if (attributes != null) {
					IPiece piece1 = main.createPiece(Context.SELECT, precomputedRollupAxis.getAxis().getDefinition());
					for (Attribute attr: attributes) {
						if (attr.getId().getAttributeId().equals("precomputedRollupLevels")) {
							int nrLevels = new Integer(attr.getExpression().getValue()).intValue();
							int offset = 0;
							if (hasGrandTotal()) {
								offset=1;
							}
							for (int level=1; level<=nrLevels; level++) {
								IPiece case_pieces[] = new IPiece[3];
								IPiece piece2 = new SimpleConstantValuePiece((level-offset), ExtendedType.INTEGER);
								case_pieces[0]  = new OperatorPiece(Operators.LESS_OR_EQUAL,new IPiece[] {piece1, piece2});
								case_pieces[1]  = new SimpleConstantValuePiece(1, ExtendedType.INTEGER);
								case_pieces[2] = new SimpleConstantValuePiece(0, ExtendedType.INTEGER);
								OperatorPiece subcase = new OperatorPiece(Operators.CASE,case_pieces);
								levels.add(main.select(subcase, "level_"+(level-offset)));
							}
						}
					}
					levelIDPiece = main.select(precomputedRollupAxis.getAxis().getDefinition(), "levelID");
				}
			}

			//
			// pretty-print
			StringBuilder sql = new StringBuilder();
			sql.append(getSkin().quoteComment("ROLLUP / Precomputed strategy")).append("\n");
			sql.append(main.render());
			// prepare the order by
			sql.append("\n");
			sql.append(renderOrderBy(main, levels, orderByMapping));
			// add the regular limit
			if (getSelect().getStatement().hasLimitValue()) {
				sql.append("\nLIMIT "+getSelect().getStatement().getLimitValue());
			}
			// add rollup metadata mapping (at the end to avoid side-effect)
			if (precomputedRollupAxis!=null) {
				AxisMapping toRemove = null;
				for (AxisMapping ax : getMapper().getAxisMapping()) {
					Axis axis = ax.getAxis();
					if (axis.equals(precomputedRollupAxis.getAxis())) {
						toRemove = ax;
					}
				}
				if (toRemove!= null) {
					getMapper().getAxisMapping().remove(toRemove);
				}
			}
			addLevelMapping(levelIDPiece);
			return new SQLScript(sql.toString(), getMapper());
		} catch (Exception e) {
			throw new SQLScopeException("cannot create a rollup statement", e);
		}
	}

	@Override
	protected void addAxis(SelectUniversal subselect) throws ScopeException, SQLScopeException {
		for (AxisMapping ax : getMapper().getAxisMapping()) {
			Axis axis = ax.getAxis();
			if (precomputedRollupAxis == null || !axis.getDimension().equals(precomputedRollupAxis.getAxis().getDimension())) {
				ISelectPiece piece = subselect.select(axis.getDefinition(), ax.getPiece().getAlias());
				piece.addComment(axis.getName()+" (Dimension)");
			}
		}
	}

}
