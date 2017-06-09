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
package com.squid.kraken.v4.core.sql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.squid.core.database.model.Column;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.reference.ColumnReference;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.AnalyzerMapping;
import com.squid.core.sql.Context;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.model.Scope;
import com.squid.core.sql.render.IFromPiece;
import com.squid.core.sql.render.IPiece;
import com.squid.core.sql.render.JoinDecorator;
import com.squid.core.sql.render.WherePiece;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.model.Intervalle;

/**
 * The analyzer is the SelectUniversal companion class. 
 * 
 * It will register all the simple constraints declared in WHERE statement, in the form of COL1=CONSTANT FILTER. 
 * This is done through the addConstraint() method.
 * It also register all equi-joins in the form (COL1=COL2)&&(...); this can be either declared using an expression or a FK.
 * All that work is performed during the SelectUniversal construction.
 * 
 * Before rendering the SelectUniversal, calling the apply() method will optimize the select.
 * It will push the constraints through the equi-join, either as WHERE statement or by extending JOIN clause.
 * 
 *
 */
public class Analyzer extends AnalyzerMapping {
	
	private LinkedList<Constraint> constraints = new LinkedList<Constraint>();
	
	private SelectUniversal select = null;
	
	public Analyzer(SelectUniversal select) {
		this.select = select;
	}
	
	public void addConstraint(Column c, Collection<DimensionMember> filters) {
			constraints.add(new Constraint(c,filters));
	}
	
	public void addConstraint(Column c, Collection<DimensionMember> filters, DimensionMember interval) {
			constraints.add(new ConstraintInterval(c,filters,interval));
	}
	


	
	public void apply() {
		for (Constraint constraint : constraints) {
			if(constraint instanceof ConstraintInterval){
				apply(new LinkedList<Column>(),((ConstraintInterval)constraint).subject,((ConstraintInterval)constraint).filters, ((ConstraintInterval)constraint).interval);
			} else {
				apply(new LinkedList<Column>(),((Constraint)constraint).subject,((Constraint)constraint).filters);
			}
		}
	}
	
	private void apply(List<Column> history, Column subject, Collection<DimensionMember> filters) {
		// look for an equivalence
		List<Mapping> mappings = getMapping(subject);
		if (mappings!=null) {
			for (Mapping mapping : mappings) {
				Column equi = mapping.column;
				if (!history.contains(equi)) {
					// got one
					apply(subject,mapping,filters);
					// step into
					history.add(subject);
					apply(history,equi,filters);
				}
			}
		}
	}

	private void apply(List<Column> history, Column subject, Collection<DimensionMember> filters, DimensionMember interval) {
		// look for an equivalence
		List<Mapping> mappings = getMapping(subject);
		if (mappings!=null) {
			for (Mapping mapping : mappings) {
				Column equi = mapping.column;
				if (!history.contains(equi)) {
					// got one
					apply(subject,mapping, filters, interval);
					// step into
					history.add(subject);
					apply(history,equi, filters, interval);
				}
			}
		}
	}
	
	private void apply(Column subject, Mapping mapping,
			Collection<DimensionMember> filters) {
		// ok, this is where the though stuff takes place
		// first we have to construct a new condition expression using the equi column
		try {
			Column equi = mapping.column;
			ExpressionAST condition = createCondition(equi,filters);
			if (condition!=null) {
				// then we need to apply to the right scope...
				Scope scope = mapping.scope;
				Object x = scope.get(equi.getTable());
				if (x!=null && x instanceof IFromPiece) {
					IFromPiece from = (IFromPiece)x;
					if (from.getStatement()!=select.getStatement()) {
						if (from instanceof FromTablePieceExt) {
							FromTablePieceExt fdp = (FromTablePieceExt)from;
							fdp.getSelect().where(mapping.scope, condition);
						}
					} else {
						if (from.getDefiningJoinDecorator()!=null) {
							// add the condition to the existing join...
							JoinDecorator join = (JoinDecorator)from.getDefiningJoinDecorator();
							IPiece piece = select.createPiece(Context.WHERE, mapping.scope, condition);
							join.addCondition(new WherePiece(piece));
						} else {
							select.where(mapping.scope, condition);
						}
					}
				} else {
					select.where(mapping.scope, condition);
				}
			}
		} catch (ScopeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLScopeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void apply(Column subject, Mapping mapping,
			Collection<DimensionMember> filters, DimensionMember interval) {
		// ok, this is where the though stuff takes place
		// first we have to construct a new condition expression using the equi column
		try {
			Column equi = mapping.column;
			
			ExpressionAST condition = createCondition(equi,filters, interval);
			// then we need to apply to the right scope...
			Scope scope = mapping.scope;
			Object x = scope.get(equi.getTable());
			if (x!=null && x instanceof IFromPiece) {
				IFromPiece from = (IFromPiece)x;
				if (from.getStatement()!=select.getStatement()) {
					if (from instanceof FromTablePieceExt) {
						FromTablePieceExt fdp = (FromTablePieceExt)from;
						fdp.getSelect().where(mapping.scope, condition);
					}
				} else {
					if (from.getDefiningJoinDecorator()!=null) {
						// add the condition to the existing join...
						JoinDecorator join = (JoinDecorator)from.getDefiningJoinDecorator();
						//IPiece newPiece = select.createPiece(Context.WHERE, mapping.scope, condition);
						//join.addCondition(new WherePiece(newPiece));
						IPiece piece = select.createPiece(Context.WHERE, mapping.scope, condition);
						join.addCondition(new WherePiece(piece));
					} else {
						select.where(mapping.scope, condition);
					}
				}
			} else {
				select.where(mapping.scope, condition);
			}
		} catch (ScopeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLScopeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private ExpressionAST createCondition(Column equi,
			Collection<DimensionMember> filters) throws ScopeException {
		// cut and paste from BaseQuery... Q&D
		List<Object> filter_by_members = new ArrayList<Object>();
		ExpressionAST filter_by_intervalle = null;
		for (DimensionMember filter : filters) {
		    Object ID = filter.getID();
			if (ID instanceof Intervalle) {
				ExpressionAST where = where(new ColumnReference(equi), (Intervalle)ID);
				if (filter_by_intervalle==null) {
					filter_by_intervalle = where;
				} else if (where!=null) {
					filter_by_intervalle = ExpressionMaker.OR(filter_by_intervalle, where);
				}
			} else {
			    filter_by_members.add(ID);
			}
		}
		ExpressionAST filterALL = null;
		if (!filter_by_members.isEmpty()) {
			ExpressionAST expr = new ColumnReference(equi);
			filterALL = ExpressionMaker.IN(expr,ExpressionMaker.CONSTANTS(filter_by_members));
		}
		if (filter_by_intervalle!=null) {
			filterALL = filterALL==null?filter_by_intervalle:ExpressionMaker.AND(filterALL,filter_by_intervalle);
		}
		return filterALL;
	}
	
	private ExpressionAST createCondition(Column equi,
			Collection<DimensionMember> filters, DimensionMember interval) throws ScopeException {
		// cut and paste from BaseQuery... Q&D
		List<Object> filter_by_members = new ArrayList<Object>();
		ExpressionAST filter_by_intervalle = null;
		filters.add(interval);
		for (DimensionMember filter : filters) {
		    Object ID = filter.getID();
			if (ID instanceof Intervalle) {
				ExpressionAST where = where(new ColumnReference(equi), (Intervalle)ID);
				if (filter_by_intervalle==null) {
					filter_by_intervalle = where;
				} else if (where!=null) {
					filter_by_intervalle = ExpressionMaker.OR(filter_by_intervalle, where);
				}
			} else {
			    filter_by_members.add(ID);
			}
		}
		filters.remove(interval);
		ExpressionAST filterALL = null;
		if (!filter_by_members.isEmpty()) {
			ExpressionAST expr = new ColumnReference(equi);
			filterALL = ExpressionMaker.IN(expr,ExpressionMaker.CONSTANTS(filter_by_members));
		}
		if (filter_by_intervalle!=null) {
			filterALL = filterALL==null?filter_by_intervalle:ExpressionMaker.AND(filterALL,filter_by_intervalle);
		}
		return filterALL;
	}

	protected ExpressionAST where(ExpressionAST expr, Intervalle intervalle) throws ScopeException {
		ExpressionAST where = null;
		ExpressionAST lower = intervalle.getLowerBoundExpression();
		ExpressionAST upper = intervalle.getUpperBoundExpression();
		where = createIntervalle(expr, expr, lower, upper);
		return where!=null?ExpressionMaker.GROUP(where):null;
	}
	protected ExpressionAST createIntervalle(ExpressionAST start, ExpressionAST end, ExpressionAST lower, ExpressionAST upper) {
		if (lower!=null && upper!=null) {
			return ExpressionMaker.AND(
					ExpressionMaker.GREATER(start, lower, false),
					ExpressionMaker.LESS(end, upper, false)
					);
		} else if (lower!=null) {
			return ExpressionMaker.GREATER(start, lower, false);
		} else if (upper!=null) {
			return ExpressionMaker.LESS(end, upper, false);
		} else {
			return null;
		}
	}
	
	public Column factorDimension(ExpressionAST dimension) {
		if (dimension instanceof ColumnReference) {
			return ((ColumnReference)dimension).getColumn();
		} else {
			// dumb
			return null;
		}
	}

}
