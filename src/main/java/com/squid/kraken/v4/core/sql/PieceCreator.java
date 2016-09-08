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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.database.model.Column;
import com.squid.core.database.model.Database;
import com.squid.core.database.model.ForeignKey;
import com.squid.core.database.model.KeyPair;
import com.squid.core.database.model.Table;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.aggregate.AggregateDomain;
import com.squid.core.domain.operators.ExtendedType;
import com.squid.core.domain.operators.IntrinsicOperators;
import com.squid.core.domain.operators.OperatorDefinition;
import com.squid.core.domain.operators.Operators;
import com.squid.core.expression.Compose;
import com.squid.core.expression.ConstantValue;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.NullExpression;
import com.squid.core.expression.Operator;
import com.squid.core.expression.reference.ColumnReference;
import com.squid.core.expression.reference.ForeignKeyReference;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.Context;
import com.squid.core.sql.ISelect;
import com.squid.core.sql.db.render.ColumnPiece;
import com.squid.core.sql.db.templates.SkinFactory;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.model.Scope;
import com.squid.core.sql.render.IFromPiece;
import com.squid.core.sql.render.IPiece;
import com.squid.core.sql.render.ISelectPiece;
import com.squid.core.sql.render.ITypedPiece;
import com.squid.core.sql.render.NullPiece;
import com.squid.core.sql.render.OperatorPiece;
import com.squid.core.sql.render.SQLSkin;
import com.squid.core.sql.render.SimpleConstantValuePiece;
import com.squid.kraken.v4.core.analysis.scope.AxisExpression;
import com.squid.kraken.v4.core.analysis.scope.MeasureExpression;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.database.impl.DatabaseServiceImpl;
import com.squid.kraken.v4.model.Domain;


public abstract class PieceCreator implements ISelect {
		
	private Universe universe;
	private Database database;
	private SQLSkin skin;
	
	private boolean sqlStyleReferences = true;// default is to make explicit references to the metamodel
	
	public PieceCreator(Universe universe) throws SQLScopeException {
		super();
		this.universe = universe;
		try {
			this.database = DatabaseServiceImpl.INSTANCE.getDatabase(universe.getProject());
			this.skin = SkinFactory.INSTANCE.createSkin(this.database);
		} catch (DatabaseServiceException e) {
			throw new SQLScopeException(e);
		}
	}
	
	public void setSqlStyle(boolean comments, boolean references) {
		this.skin.setComments(comments);
		this.sqlStyleReferences = references;
	}
	
	protected boolean getSqlStyleReferences() {
		return sqlStyleReferences;
	}

	public Universe getUniverse() {
		return universe;
	}
	
	/**
	 * return the database associated to the query
	 * @return
	 */
	public Database getDatabase() {
		return database;
	}
	
	/**
	 * return the skin associated to the query
	 * @return
	 */
	public SQLSkin getSkin() {
		return skin;
	}

	/**
	 * return the underlying table by parsing the Domain definition
	 * @return
	 */
	protected Table getTable(Domain domain) throws ScopeException {
		return getUniverse().getTable(domain);
	}
	
	public IPiece createPiece(Context ctx, ExpressionAST expression) throws SQLScopeException, ScopeException {
		return createPiece(ctx, getScope(), expression);
	}
	
	@Override
	public IPiece createPiece(Context ctx, Scope parent, ExpressionAST expression) throws SQLScopeException, ScopeException {
		//
		// check the binding... that enable overriding an expression
		Object binding = parent.get(expression);
		if (binding instanceof IPiece) {
			Scope scope = parent.getDefiningScope(expression);
			if (scope==parent) {
				if (binding instanceof ISelectPiece) {
					return ((ISelectPiece)binding).getSelect();
				} else{
					return (IPiece)binding;
				}
			}
		}
		//
		IDomain image = expression.getImageDomain();
		// handle aggregate expressions
		if (image.isInstanceOf(AggregateDomain.DOMAIN)) {
			getGrouping().setForceGroupBy(true);
		}
		if (expression instanceof Compose) {
			return createPieceCompose(ctx, parent, (Compose)expression);
		} else if (expression instanceof Operator) {
			return createPieceOperator(ctx, parent, (Operator)expression);
		} else {
			return createPieceLeaf(ctx, parent,(ExpressionAST)expression);
		}
	}

	protected IPiece createPieceCompose(Context ctx, Scope parent, Compose expression) throws SQLScopeException, ScopeException {
		Scope subscope = parent;
		for (ExpressionAST segment : expression.getBody()) {
			if (segment!=expression.getHead()) {
				IFromPiece from = from(ctx, subscope, segment);
				subscope = from.getScope();
			} else {
				return createPiece(ctx, subscope, segment);
			}
		}
		// we should not get there...
		throw new ScopeException("invalid expression: "+expression.toString());
	}

	private IPiece createPieceLeaf(Context ctx, Scope parent, ExpressionAST expression) throws SQLScopeException, ScopeException {
		//
		// Universe scope
		if (expression instanceof AxisExpression) {
			return createPiece(ctx, parent, ((AxisExpression)expression).getAxis().getDefinition());
		}
		if (expression instanceof MeasureExpression) {
			return createPiece(ctx, parent, ((MeasureExpression)expression).getMeasure().getDefinition());
		}
		//
		// database scope
		if (expression instanceof ColumnReference) {
			return createPiece(parent,((ColumnReference)expression).getColumn());
		}
		if (expression instanceof ForeignKeyReference) {
			return createPiece(ctx, parent,((ForeignKeyReference)expression).getForeignKey());
		}
		//
		// intrinsic scope
		if (expression instanceof NullExpression) {
			return new NullPiece();
		}
		if (expression instanceof ConstantValue) {
			ConstantValue cst = (ConstantValue)expression;
			return new SimpleConstantValuePiece(cst.getValue(),cst.getImageDomain());
		}
		// else	
		throw new SQLScopeException("expression not supported: " + expression.prettyPrint());
	}

	/**
	 * create a reference to a foreignKey
	 * @param parent
	 * @param foreignKey
	 * @return
	 * @throws SQLScopeException 
	 * @throws ScopeException 
	 */
	private IPiece createPiece(Context ctx, Scope parent, ForeignKey foreignKey) throws ScopeException, SQLScopeException {
		// create the fk expression
		List<ExpressionAST> joins = new LinkedList<ExpressionAST>();
		for (KeyPair pair : foreignKey.getKeys()) {
			joins.add(ExpressionMaker.EQUAL(new ColumnReference(pair.getPrimary()), new ColumnReference(pair.getExported())));
		}
		if (joins.size()==1) {
			return createPiece(ctx, parent,joins.get(0));
		} else if (joins.size()>1) {
			return createPiece(ctx, parent,ExpressionMaker.AND(joins));
		} else {
			throw new ScopeException("undefined foreignKey '"+foreignKey.getName()+"' from table "+foreignKey.getForeignTable()+" to table "+foreignKey.getPrimaryTable());
		}
	}

	/**
	 * Create a SQL reference to the given column
	 * @param parent
	 * @param column
	 * @return
	 */
	public IPiece createPiece(Scope parent, Column column) {
		return new ColumnPiece(parent,column);
	}

	/**
	 * create a reference to a operator (recursive)
	 * @param parent
	 * @param operator
	 * @return
	 * @throws SQLScopeException
	 * @throws ScopeException
	 */
	private IPiece createPieceOperator(Context ctx, Scope parent, Operator operator) throws SQLScopeException, ScopeException {
		// special case to handle EXISTS operator
		if (operator.getOperatorDefinition().getId()==IntrinsicOperators.EXISTS) {
			return createExistsOperator(parent,operator);
		} else {
			//
			IPiece[] pieces = new IPiece[operator.getArguments().size()];
			//IDomain[] domains = new IDomain[node.getChildCount()];
			ExtendedType[] types = new ExtendedType[operator.getArguments().size()];
			int i=0;
			for (Iterator<ExpressionAST> iter = operator.getArguments().iterator();iter.hasNext();i++) {
				ExpressionAST argument = iter.next();
				IPiece p = createPiece(ctx, parent, argument);
				if (p instanceof ITypedPiece) {
					types[i] = ((ITypedPiece)p).getType();
					// double-check
					//ExtendedType check = computeType(argument);
					//if (!types[i].equals(check)) {
					//	System.out.println("bad things happen...");
					//}
				} else {
					types[i] = argument.computeType(this.getSkin());
				}
				// ticket:1518
				if (operator.getOperatorDefinition().getPosition()!=OperatorDefinition.PREFIX_POSITION) {
					if (p instanceof OperatorPiece) {
						OperatorPiece op = (OperatorPiece)p;
						if (operator.getOperatorDefinition().getPrecedenceOrder()<=op.getOpDef().getPrecedenceOrder()) {
							p = new OperatorPiece(Operators.IDENTITY,new IPiece[]{p},new ExtendedType[]{types[i]});
						}
					}
				}
				pieces[i] = p;
			}
			return new OperatorPiece(operator.getOperatorDefinition(),pieces,types);
		}
	}

	private IPiece createExistsOperator(Scope parent, Operator operator) {
		throw new RuntimeException("EXISTS: unsupported function");
	}

}
