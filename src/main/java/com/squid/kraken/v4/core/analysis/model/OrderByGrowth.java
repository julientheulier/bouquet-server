package com.squid.kraken.v4.core.analysis.model;

import com.squid.core.expression.ExpressionAST;
import com.squid.core.sql.render.IOrderByPiece.ORDERING;
import com.squid.kraken.v4.model.Expression;

public class OrderByGrowth extends OrderBy {
	
	public Expression expr;  

	public OrderByGrowth(int pos, ExpressionAST exprAST, ORDERING ordering, Expression expression) {
		super(pos, exprAST, ordering);
		this.expr = expression;
	}

}
