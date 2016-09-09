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
package com.squid.kraken.v4.core.analysis.engine.processor;

import com.squid.core.domain.DomainConstant;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.extensions.cast.CastOperatorDefinition;
import com.squid.core.domain.extensions.date.AddMonthsOperatorDefinition;
import com.squid.core.domain.extensions.date.DateTruncateOperatorDefinition;
import com.squid.core.domain.extensions.date.DateTruncateShortcutsOperatorDefinition;
import com.squid.core.domain.extensions.date.operator.DateOperatorDefinition;
import com.squid.core.domain.operators.IntrinsicOperators;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.ExpressionLeaf;
import com.squid.core.expression.Operator;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.scope.AxisExpression;
import com.squid.kraken.v4.core.analysis.scope.MeasureExpression;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;

/**
 * if the expression is T(E) then it returns E, given that for any x, T(E)+x==T(E+x)
 * @author sfantino
 *
 */
public class DateExpressionAssociativeTransformationExtractor {

	public DateExpressionAssociativeTransformationExtractor() {
	}
	
	/**
	 * check if the expression are equivalent
	 * @param e
	 * @return
	 * @throws ScopeException 
	 */
	public ExpressionAST eval(ExpressionAST transformed) {
		if (transformed instanceof Operator) {
			Operator op = (Operator)transformed;
			return eval_operator(op);
		} else if (transformed instanceof ExpressionLeaf) {
			return eval_leaf((ExpressionLeaf)transformed);
		} else {
			return transformed;
		}
	}
	
	protected ExpressionAST eval_leaf(ExpressionLeaf leaf) {
		if (leaf instanceof AxisExpression) {
			Axis axis = ((AxisExpression)leaf).getAxis();
			return eval(axis.getDefinitionSafe());
		} else if (leaf instanceof MeasureExpression) {
			Measure measure = ((MeasureExpression)leaf).getMeasure();
			return eval(measure.getDefinitionSafe());
		} else {
			return leaf;
		}
	}

	private ExpressionAST eval_operator(Operator op) {
		switch (op.getOperatorDefinition().getId()) {
		case IntrinsicOperators.SUBTRACTION:
		case IntrinsicOperators.PLUS:{
			return eval_associative_op(op);
		}
		case IntrinsicOperators.EXTENDED_ID:{
			return eval_extended_operator(op, op.getOperatorDefinition().getExtendedID());
		}
		}
		return op;
	}

	private ExpressionAST eval_associative_op(Operator op) {
		if (op.getArguments().size()==2) {
			if (op.getArguments().get(1).getImageDomain().isInstanceOf(DomainConstant.DOMAIN)) {
				return eval(op.getArguments().get(0));
			} else if (op.getArguments().get(0).getImageDomain().isInstanceOf(DomainConstant.DOMAIN)) {
				return eval(op.getArguments().get(1));
			}
		}
		// else
		return op;
	}

	private ExpressionAST eval_extended_operator(Operator op, String extendedID) {
		if (extendedID.equals(CastOperatorDefinition.TO_DATE)) {
			if (op.getArguments().size()==1 && op.getArguments().get(0).getImageDomain().isInstanceOf(IDomain.TEMPORAL)) {
				return eval(op.getArguments().get(0));
			}
		} else
		if (extendedID.equals(AddMonthsOperatorDefinition.ADD_MONTHS)) {
			return eval_add_months(op);
		} else
		if (extendedID.equals(DateOperatorDefinition.DATE_ADD)
		  | extendedID.equals(DateOperatorDefinition.DATE_SUB)) {
			// only eval the first argument
			if (op.getArguments().size()==3) {
				return eval(op.getArguments().get(0));
			}
		}
		// DATE TRUNCATE
		else if (extendedID.equals(DateTruncateOperatorDefinition.DATE_TRUNCATE) && op.getArguments().size()==2) {
			return eval(op.getArguments().get(0));
		}
		// DATE TRUNCATE shortcuts
		else if (extendedID.toUpperCase().startsWith(DateTruncateShortcutsOperatorDefinition.SHORTCUT_BASE.toUpperCase()) && op.getArguments().size()==1) {
			return eval(op.getArguments().get(0));
		}
		return op;
	}

	private ExpressionAST eval_add_months(Operator op) {
		if (op.getArguments().size()==2) {
			return eval(op.getArguments().get(0));
		} else {
			return op;
		}
	}

}
