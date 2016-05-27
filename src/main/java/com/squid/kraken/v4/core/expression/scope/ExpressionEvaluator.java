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
package com.squid.kraken.v4.core.expression.scope;

import java.util.ArrayList; 
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.squid.core.domain.extensions.date.AddMonthsOperatorDefinition;
import com.squid.core.domain.extensions.date.operator.DateOperatorDefinition;
import com.squid.core.domain.extensions.date.DateTruncateOperatorDefinition;
import com.squid.core.domain.extensions.date.DateTruncateShortcutsOperatorDefinition;
import com.squid.core.domain.extensions.date.extract.ExtractOperatorDefinition;
import com.squid.core.domain.operators.IntrinsicOperators;
import com.squid.core.domain.operators.OperatorScope;
import com.squid.core.domain.vector.VectorOperatorDefinition;
import com.squid.core.expression.ConstantValue;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.Operator;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.expression.reference.ParameterReference;
import com.squid.kraken.v4.persistence.AppContext;

/**
 * Try to evaluate an expression
 * @author sfantino
 *
 */
public class ExpressionEvaluator {

	private HashMap<String, Object> paramValues = new HashMap<>();

	public ExpressionEvaluator(AppContext ctx) {
		//this.ctx = ctx;
	}
	
	public void setParameterValue(String param, Object value) {
		paramValues.put(param.toUpperCase(), value);
	}
	
	/**
	 * evaluate the expression into a list of constants - it supports a Vector as an input expression
	 * @param e
	 * @return
	 * @throws ScopeException 
	 */
	public List<Object> eval(ExpressionAST e) throws ScopeException {
		List<Object> results = new ArrayList<Object>();
		if (e instanceof Operator && ((Operator)e).getOperatorDefinition().equals(OperatorScope.getDefault().lookupByExtendedID(VectorOperatorDefinition.ID))) {
			Operator op = (Operator)e;
			for (ExpressionAST param : op.getArguments()) {
				results.add(eval_single(param));
			}
		} else {
			results.add(eval_single(e));
		}
		//
		return results;
	}
	
	/**
	 * evaluate the expression into a constant
	 * @param e
	 * @return
	 * @throws ScopeException
	 */
	public Object evalSingle(ExpressionAST e) throws ScopeException {
		return eval_single(e);
	}

	private Object eval_single(ExpressionAST e) throws ScopeException {
		if (e instanceof ConstantValue) {
			ConstantValue c = (ConstantValue)e;
			return c.getValue();
		} else if (e instanceof Operator) {
			Operator op = (Operator)e;
			return eval_operator(op);
		} else if (e instanceof ParameterReference) {
			return eval_parameter((ParameterReference)e);
		} else {
			return null;// this is a kind of error
		}
	}

	private Object eval_parameter(ParameterReference e) {
		// check if there is a value associated with the parameter
		if (paramValues.containsKey(e.getReferenceName().toUpperCase())) {
			return paramValues.get(e.getReferenceName().toUpperCase());
		} else {
			return null;// not set
		}
	}

	private Object eval_operator(Operator op) throws ScopeException {
		switch (op.getOperatorDefinition().getId()) {
		case IntrinsicOperators.SUBTRACTION:{
			return eval_minus(eval(op.getArguments()));
		}
		case IntrinsicOperators.PLUS:{
			return eval_plus(eval(op.getArguments()));
		}
		case IntrinsicOperators.EXTENDED_ID:{
			return eval_extended_operator(op,op.getOperatorDefinition().getExtendedID());
		}
		}
		return null;
	}

	private Object eval_minus(List<Object> eval) {
		if (eval.size()==2) {
			Object one = eval.get(0);
			Object two = eval.get(1);
			if (one instanceof Date) {
				if (two instanceof Number) {
					Calendar cal = Calendar.getInstance();
					cal.setTime((Date)one);
					cal.add(Calendar.DAY_OF_MONTH, -((Number)two).intValue());
					return new Date(cal.getTimeInMillis());
				}
			}
			if (one instanceof Number) {
				if (two instanceof Number) {
					return ((Number)one).doubleValue()-((Number)two).doubleValue();
				}
			}
		}
		// else
		return null;
	}

	private Object eval_plus(List<Object> eval) {
		if (eval.size()==2) {
			Object one = eval.get(0);
			Object two = eval.get(1);
			if (one instanceof Date) {
				if (two instanceof Number) {
					Calendar cal = Calendar.getInstance();
					cal.setTime((Date)one);
					cal.add(Calendar.DAY_OF_MONTH, ((Number)two).intValue());
					return new Date(cal.getTimeInMillis());
				}
			}
			if (one instanceof Number) {
				if (two instanceof Number) {
					return ((Number)one).doubleValue()+((Number)two).doubleValue();
				}
			}
		}
		// else
		return null;
	}

	private List<Object> eval(List<ExpressionAST> arguments) throws ScopeException {
		List<Object> results = new ArrayList<Object>();
		for (ExpressionAST param : arguments) {
			results.add(eval_single(param));
		}
		return results;
	}

	private Object eval_extended_operator(Operator op, String extendedID) throws ScopeException {
		if (extendedID.equals(DateOperatorDefinition.CURRENT_DATE)) {
			return eval_curent_date();
		} else if (extendedID.equals(AddMonthsOperatorDefinition.ADD_MONTHS)) {
			return eval_add_months(eval(op.getArguments()));
		} else if (extendedID.startsWith(ExtractOperatorDefinition.EXTRACT_BASE)) {
			return eval_extract(extendedID,eval(op.getArguments()));
		} else
		if (extendedID.equals(DateOperatorDefinition.DATE_ADD)
		  | extendedID.equals(DateOperatorDefinition.DATE_SUB)) {
			// only eval the first argument
			if (op.getArguments().size()==3) {
				List<Object> eval = eval(op.getArguments());
				Object date = eval.get(0);
				Object incr = eval.get(1);
				Object type = eval.get(2);
				if (date!=null && date instanceof Date  && incr!=null && incr instanceof Double && type!=null && type instanceof String) {
					return eval_date_add(extendedID, (Date)date, (Double)incr, (String)type);
				}
			}
		}
		// DATE TRUNCATE
		else if (extendedID.equals(DateTruncateOperatorDefinition.DATE_TRUNCATE) && op.getArguments().size()==2) {
			return eval_date_truncate(eval_single(op.getArguments().get(0)), eval(op.getArguments().get(1)));
		}
		// DATE TRUNCATE shortcuts
		else if (extendedID.toUpperCase().startsWith(DateTruncateShortcutsOperatorDefinition.SHORTCUT_BASE.toUpperCase()) && op.getArguments().size()==1) {
			String mode = extendedID.substring(DateTruncateShortcutsOperatorDefinition.SHORTCUT_BASE.length());
			return eval_date_truncate(eval_single(op.getArguments().get(0)), mode);
		}
		return null;
	}

	private Object eval_date_truncate(Object odate, Object omode) {
		if (odate!=null && omode!=null && odate instanceof Date) {
			Date date = (Date)odate;
			String mode = omode.toString();
			if (mode.equalsIgnoreCase(DateTruncateOperatorDefinition.YEAR)) {
				// return the first day of year
				Calendar cal = Calendar.getInstance();
				cal.setTime(date);
				cal.set(Calendar.MONTH, 0);
				cal.set(Calendar.DAY_OF_MONTH, 1);
				cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH), 0, 0, 0);// reset hours
				return cal.getTime();
			}
			if (mode.equalsIgnoreCase(DateTruncateOperatorDefinition.MONTH)) {
				// return the first day of month
				Calendar cal = Calendar.getInstance();
				cal.setTime(date);
				cal.set(Calendar.DAY_OF_MONTH, 1);
				cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH), 0, 0, 0);// reset hours
				return cal.getTime();
			}
			if (mode.equalsIgnoreCase(DateTruncateOperatorDefinition.WEEK)) {
				// return the first day of week
				Calendar cal = Calendar.getInstance();
				cal.setTime(date);
				cal.add(Calendar.DAY_OF_MONTH, 1-cal.get(Calendar.DAY_OF_WEEK));
				cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH), 0, 0, 0);// reset hours
				return cal.getTime();
			}
			if (mode.equalsIgnoreCase(DateTruncateOperatorDefinition.DAY)) {
				// return the day with no time info
				Calendar cal = Calendar.getInstance();
				cal.setTime(date);
				cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH), 0, 0, 0);// reset hours
				return cal.getTime();
			}
			if (mode.equalsIgnoreCase(DateTruncateOperatorDefinition.HOUR)) {
				// return the day with only hour
				Calendar cal = Calendar.getInstance();
				cal.setTime(date);
				cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY), 0, 0);
				return cal.getTime();
			}
			if (mode.equalsIgnoreCase(DateTruncateOperatorDefinition.MINUTE)) {
				// return the day with only minute
				Calendar cal = Calendar.getInstance();
				cal.setTime(date);
				cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), 0);
				return cal.getTime();
			}
			if (mode.equalsIgnoreCase(DateTruncateOperatorDefinition.SECOND)) {
				// return the day with only second
				Calendar cal = Calendar.getInstance();
				cal.setTime(date);
				cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
				return cal.getTime();
			}
		}
		// else
		return null;
	}

	private Object eval_date_add(String extendedID, Date date, Double incr,
			String type) {
		boolean add = extendedID.equals(DateOperatorDefinition.DATE_ADD);
		Calendar result = Calendar.getInstance();
		result.setTime(date);
		int calType = convertCalendarType(type);
		if (calType!=0) {
			int incrInt = incr.intValue();
			if (!add) incrInt = -incrInt;
			result.add(calType, incrInt);
			return result.getTime();
		}
		// else
		return null;
	}
	
	private int convertCalendarType(String type) {
		if (type.equalsIgnoreCase("month")) {
			return Calendar.MONTH;
		}
		if (type.equalsIgnoreCase("year")) {
			return Calendar.YEAR;
		}
		if (type.equalsIgnoreCase("day")) {
			return Calendar.DAY_OF_YEAR;
		}
		return 0;
	}

	private Object eval_extract(String extendedID, List<Object> eval) {
		if (eval.size()==1 && eval.get(0) instanceof Date) {
			Calendar cal = Calendar.getInstance();
			cal.setTime((Date)eval.get(0));
			if (extendedID.equals(ExtractOperatorDefinition.EXTRACT_DAY)) {
				return cal.get(Calendar.DAY_OF_MONTH);
			}
			if (extendedID.equals(ExtractOperatorDefinition.EXTRACT_DAY_OF_WEEK)) {
				return cal.get(Calendar.DAY_OF_WEEK);
			}
			if (extendedID.equals(ExtractOperatorDefinition.EXTRACT_DAY_OF_YEAR)) {
				return cal.get(Calendar.DAY_OF_YEAR);
			}
			if (extendedID.equals(ExtractOperatorDefinition.EXTRACT_MONTH)) {
				return cal.get(Calendar.MONTH);
			}
			if (extendedID.equals(ExtractOperatorDefinition.EXTRACT_YEAR)) {
				return cal.get(Calendar.YEAR);
			}
			if (extendedID.equals(ExtractOperatorDefinition.EXTRACT_HOUR)) {
				return cal.get(Calendar.HOUR);
			}
			if (extendedID.equals(ExtractOperatorDefinition.EXTRACT_MINUTE)) {
				return cal.get(Calendar.MINUTE);
			}
			if (extendedID.equals(ExtractOperatorDefinition.EXTRACT_SECOND)) {
				return cal.get(Calendar.SECOND);
			}
		}
		// else
		return null;
	}

	private Object eval_add_months(List<Object> eval) {
		if (eval.size()==2 && eval.get(0) instanceof Date && eval.get(1) instanceof Number) {
			Calendar result = Calendar.getInstance();
			result.setTime((Date)eval.get(0));
			result.add(Calendar.MONTH, ((Double)eval.get(1)).intValue());
			return result.getTime();
		}
		// else
		return null;
	}

	private Object eval_curent_date() {
		return new Date();
	}

}
