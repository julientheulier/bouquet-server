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
package com.squid.kraken.v4.core.analysis.engine.hierarchy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.model.IntervalleObject;
import com.squid.kraken.v4.core.expression.scope.DimensionDefaultValueScope;
import com.squid.kraken.v4.core.expression.scope.ExpressionEvaluator;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.model.DimensionOption;
import com.squid.kraken.v4.persistence.AppContext;

/**
 * Handle the DimensionOption logic (krkn-61)
 * @author sergefantino
 *
 */
public class DimensionOptionUtils {
	
	/**
	 * compute the DimensionOption that applies to the user context, or return null if undefined or not applies.
	 * If the context is null, merge all available options
	 * @param dimension
	 * @param ctx - the user context or null to merge all options
	 * @return
	 */
	public static DimensionOption computeContextOption(Dimension dimension, AppContext ctx) {
		List<DimensionOption> options = dimension.getOptions();
		if (dimension.getOptions()==null || dimension.getOptions().isEmpty()) {
			return null;
		}
		// iter over the options that applies and merge into a single option 
		DimensionOption merge = null;
		for (DimensionOption option : options) {
			if (ctx==null || isOptionApplies(option, ctx)) {
				if (merge==null) {
					merge = option;
				} else {
					merge = new DimensionOption(merge, option);
				}
			}
		}
		return merge;
	}

	/**
	 * check if the option applies to the user context
	 * @param ctx
	 * @param option
	 * @return
	 */
    public static boolean isOptionApplies(DimensionOption option, AppContext ctx) {
    	if (option.getGroupFilter()==null && option.getUserFilter()==null) {
    		return true;
    	} else {
	    	if (option.getGroupFilter()!=null) {
				for (String groupFilter : option.getGroupFilter()) {
					if (ctx.getUser().getGroups().contains(groupFilter)) {
						return true;
					}
				}
			}
	    	if (option.getUserFilter()!=null) {
	    		String userNamep = ctx.getUser().getLogin().toUpperCase();
				for (String userFilter : option.getGroupFilter()) {
					if (userFilter.toUpperCase().equals(userNamep)) return true;
				}
			}
	    	// else
	    	return false;
		}
    }
    
    public static List<DimensionMember> computeDefaultSelection(DimensionIndex index, DimensionOption option, AppContext ctx) throws ScopeException {
		if (option==null) {
			return Collections.emptyList();
		}
		if (option.getDefaultSelection()==null) {
			if (option.isMandatorySelection() && option.isUnmodifiableSelection()) {
				throw new ScopeException("Dimension '"+index.getDimensionName()+"' default value must be defined");
			}
		}
    	DimensionDefaultValueScope scope = new DimensionDefaultValueScope(ctx, index);
		List<Object> defaultValues = null;// to support smart error message
		try {
			ExpressionAST defaultExpression = scope.parseExpression(option.getDefaultSelection().getValue());
			ExpressionEvaluator evaluator = new ExpressionEvaluator(ctx);
			defaultValues = evaluator.eval(defaultExpression);
			if (!defaultValues.isEmpty()) {
				if (index.getDimension().getType()==Type.CONTINUOUS) {
					// create an Interval selection
					if (defaultValues.size()==2) {
						Object lower = defaultValues.get(0);
						Object upper = defaultValues.get(1);
						IntervalleObject range = IntervalleObject.createInterval(lower, upper);
						DimensionMember member = index.getMemberByID(range);
						if (member!=null) {
							return Collections.singletonList(member);
						}
					}
				} else {
					// create a category
					if (defaultValues.size()==1) {
						if (defaultValues.get(0)!=null) {
							DimensionMember member = index.getMemberByID(defaultValues.get(0));
							if (member!=null && member.getIndex()!=-1) {
								return Collections.singletonList(member);
							} else {
								// create a temporary member
								return Collections.singletonList(new DimensionMember(-1, defaultValues.get(0), index.getAttributeCount()));
							}
						}
					} else {
						ArrayList<DimensionMember> values = new ArrayList<DimensionMember>();
						for (Object value : defaultValues) {
							DimensionMember member = index.getMemberByID(value);
							if (member!=null) {
								values.add(member);
							} else {
								// create a temporary member
								values.add(new DimensionMember(-1, defaultValues.get(0), index.getAttributeCount()));
							}
						}
					}
				}
			}
		} catch (ScopeException e) {
			if (option.isMandatorySelection() || option.isUnmodifiableSelection()) {
				throw new ScopeException("Dimension '"+index.getDimensionName()+"' default value cannot be computed:\n"+e.getMessage(),e);
			}
		}
		// else
		if (option.isMandatorySelection() || option.isUnmodifiableSelection()) {
			throw new ScopeException("Dimension '"+index.getDimensionName()+"' default value cannot be resolved: "+option.getDefaultSelection().getValue()+"=>"+(defaultValues!=null?defaultValues.toString():"[]"));
		} else {
			return Collections.emptyList();
		}
    }

}
