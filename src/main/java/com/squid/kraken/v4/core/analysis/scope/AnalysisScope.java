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
package com.squid.kraken.v4.core.analysis.scope;

//import com.squid.kraken.v4.core.analysis.model.universe.Axis;
//import com.squid.kraken.v4.core.analysis.model.universe.Measure;
import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.DefaultScope;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ExpressionScope;
import com.squid.core.expression.scope.IdentifierType;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Space;

public class AnalysisScope 
extends DefaultScope
{

	public static final IdentifierType DOMAIN = new IdentifierType("domain");
	public static final IdentifierType MEASURE = new IdentifierType("measure");
	public static final IdentifierType AXIS = new IdentifierType("axis");

	@Override
	public ExpressionAST createCompose(ExpressionAST first, ExpressionAST second)
			throws ScopeException {
		if (first instanceof SpaceExpression && second instanceof AxisExpression) {
			SpaceExpression sx = (SpaceExpression)first;
			AxisExpression ax = (AxisExpression)second;
			// the axis is already composed with the space through the applyExpression()
			if (sx.getSpace().getRoot().equals(ax.getAxis().getParent().getRoot())) {
				return second;
			} else {
				try {
					return new AxisExpression(sx.getSpace().A(ax.getAxis()));
				} catch (InterruptedException | ComputingException e) {
					// let it fails
				}
			}
		} else if (first instanceof AnalysisExpression && second instanceof AnalysisExpression) {
			return second;
		} else {
		    IDomain image = first.getImageDomain();
		    IDomain source = second.getSourceDomain();
		    if (image.isInstanceOf(source)) {
		        return ExpressionMaker.COMPOSE(first, second);
		    } else if (source.equals(IDomain.NULL)) {
                return ExpressionMaker.COMPOSE(first, second);
		    }
		} 
		//else
		throw new ScopeException("cannot compose expression [" + first.prettyPrint() + "] with [" + second.prettyPrint() + "]");
	}
	
	@Override
	public ExpressionScope applyExpression(ExpressionAST expression) {
		if (expression instanceof SpaceExpression) {
			SpaceExpression sx = (SpaceExpression)expression;
			return new SpaceScope(sx.getSpace());
		}
		if (expression instanceof AxisExpression) {
			AxisExpression ax = (AxisExpression)expression;
			return new AxisScope(ax.getAxis());
		}
		// else
		return null;
	}
	
	@Override
	public IdentifierType lookupIdentifierType(String token)
			throws ScopeException {
		if (token.equals(DOMAIN.getToken())) {
			return DOMAIN;
		}
		if (token.equals(MEASURE.getToken())) {
			return MEASURE;
		}
		if (token.equals(AXIS.getToken())) {
			return AXIS;
		}
		return super.lookupIdentifierType(token);
	}
	
	@Override
	public ExpressionAST createReferringExpression(Object object)
			throws ScopeException {
		if (object instanceof Space) {
			SpaceExpression expr = new SpaceExpression((Space)object);
			return expr;
		}
		if (object instanceof Measure) {
			MeasureExpression expr = new MeasureExpression((Measure)object);
			return expr;
		}
		if (object instanceof Axis) {
			AxisExpression expr = new AxisExpression((Axis)object);
			return expr;
		}
		// else
		throw new ScopeException("unknown object type");
	}
	
}
