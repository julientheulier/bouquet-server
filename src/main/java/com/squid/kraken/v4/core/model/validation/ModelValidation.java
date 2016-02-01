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
package com.squid.kraken.v4.core.model.validation;

import java.util.LinkedList;
import java.util.List;

import com.squid.core.database.model.Table;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.Attribute;

/**
 * provide a simple method to validate a project model. It will perform a deep traversal of the model and validate each expression.
 *
 */
public class ModelValidation {
	
	/**
	 * main method to perform a project validation
	 * @param universe
	 * @return
	 */
	public List<ValidationMessage> validateModel(Universe universe) {
		List<ValidationMessage> messages = new LinkedList<ValidationMessage>();
		List<Space> spaces;
		try {
			spaces = universe.S();
		} catch (ScopeException e) {
			messages.add(new ValidationMessage(e.getLocalizedMessage(), universe.getProject().getId()));
			return messages;
		}
		for (Space s : spaces) {
			System.out.println("Domain "+s.getDomain().getName());
			if (getTable(s)==null) {
				messages.add(new ValidationMessage("domain '"+s.getDomain().getName()+"' is not bound to table", s.getDomain().getId()));
			}
			try {
				List<Space> subspaces = s.S();
				for (Space ss : subspaces) {
					System.out.println("==> Relation "+ss.getRelationName()+" to Domain "+ss.getDomain().getName());
					if (parseRelation(ss)==null) {
						messages.add(new ValidationMessage("Relation '"+ss.getRelationName()+"' is not well defined", ss.getRelation().getId()));
					}
				}
				for (Axis a : s.A()) {
					System.out.println("==> Dimension "+a.getDimension().getName());
					if (parseDimension(a)==null) {
						messages.add(new ValidationMessage("Dimension '"+a.getDimension().getName()+"' is not well defined",a.getDimension().getId()));
					}
					for (Attribute attr : a.getAttributes()) {
						System.out.println("====> Attribute "+attr.getName());
						if (parseAttribute(a,attr)==null) {
							messages.add(new ValidationMessage(a.toString()+": Attribute '"+attr.getName()+"' is not well defined",attr.getId()));
						}
					}
				}
				for (Measure m : s.M()) {
					System.out.println("====> Measure "+m.getName());
					if (parseMeasure(m)==null) {
						messages.add(new ValidationMessage(s.toString()+": Measure '"+m.getName()+"' is not well defined: "+m.getMetric().getExpression().getValue(), m.getMetric().getId()));
					}
				}
			} catch (Exception e) {
				messages.add(new ValidationMessage(e.getLocalizedMessage(), s.getDomain().getId()));
			}
		}
		//
		return messages;
	}

	private ExpressionAST parseMeasure(Measure m) {
		try {
			return m.getDefinition();
		} catch (ScopeException e) {
			return null;
		}
	}

	private ExpressionAST parseAttribute(Axis a, Attribute attr) {
		try {
			return a.getParent().getUniverse().getParser().parse(a.getParent().getDomain(), attr);
		} catch (ScopeException e) {
			return null;
		}
	}

	private ExpressionAST parseDimension(Axis a) {
		try {
			return a.getDefinition();
		} catch (ScopeException e) {
			return null;
		}
	}

	private Table getTable(Space space) {
		try {
			return space.getTable();
		} catch (ScopeException e) {
			return null;
		}
	}
	
	private ExpressionAST parseRelation(Space subspace) {
		try {
			return subspace.getUniverse().getParser().parse(subspace.getRelation());
		} catch (ScopeException e) {
			return null;
		}
	}

}
