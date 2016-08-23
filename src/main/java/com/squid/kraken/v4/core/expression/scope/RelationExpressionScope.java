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

import java.util.HashSet; 
import java.util.List; 
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.squid.core.database.model.Column;
import com.squid.core.database.model.ForeignKey;
import com.squid.core.database.model.Table;
import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.PrettyPrintOptions;
import com.squid.kraken.v4.core.expression.reference.DomainReference;
import com.squid.kraken.v4.core.expression.reference.ParameterReference;
import com.squid.core.expression.reference.ColumnReference;
import com.squid.core.expression.reference.ForeignKeyReference;
import com.squid.core.expression.scope.DefaultScope;
import com.squid.kraken.v4.core.expression.scope.DomainExpressionScope;
import com.squid.core.expression.scope.ExpressionDiagnostic;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ExpressionScope;
import com.squid.core.expression.scope.IdentifierType;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.model.domain.DomainDomain;
import com.squid.kraken.v4.core.model.domain.ProxyDomainDomain;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Relation;

/**
 * this is the scope for the Relation object
 * 
 * Relation can only access database objects!!!
 *
 */
public class RelationExpressionScope extends DefaultScope {

	private Universe universe;
	private Domain left;
	private String leftName = null;
	private Domain right;
	private String rightName = null;
	
	private List<Relation> relationScope = null;

	public RelationExpressionScope(Universe universe, Relation relation) throws ScopeException {
		this(universe,universe.getLeft(relation),universe.getRight(relation));
		this.leftName = relation.getLeftName();
		this.rightName = relation.getRightName();
	}

	public RelationExpressionScope(Universe universe, Relation relation, List<Relation> scope) throws ScopeException {
		this(universe,universe.getLeft(relation),universe.getRight(relation));
		this.relationScope = scope;
		this.leftName = relation.getLeftName();
		this.rightName = relation.getRightName();
	}
	
	public RelationExpressionScope(Universe universe, Domain left, Domain right) throws ScopeException {
		super();
		if (left==null || right==null) {
			throw new ScopeException("Undefined relation");
		}
		this.universe = universe;
		this.left = left;
		this.right = right;
	}
	
	public RelationExpressionScope(Universe universe, Domain left, String leftName, Domain right, String rightName) throws ScopeException {
		super();
		if (left==null || right==null) {
			throw new ScopeException("Undefined relation");
		}
		this.universe = universe;
		this.left = left;
		this.leftName = leftName;
		this.right = right;
		this.rightName = rightName;
	}

	@Override
	public ExpressionScope applyExpression(ExpressionAST expression)
			throws ScopeException {
		if (expression instanceof DomainReference) {
			if (relationScope!=null) {
				return new DomainExpressionScope(universe, ((DomainReference)expression).getDomain(), DomainExpressionScope.RESTRICTED, relationScope);
			} else {
				return new DomainExpressionScope(universe, ((DomainReference)expression).getDomain(), DomainExpressionScope.RESTRICTED);
			}
		} else {
			return this;
		}
	}
	
	@Override
	public void buildDefinitionList(List<Object> definitions) {
		super.buildDefinitionList(definitions);
		//
		ExpressionAST leftAlias = null;
		ExpressionAST rightAlias = null;
		if (!left.equals(right)) {
			// add left & right domains
			definitions.add(left);
			definitions.add(right);
		} else {
			// left==right
			if (leftName==rightName) {// that also accounts for nulls
				definitions.add(leftAlias = new ParameterReference("LEFT", new ProxyDomainDomain(universe, left)));
				definitions.add(rightAlias = new ParameterReference("RIGHT", new ProxyDomainDomain(universe, right)));
			} else {
				definitions.add(leftAlias = new ParameterReference(leftName, new ProxyDomainDomain(universe, left)));
				definitions.add(rightAlias = new ParameterReference(rightName, new ProxyDomainDomain(universe, right)));
			}
		}
		//
		buildSymetricNaturalJoinDefinition(definitions,left,right,leftAlias,rightAlias);
		//
		// add the foreign keys
		try {
			Table ltable = universe.getTable(left);
			Table rtable = universe.getTable(right);
			for (ForeignKey fk : ltable.getForeignKeys()) {
				if (fk.getPrimaryTable().equals(rtable)) {
					definitions.add(fk);
				}
			}
			for (ForeignKey fk : rtable.getForeignKeys()) {
				if (fk.getPrimaryTable().equals(ltable)) {
					definitions.add(fk);
				}
			}
		} catch (ScopeException | ExecutionException e) {
			// ignore
		}
	}
	
	/**
	 * create natural join between left & right columns. Condition: must have same name and compatible types.
	 * @param definitions
	 * @param left
	 * @param right
	 * @param rightAlias 
	 * @param leftAlias 
	 */
	private void buildSymetricNaturalJoinDefinition(List<Object> definitions, Domain left, Domain right, ExpressionAST leftAlias, ExpressionAST rightAlias) {
		try {
			if (leftAlias==null) leftAlias = createReferringExpression(left);
			if (rightAlias==null) rightAlias = createReferringExpression(right);
			Table tleft = universe.getTable(left);
			Table tright = universe.getTable(right);
			Set<String> lnames = addColumnName(tleft);
			Set<String> rnames = addColumnName(tright);
			lnames.retainAll(rnames);
			for (String name : lnames) {
				Column cleft = tleft.findColumnByName(name);
				Column cright = tright.findColumnByName(name);
				if (cleft!=null && cright!=null) {
					IDomain dleft = cleft.getTypeDomain();
					IDomain dright = cright.getTypeDomain();
					if (dleft.isInstanceOf(dright) || dright.isInstanceOf(dleft)) {
						ExpressionAST eleft = ExpressionMaker.COMPOSE(leftAlias, new ColumnReference(cleft));
						ExpressionAST eright = ExpressionMaker.COMPOSE(rightAlias, new ColumnReference(cright));
						ExpressionAST equal = ExpressionMaker.EQUAL(eleft, eright);
						definitions.add(equal);
					}
				}
			}
		} catch (ScopeException | ExecutionException e1) {
			// ignore, but it's not good
		}
	}
	
	private Set<String> addColumnName(Table table) throws ExecutionException {
		Set<String> names = new HashSet<String>();
		for (Column col : table.getColumns()) {
			names.add(col.getName());
		}
		return names;
	}

	/*
	private void buildPrimaryKeyDefinitions(List<Object> definitions, Domain domain) {
		try {
			Table tleft = universe.getTable(domain);
			Index pk = tleft.getPrimaryKey();
			if (pk!=null) {
				for (Column col : pk.getColumns()) {
					definitions.add(ExpressionMaker.COMPOSE(createReferringExpression(domain), new ColumnReference(col)));
				}
			}
		} catch (ScopeException e1) {
			// ignore, but it's not good
		}
	}
	
	private void buildNaturalJoinDefinition(List<Object> definitions, Domain foreign, Domain primary) {
		ExpressionAST join = createNaturalJoinExpression(foreign, primary);
		if (join!=null) {
			definitions.add(join);
		}
	}

	private ExpressionAST createNaturalJoinExpression(Domain foreign, Domain primary) {
		try {
			ExpressionAST result = null;
			Table tforeign = universe.getTable(foreign);
			Table tprimary = universe.getTable(primary);
			Index pk = tprimary.getPrimaryKey();
			if (pk!=null) {
				for (Column col : pk.getColumns()) {
					Column fk = tforeign.findColumnByName(col.getName());
					if (fk!=null) {
						ExpressionAST ePK = ExpressionMaker.COMPOSE(createReferringExpression(primary), new ColumnReference(col));
						ExpressionAST eFK = ExpressionMaker.COMPOSE(createReferringExpression(foreign), new ColumnReference(fk));
						ExpressionAST equal = ExpressionMaker.EQUAL(ePK, eFK);
						if (result==null) {
							result = equal;
						} else {
							result = ExpressionMaker.AND(result,equal);
						}
					} else {
						return null;// can't make it
					}
				}
			}
			return result;
		} catch (ScopeException | ExecutionException e1) {
			// ignore, but it's not good
			return null;
		}
	}
	*/
	
	@Override
	public Object lookupObject(IdentifierType identifierType, String identifier) throws ScopeException {
		// this is either the source or the target... ?
		if (identifier.compareTo(left.getName())==0 || (identifierType==IdentifierType.IDENTIFIER && identifier.compareTo(left.getOid())==0) || (identifierType==IdentifierType.PARAMETER && identifier.compareTo("LEFT")==0)) {
			return left;
		} 
		if (identifier.compareTo(right.getName())==0 || (identifierType==IdentifierType.IDENTIFIER && identifier.compareTo(right.getOid())==0) || (identifierType==IdentifierType.PARAMETER && identifier.compareTo("RIGHT")==0)) {
			return right;
		}
		// check names as parameters
		if (identifierType==IdentifierType.PARAMETER) {
			if (leftName!=null && identifier.equals(leftName)) {
				return left;
			}
			if (rightName!=null && identifier.equals(rightName)) {
				return right;
			}
		}
		// check names ?
		if (leftName!=null && rightName!=null && !leftName.equals(rightName)) {// if leftName===rightName, cannot use it
			if (identifier.equals(leftName)) {
				return left;
			} else if (identifier.equals(leftName)) {
				return right;
			}
		}
		// or it could be a FK from one side to the other...
		if (identifierType==IdentifierType.DEFAULT) {
			Table tleft = universe.getTable(left);
			Table tright = universe.getTable(right);
			try {
				{
					ForeignKey fk = tleft.findForeignKeyByName(identifier);
					if (fk!=null && fk.getPrimaryTable().equals(tright)) {
						return fk;
					}
				}
				{
					ForeignKey fk = tright.findForeignKeyByName(identifier);
					if (fk!=null && fk.getPrimaryTable().equals(tleft)) {
						return fk;
					}
				}
			} catch (ExecutionException e) {
				// ignore
			}
		}
		// else
		throw new ScopeException("cannot find object '"+identifier+"' (could be: Domain, ForeignKey)");
	}
	
	@Override
	public ExpressionAST createReferringExpression(Object reference)
			throws ScopeException {
		if (reference instanceof Domain) {
			return new DomainReference(universe,(Domain)reference);
		} else if (reference instanceof ForeignKey) {
			return new ForeignKeyReference((ForeignKey)reference);
		} else {
			return super.createReferringExpression(reference);
		}
	}
	
	@Override
	public ExpressionDiagnostic validateExpression(ExpressionAST expression) {
		IDomain source = expression.getSourceDomain();
		if (left.equals(right)) {
			// special case for self join
			IDomain domain = new ProxyDomainDomain(this.universe, this.left);
			if (domain.isInstanceOf(source)) {
				// ok
			} else {
				return new ExpressionDiagnostic("This is not a valid join expression for domains");
			}
		} else {
			List<IDomain> domains = source.flatten();
			if (domains.size()==2) {
				IDomain one = domains.get(0);
				IDomain two = domains.get(1);
				IDomain left = new ProxyDomainDomain(this.universe, this.left);
				IDomain right = new ProxyDomainDomain(this.universe, this.right);
				if (( (left.isInstanceOf(one) && right.isInstanceOf(two)) 
					|| (left.isInstanceOf(two) && right.isInstanceOf(one)) ) ) {
					// ok
				} else {
					return new ExpressionDiagnostic("This is not a valid join expression between left/right domains");
				}
			} else {
				return new ExpressionDiagnostic("This is not a valid join expression between left/right domains");
			}
		}
		if (!expression.getImageDomain().isInstanceOf(IDomain.CONDITIONAL)) {
			return new ExpressionDiagnostic("This is not a valid join expression: must be a condition");
		}
		return ExpressionDiagnostic.IS_VALID;
	}

	@Override
	public String prettyPrint(ExpressionAST expression, PrettyPrintOptions options) {
		// check if it is a relation - 
		// in that case add a trailing dot to force applying the current scope
		if (expression.getImageDomain().isInstanceOf(DomainDomain.DOMAIN)) {
			return expression.prettyPrint(options)+".";
		} else {
			return expression.prettyPrint(options);
		}
	}

}
