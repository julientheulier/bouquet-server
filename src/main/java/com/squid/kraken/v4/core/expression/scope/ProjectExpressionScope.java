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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.squid.core.database.domain.TableDomain;
import com.squid.core.database.model.Table;
import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.parser.ExpressionParserImp;
import com.squid.core.expression.parser.Token;
import com.squid.core.expression.reference.TableReference;
import com.squid.core.expression.scope.DefaultScope;
import com.squid.core.expression.scope.ExpressionDiagnostic;
import com.squid.core.expression.scope.ExpressionScope;
import com.squid.core.expression.scope.IdentifierType;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.reference.QueryExpression;
import com.squid.kraken.v4.core.expression.reference.TableDomainReference;
import com.squid.kraken.v4.core.expression.reference.DomainReference;
import com.squid.kraken.v4.core.model.domain.DomainDomain;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.ExpressionObject;

/**
 * The ProjectExpressionScope is used to parse the Domain's Subject.
 * 
 * The Domain subject can be a table reference, a domain reference, or a query expression (T821).
 * 
 * @author sergefantino
 *
 */
public class ProjectExpressionScope extends DefaultScope {
	
	private Universe universe;
	private Domain self = null;// if editing an existing Domain
	private List<Domain> scope = null;

	public ProjectExpressionScope(Universe universe) {
		this.universe = universe;
	}

	public ProjectExpressionScope(Universe universe, Domain self) {
		this.universe = universe;
		this.self = self;
	}

	public ProjectExpressionScope(Universe universe, Domain self, List<Domain> scope) {
		this.universe = universe;
		this.self = self;
		this.scope  = scope;
	}
	
	private List<Domain> getDomains() throws ScopeException {
		if (this.scope!=null) {
			return this.scope;
		} else {
			return universe.getDomains();
		}
	}

	@Override
	public ExpressionScope applyExpression(ExpressionAST expression) throws ScopeException {
		if (expression instanceof DomainReference) {
			if (scope==null) {
				return new QueryExpressionScope(universe, ((DomainReference)expression).getDomain());
			} else {
				return new QueryExpressionScope(universe, ((DomainReference)expression).getDomain(), Collections.<ExpressionObject<?>>emptyList());
			}
		} else if (expression instanceof TableDomainReference) {
			// if the reference is valid as both a Table or a Domain, decide now to cast it as a Domain because the parser is asking for a composable
			Domain domain = ((TableDomainReference)expression).getDomain();
			if (scope==null) {
				return new QueryExpressionScope(universe, domain);
			} else {
				return new QueryExpressionScope(universe, domain, Collections.<ExpressionObject<?>>emptyList());
			}
		} else {
			throw new ScopeException("cannot compose");
		}
	}

	@Override
	public Object lookupObject(IdentifierType identifierType, String identifier)
			throws ScopeException {
		//
		//lookup for table
		Table table = null;
		if (identifierType==IdentifierType.DEFAULT || identifierType==IdentifierType.TABLE) {
			try {
				table = universe.getTable(identifier);
				if (table==null) {
					throw new ScopeException("cannot lookup the table '"+identifier+"'");
				}
				if (identifierType==IdentifierType.TABLE) {
					return table;
				} else {
					// maybe it can be also resolved as a domain
				}
			} catch (ExecutionException e) {
				throw new ScopeException("cannot lookup the table '"+identifier+"'");
			}
		}
		//
		// lookup for Domain - mainly for supporting the T821
		if (identifierType==IdentifierType.DEFAULT) {
			for (Domain domain : getDomains()) {
				if (domain.getName().equals(identifier)) {
					if (self==null || !self.equals(domain)) {
						if (table!=null) {
							// if it can be resolve as both a Table or a Domain, use a TableDomainReference and decide latter
							return new TableDomainReference(table, domain);
						} else {
							return domain;
						}
					}
				}
			}
		}
		// table ?
		if (table!=null) {
			return table;
		}
		//
		// else
		return super.lookupObject(identifierType, identifier);
	}

	@Override
	public ExpressionAST createReferringExpression(Object reference)
			throws ScopeException {
		if (reference instanceof Table) {
			return new TableReference((Table)reference);
		} else if (reference instanceof Domain) {
			return new DomainReference(universe, (Domain)reference);
		} else
			return super.createReferringExpression(reference);
	}
	
	@Override
	public void buildDefinitionList(List<Object> definitions) {
		super.buildDefinitionList(definitions);
		//
		// list the domains visible to the user
		try {
			// create the spaces here so we can exclude self if defined
			for (Domain domain : getDomains()) {
				if (this.self==null || this.self!=domain) {
					definitions.add(domain);
				}
			}
		} catch (ScopeException e1) {
			// ignore
		}
		//
		// list the tables only if the user has some super powers
		if (AccessRightsUtils.getInstance().hasRole(universe.getContext(), universe.getProject(), Role.WRITE)) {
			// this will prevent anyone to be able to lit the database tables
			try {
	            List<Table> tables = universe.getTables();
	            definitions.addAll(tables);
	        } catch (ExecutionException e) {
	            // ignore
	        }
		}
	}
	
	@Override
	public ExpressionDiagnostic validateExpression(ExpressionAST expression) {
		if (expression.getImageDomain().isInstanceOf(TableDomain.DOMAIN)) {
			return ExpressionDiagnostic.IS_VALID;
		} else if (expression.getImageDomain().isInstanceOf(DomainDomain.DOMAIN)) {
				return ExpressionDiagnostic.IS_VALID;
		} else if (expression.getImageDomain().isInstanceOf(IDomain.OBJECT)) {
			// TODO: need to have a better appropriate domain
			return ExpressionDiagnostic.IS_VALID;
		} else {
			return new ExpressionDiagnostic("The expression must be a valid table or domain");
		}
	}
	
	@Override
	public ExpressionAST createCompose(ExpressionAST first, ExpressionAST second, Token operator)
			throws ScopeException {
		if (operator.kind==ExpressionParserImp.FILTER) {
			return createAnalysisExpression(first, operator).filter(second);
		} else if (operator.kind==ExpressionParserImp.FACET) {
			return createAnalysisExpression(first, operator).facet(second);
		} else if (operator.kind==ExpressionParserImp.METRIC) {
			return createAnalysisExpression(first, operator).metric (second);
		} else {
    		throw new ScopeException("composition operator '"+operator.image+"' is not supported in this scope");
		}
	}
	
	private QueryExpression createAnalysisExpression(ExpressionAST first, Token operator) throws ScopeException {
		if (first instanceof DomainReference) {
			DomainReference ref = (DomainReference)first;
			return new QueryExpression(ref);
		} else if (first instanceof TableDomainReference) {
			TableDomainReference ref = (TableDomainReference)first;
			// create a DomainReference
			DomainReference domain = new DomainReference(universe, ref.getDomain());
			domain.setTokenPosition(ref.getTokenPosition());// copy the token info
			return new QueryExpression(domain);
		} else if (first instanceof QueryExpression) {
			return (QueryExpression)first;
		} else {
			throw new ScopeException("cannot apply operator "+operator.image);
		}
	}

}
