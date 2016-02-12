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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Optional;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.ExpressionRef;
import com.squid.core.expression.parser.ParseException;
import com.squid.core.expression.parser.TokenMgrError;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.core.expression.scope.AttributeExpressionScope;
import com.squid.kraken.v4.core.expression.scope.DimensionExpressionScope;
import com.squid.core.expression.scope.ExpressionDiagnostic;
import com.squid.core.expression.scope.ExpressionScope;
import com.squid.core.expression.scope.IdentifierType;
import com.squid.kraken.v4.core.expression.scope.MetricExpressionScope;
import com.squid.kraken.v4.core.expression.scope.Parser;
import com.squid.kraken.v4.core.expression.scope.ProjectExpressionScope;
import com.squid.kraken.v4.core.expression.scope.RelationExpressionScope;
import com.squid.kraken.v4.core.expression.visitor.ExtractReferences;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainContent;
import com.squid.kraken.v4.core.analysis.universe.Property;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.Attribute;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Expression;
import com.squid.kraken.v4.model.ExpressionObject;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.ReferenceDimensionPK;
import com.squid.kraken.v4.model.ReferenceMetricPK;
import com.squid.kraken.v4.model.ReferencePK;
import com.squid.kraken.v4.model.Relation;
import com.squid.kraken.v4.persistence.DAOFactory;

/**
 * A Parser with local cache => should be used within a transaction for a given Project.
 * 
 * Since the expression cache is local to a transaction, there is no need to invalidate
 * @author sfantino
 *
 */
public class Parser {
	
	class CacheKey {
		
		public GenericPK PK;
		public String expression;
		
		public CacheKey(GenericPK pK, String expression) {
			super();
			PK = pK;
			this.expression = expression;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((PK == null) ? 0 : PK.hashCode());
			result = prime * result
					+ ((expression == null) ? 0 : expression.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			CacheKey other = (CacheKey) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (PK == null) {
				if (other.PK != null)
					return false;
			} else if (!PK.equals(other.PK))
				return false;
			if (expression == null) {
				if (other.expression != null)
					return false;
			} else if (!expression.equals(other.expression))
				return false;
			return true;
		}

		private Parser getOuterType() {
			return Parser.this;
		}
		
	}

	private Universe universe;
	
	// note: the parser cache should not last longer than a transaction - which is ok since the parser is created by the universe
	//       So we don't need to care about cache invalidation.
	private ConcurrentHashMap<CacheKey, ExpressionAST> cache = new ConcurrentHashMap<CacheKey,ExpressionAST>();
	
	public Parser(Universe universe) {
		this.universe = universe;
	}
	
	public ExpressionAST parse(GenericPK container, ExpressionScope scope, String expression) throws ScopeException {
		try {
			CacheKey key = new CacheKey(container, expression);
			ExpressionAST AST = cache.get(key);
			if (AST==null) {
				AST = scope.parseExpression(expression);
				// check if the expression is valid
				ExpressionDiagnostic check = scope.validateExpression(AST);
				if (!check.isValid()) {
					throw new ScopeException("Invalid expression: "+expression+"\n"+check.getErrorMessage());
				}
				// cache me if you can
				cache.put(key, AST);
			}
	        return AST;
		} catch (ParseException e) {
			throw new ScopeException("error while parsing expression: "+expression+"\ncaused by: "+e.getLocalizedMessage(), e);
		} catch (TokenMgrError e) {
		    throw new ScopeException("error while parsing expression: "+expression+"\ncaused by: "+e.getLocalizedMessage(), e);
		}
	}
	
	public ExpressionAST parse(Relation relation, String formula, List<Relation> scope) throws ScopeException {
		return parse(relation.getId(), new RelationExpressionScope(universe, relation, scope), formula);
	}
	
	public ExpressionAST parse(Relation relation, String formula) throws ScopeException {
		return parse(relation.getId(), new RelationExpressionScope(universe, relation), formula);
	}

	public ExpressionAST parse(Relation relation) throws ScopeException {
		try {
			return parse(relation.getId(), new RelationExpressionScope(universe,relation), relation.getJoinExpression().getValue());
		} catch (ScopeException e) {
			throw new ScopeException("error while parsing Relation '"+relation.getName()+"'\n caused by: "+e.getLocalizedMessage());
		}
	}
	
	public ExpressionAST parse(Domain domain) throws ScopeException {
		try {
			return parse(domain.getId(), new ProjectExpressionScope(universe, domain), domain.getSubject().getValue());
		} catch (ScopeException e) {
			throw new ScopeException("error while parsing Domain '"+domain.getName()+"'\n caused by: "+e.getLocalizedMessage());
		}
	}
	
	public ExpressionAST parse(Domain domain, List<Domain> scope) throws ScopeException {
		return parse(domain, domain.getSubject().getValue(), scope);
	}
	
	public ExpressionAST parse(Domain domain, String value, List<Domain> scope) throws ScopeException {
		try {
			return parse(domain.getId(), new ProjectExpressionScope(universe, domain, scope), value);
		} catch (ScopeException e) {
			throw new ScopeException("error while parsing Domain '"+domain.getName()+"'\n caused by: "+e.getLocalizedMessage());
		}
	}

	public ExpressionAST parse(Domain domain, Dimension dimension) throws ScopeException {
		return parse(dimension.getId(), new DimensionExpressionScope(universe, domain, dimension), dimension.getExpression().getValue());
	}

	public ExpressionAST parse(Domain domain, Dimension dimension, String value, DomainContent scope) throws ScopeException {
		try {
			return parse(dimension.getId(), new DimensionExpressionScope(universe, domain, dimension, scope), value);
		} catch (ScopeException e) {
			throw new ScopeException("error while parsing Dimension '"+dimension.getName()+"'\n caused by: "+e.getLocalizedMessage());
		}
	}

	public ExpressionAST parse(Domain domain, Metric metric) throws ScopeException {
		return parse(metric.getId(), new MetricExpressionScope(universe, domain, metric), metric.getExpression().getValue());
	}

	public ExpressionAST parse(Domain domain, Metric metric, String value, DomainContent scope) throws ScopeException {
		try {
			return parse(metric.getId(), new MetricExpressionScope(universe, domain, metric, scope), value);
		} catch (ScopeException e) {
			throw new ScopeException("error while parsing Metric '"+metric.getName()+"'\n caused by: "+e.getLocalizedMessage());
		}
	}

	public ExpressionAST parse(Domain domain, Attribute attr) throws ScopeException {
		try {
			return parse(attr.getId(), new AttributeExpressionScope(universe, domain), attr.getExpression().getValue());
		} catch (ScopeException e) {
			throw new ScopeException("error while parsing Attribute '"+attr.getName()+"'\n caused by: "+e.getLocalizedMessage());
		}
	}

	/**
	 * rewrite the formula as an "intern" formula with ID only and to be resilient
	 * @param formula
	 * @param expr
	 * @param references 
	 * @return
	 * @throws ScopeException
	 */
    private String rewriteExpressionValue(String formula, ExpressionAST expr, List<ExpressionRef> references) throws ScopeException {
		if (!references.isEmpty()) {
			int line = 1;
			int col = 1;
			StringBuilder output = new StringBuilder(formula.length());
			int pos = 0;
			while (pos<formula.length()) {
				char c = formula.charAt(pos);
				if (c=='\n') {
					output.append(c);
					line++;
					col = 1;
					pos++;
				} else {
					ExpressionRef ref = findReference(references,line,col);
					if (ref!=null && ref.getTokenPosition().getType()!=IdentifierType.IDENTIFIER && ref.getReferenceIdentifier()!=null) {
						String id = ref.getReferenceIdentifier();
						output.append(id);
						pos+=ref.getTokenPosition().length();
						col+=ref.getTokenPosition().length();
					} else {
						output.append(c);
						pos++;
						col++;
					}
				}
			}
			return output.toString();
		} else {
			return formula;
		}
    }

	/**
	 * rewrite the "intern" formula as a "public" formula with named references
	 * @param formula
	 * @param expr
	 * @return
	 * @throws ScopeException
	 */
    public String rewriteExpressionIntern(String formula, ExpressionAST expr) throws ScopeException {
		ExtractReferences extractor = new ExtractReferences();
		List<ExpressionRef> refs = extractor.apply(expr);
		if (!refs.isEmpty()) {
			int line = 1;
			int col = 1;
			StringBuilder output = new StringBuilder(formula.length());
			int pos = 0;
			while (pos<formula.length()) {
				char c = formula.charAt(pos);
				if (c=='\n') {
					output.append(c);
					line++;
					col = 1;
					pos++;
				} else {
					ExpressionRef ref = findReference(refs,line,col);
					if (ref!=null && ref.getTokenPosition().getType()==IdentifierType.IDENTIFIER && ref.getReferenceIdentifier()!=null) {
						String name = ref.getReferenceName();
						output.append("'"+name+"'");
						pos+=ref.getTokenPosition().length();
						col+=ref.getTokenPosition().length();
					} else {
						output.append(c);
						pos++;
						col++;
					}
				}
			}
			return output.toString();
		} else {
			return formula;
		}
    }

	private ExpressionRef findReference(List<ExpressionRef> references, int line,
			int col) {
		for (ExpressionRef ref : references) {
			if (ref.getTokenPosition()!=null) {
				if (ref.getTokenPosition().getLine()==line && ref.getTokenPosition().getStart()==col) {
					return ref;
				}
			}
		}
		// else
		return null;
	}
	
	private int computeReferenceTree(ExpressionAST expr, List<ExpressionRef> references) throws ScopeException {
		int level = 0;
		for (ExpressionRef ref : references) {
			Object reference = ref.getReference();
			if (reference!=null && reference instanceof Property) {
				Property prop = (Property)reference;
				ExpressionObject<?> x = prop.getExpressionObject();
				if (x!=null && x.getExpression()!=null) {
					int refLevel = x.getExpression().getLevel();
					if (refLevel+1>level) {
						level = refLevel+1;
					}
				}
			}
		}
		return level;
	}

	/**
	 * Analyze the formula using the given parsed expression and update the formula private fields accordingly
	 * @param expression
	 * @param expr
	 * @throws ScopeException
	 */
	public void analyzeExpression(GenericPK id, Expression formula, ExpressionAST expression) throws ScopeException {
        ExtractReferences visitor = new ExtractReferences();
		List<ExpressionRef> references = visitor.apply(expression);
        String internal = rewriteExpressionValue(formula.getValue(), expression, references);
		if (!internal.equals(formula.getValue())) {
			formula.setInternal(internal);
		} else {
			formula.setInternal(null);
		}
        int level = computeReferenceTree(expression, references);
        formula.setLevel(level);
        // compute references
        Collection<ReferencePK> IDs = new ArrayList<>();
        Collection<ExpressionObject<?>> objects = new ArrayList<>();
        for (ExpressionRef expr : references) {
			Object ref = expr.getReference();
			if (ref!=null && ref instanceof Property) {
				Property property = (Property)ref;
				ExpressionObject<?> object = property.getExpressionObject();
				if (object!=null) {
					objects.add(object);
					ReferencePK refPk = reference(object);
					if (refPk!=null) {
						IDs.add(refPk);
					}
				}
			}
        }
        if (!IDs.isEmpty()) {
        	formula.setReferences(IDs);
        	// check for cyclic dependencies
        	Collection<ExpressionObject<?>> closure = null;
    		Collection<ExpressionObject<?>> transitiveClosure = objects;
        	do {
        		closure = transitiveClosure;
        		transitiveClosure = transitiveClosure(id, closure);
        	} while (closure.size()>transitiveClosure.size());
        }
	}
	
	private Set<ExpressionObject<?>> transitiveClosure(GenericPK id, Collection<ExpressionObject<?>> references) throws ScopeException {
		HashSet<ExpressionObject<?>> transitive = new HashSet<ExpressionObject<?>>(references);
		for (ExpressionObject<?> expr : references) {
			if (expr.getId().equals(id)) {
				throw new ScopeException("Cyclic dependency found in the expression definition");
			}
			if (expr.getExpression()!=null && expr.getExpression().getReferences()!=null) {
				transitive.addAll(resolve(expr.getExpression().getReferences()));
			}
		}
		return transitive;
	}

	private Collection<? extends ExpressionObject<?>> resolve(
			Collection<ReferencePK> collection) {
		if (collection==null) {
			return Collections.emptyList();
		}
		Collection<ExpressionObject<?>> objects = new ArrayList<>();
		for (ReferencePK reference : collection) {
			Optional<? extends ExpressionObject<?>> object = resolve(reference);
			if (object.isPresent()) {
				objects.add(object.get());
			}
		}
		return objects;
	}
	
	private ReferencePK reference(ExpressionObject<? extends GenericPK> object) {
		if (object instanceof Metric) {
			return new ReferenceMetricPK(((Metric)object).getId());
		}
		if (object instanceof Dimension) {
			return new ReferenceDimensionPK(((Dimension)object).getId());
		}
		// else
		return null;
	}

	private Optional<? extends ExpressionObject<?>> resolve(ReferencePK ref) {
		if (ref instanceof ReferenceDimensionPK) {
			return DAOFactory.getDAOFactory().getDAO(Dimension.class).read(ServiceUtils.getInstance().getRootUserContext(universe.getContext()), ((ReferenceDimensionPK)ref).getReference());
		} else if (ref instanceof ReferenceMetricPK) {
			return DAOFactory.getDAOFactory().getDAO(Metric.class).read(ServiceUtils.getInstance().getRootUserContext(universe.getContext()), ((ReferenceMetricPK)ref).getReference());
		} else {
			return null;
		}
	}

}
