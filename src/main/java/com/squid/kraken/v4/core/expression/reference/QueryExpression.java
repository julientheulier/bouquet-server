package com.squid.kraken.v4.core.expression.reference;

import java.util.ArrayList;
import java.util.List;

import com.squid.core.domain.IDomain;
import com.squid.core.domain.operators.ExtendedType;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.ExpressionLeaf;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.render.SQLSkin;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.model.domain.ProxyDomainDomain;
import com.squid.kraken.v4.model.Domain;

/**
 * The QueryExpression allows to define a Query (or view, or analysis) on a Domain.
 * It will be equivalent to defining a AnalysisJob, though it doesn't require to create a model object.
 * 
 * A QueryExpression can be used to define a new DOmain Subject, thus allowing to run complex computations
 * 
 * @author sergefantino
 *
 */
public class QueryExpression implements ExpressionLeaf {
	
	private Universe universe;
	private Domain subject;
	
	private List<ExpressionAST> filters = new ArrayList<>();
	private List<ExpressionAST> facets = new ArrayList<>();
	private List<ExpressionAST> metrics = new ArrayList<>();
	
	public QueryExpression(Universe universe, Domain subject) {
		this.universe = universe;
		this.subject = subject;
	}
	
	public DomainReference getSubject() {
		return new DomainReference(universe, subject);
	}
	
	public List<ExpressionAST> getFilters() {
		return filters;
	}
	
	public List<ExpressionAST> getFacets() {
		return facets;
	}
	
	public List<ExpressionAST> getMetrics() {
		return metrics;
	}
	
	public QueryExpression filter(ExpressionAST filter) throws ScopeException {
		if (!(filter.getImageDomain().isInstanceOf(IDomain.CONDITIONAL))) {
			throw new ScopeException("FILTER: operator expecting a Conditional expression");
		}
		filters.add(filter);
		return this;
	}
	
	public QueryExpression facet(ExpressionAST facet) throws ScopeException {
		facets.add(facet);
		return this;
	}
	
	public QueryExpression metric(ExpressionAST metric) throws ScopeException {
		metrics.add(metric);
		return this;
	}

	@Override
	public ExtendedType computeType(SQLSkin skin) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDomain getImageDomain() {
		if (facets.isEmpty() && metrics.isEmpty()) {
			// if no need to groupBy, the query is the same as the original domain
			return new ProxyDomainDomain(universe, subject);
		} else {
			// else it's a new kind of object. Maybe we will need to create a QueryDomain at some point.
			return IDomain.OBJECT;
		}
	}

	@Override
	public IDomain getSourceDomain() {
		return new ProxyDomainDomain(universe, subject);
	}

	@Override
	public String prettyPrint() {
		StringBuffer print = new StringBuffer((new DomainReference(universe, subject)).prettyPrint());
		for (ExpressionAST filter : filters) {
			print.append(" FILTER ").append(filter.prettyPrint());
		}
		for (ExpressionAST facet : facets) {
			print.append(" FILTER ").append(facet.prettyPrint());
		}
		for (ExpressionAST metric : metrics) {
			print.append(" FILTER ").append(metric.prettyPrint());
		}
		return print.toString();
	}

}
