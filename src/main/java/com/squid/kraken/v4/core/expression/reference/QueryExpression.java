package com.squid.kraken.v4.core.expression.reference;

import java.util.ArrayList;
import java.util.List;

import com.squid.core.domain.IDomain;
import com.squid.core.domain.operators.ExtendedType;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.NamedExpression;
import com.squid.core.expression.PrettyPrintOptions;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.render.SQLSkin;

/**
 * The QueryExpression allows to define a Query (or view, or analysis) on a Domain.
 * It will be equivalent to defining a AnalysisJob, though it doesn't require to create a model object.
 * 
 * A QueryExpression can be used to define a new Domain Subject, thus allowing to run complex computations
 * 
 * @author sergefantino
 *
 */
public class QueryExpression extends NamedExpression implements ExpressionAST {
	
	private DomainReference subject;
	
	private List<ExpressionAST> filters = new ArrayList<>();
	private List<ExpressionAST> facets = new ArrayList<>();
	private List<ExpressionAST> metrics = new ArrayList<>();
	
	public QueryExpression(DomainReference subject) {
		this.subject = subject;
	}
	
	public DomainReference getSubject() {
		return subject;
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
			return subject.getImageDomain();
		} else {
			// else it's a new kind of object. Maybe we will need to create a QueryDomain at some point.
			return IDomain.OBJECT;
		}
	}

	@Override
	public IDomain getSourceDomain() {
		return subject.getSourceDomain();
	}

	@Override
	public String prettyPrint(PrettyPrintOptions options) {
		StringBuffer print = new StringBuffer(subject.prettyPrint(options));
		for (ExpressionAST filter : filters) {
			print.append(" FILTER ").append(filter.prettyPrint(options));
		}
		for (ExpressionAST facet : facets) {
			print.append(" FACET ").append(facet.prettyPrint(options));
		}
		for (ExpressionAST metric : metrics) {
			print.append(" METRIC ").append(metric.prettyPrint(options));
		}
		return print.toString();
	}

}
