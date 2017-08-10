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
package com.squid.kraken.v4.core.analysis.universe;

import com.squid.core.database.domain.TableDomain;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.aggregate.AggregateDomain;
import com.squid.core.domain.analytics.AnalyticDomain;
import com.squid.core.domain.associative.AssociativeDomainInformation;
import com.squid.core.domain.operators.OperatorScope;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.PrettyPrintConstant;
import com.squid.core.expression.PrettyPrintOptions;
import com.squid.core.expression.PrettyPrintOptions.ReferenceStyle;
import com.squid.core.expression.UndefinedExpression;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.scope.AnalysisScope;
import com.squid.kraken.v4.core.analysis.scope.MeasureExpression;
import com.squid.kraken.v4.core.model.domain.DomainDomain;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.ExpressionObject;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.LzPersistentBaseImpl;
import com.squid.kraken.v4.model.Metric;

// Measure
public class Measure implements Property {

	private Space parent = null;
	private Metric metric;
	private String ID = "";
	private String name = null;

	private ExpressionAST definition = null;

	private OriginType originType = OriginType.USER;// default to User type

	private String description = null;
	private String format = null;

	public Measure(Measure copy) {
		this.parent = copy.parent;
		this.metric = copy.metric;
		this.ID = copy.ID;
		this.name = copy.name;
		this.definition = copy.definition;
	}

	public Measure(Space parent, String metricName) throws ScopeException {
		this.parent = parent;
		this.metric = lookup(parent.getDomain(), metricName);
		this.ID = (parent!=null?parent.getID()+"/":"")+this.metric.getId().toUUID();
	}

	public Measure(Space parent, ExpressionAST definition) throws ScopeException {
		this(parent, definition,  (parent!=null?parent.getID()+"/":"")+definition.prettyPrint());
	}

	public Measure(Space parent, ExpressionAST definition, String id) throws ScopeException {
		IDomain source = definition.getSourceDomain();
		this.parent = parent;
		if (source.isInstanceOf(DomainDomain.DOMAIN)) {
			DomainDomain domain = (DomainDomain)source;
			if (!this.parent.getDomain().equals(domain.getDomain())) {
				throw new ScopeException("Invalid expression: incompatible domain for "+definition.prettyPrint());
			}
		} else if (source.isInstanceOf(TableDomain.DOMAIN)) {
			TableDomain domain = (TableDomain)source;
			if (!this.parent.getTable().equals(domain.getTable())) {
				throw new ScopeException("Invalid expression: incompatible domain for "+definition.prettyPrint());
			}
		} else if (source.equals(IDomain.NULL)) {
			// count() ?
		} else {
			throw new ScopeException("Invalid expression: incompatible domain for "+definition.prettyPrint());
		}
		//
		this.metric = null;
		this.ID = id;
		setDefinition(definition);
	}

	public void setDefinition(ExpressionAST definition) throws ScopeException {
		// check the image domain
		IDomain image = definition.getImageDomain();
		if (image.isInstanceOf(AnalyticDomain.DOMAIN)) {
			// just keep it like that... cannot apply SUM anyway
		} else if (!image.isInstanceOf(AggregateDomain.AGGREGATE)) {
			// if it's numeric domain, SUM it
			if (image.isInstanceOf(IDomain.NUMERIC)) {
				definition = ExpressionMaker.SUM(definition);
			} else {
				throw new ScopeException("Invalid expression: incompatible type for "+definition.prettyPrint());
			}
		}
		this.definition = definition;
	}

	protected Measure(Space parent, Metric metric) {
		this.parent = parent;
		this.metric = metric;
		this.ID = (parent!=null?parent.getID()+"/":"")+this.metric.getId().toUUID();
	}

	@Override
	public OriginType getOriginType() {
		return originType;
	}

	public void setOriginType(OriginType originType) {
		this.originType = originType;
	}

	public String getId() {
		return parent.getDomain().getId().toUUID()+"/"+ID;
	}

	@Override
	public Space getParent() {
		return parent;
	}

	/**
	 * return the metric or null if it was defined directly through an expression
	 * @return
	 */
	public Metric getMetric() {
		return this.metric;
	}

	@Override
	public ExpressionObject<?> getExpressionObject() {
		return metric;
	}

	public String getName() {
		if (name!=null) {
			return name;
		} else if (metric!=null) {
			return metric.getName();
		} else {
			return definition.prettyPrint();
		}
	}

	/**
	 * override the standard name
	 * @param name
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * set this measure name
	 * @param name
	 * @return
	 */
	public Measure withName(String name) {
		this.name = name;
		return this;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.core.analysis.universe.Property#getDescription()
	 */
	@Override
	public String getDescription() {
		return this.description!=null?this.description:(this.metric!=null?this.metric.getDescription():null);
	}

	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.core.analysis.universe.Property#getFormat()
	 */
	@Override
	public String getFormat() {
		return this.format;
	}

	/**
	 * @param format the format to set
	 */
	public void setFormat(String format) {
		this.format = format;
	}

	@Override
	public ExpressionAST getDefinition() throws ScopeException {
		if (this.definition==null) {
			ExpressionAST measure = getParent().getUniverse().getParser().parse(getParent().getDomain(), metric);
			setDefinition(parent.compose(measure));
		}
		return this.definition;
	}

	/**
	 * return the measure definition. If the definition is invalid, return an UndefinedExpression wrapping the offending expression
	 * @return
	 */
	@Override
	public ExpressionAST getDefinitionSafe() {
		try {
			return getDefinition();
		} catch (ScopeException e) {
			return new UndefinedExpression(metric!=null?metric.getExpression().getValue():"");
		}
	}

	private Metric lookup(Domain domain, String metricName) throws ScopeException {
		for (Metric m : parent.getUniverse().getMetrics(domain)) {
			if (m.getName().equals(metricName)) {
				return m;
			}
		}
		// else
		throw new ScopeException("metric not found: "+metricName);
	}

	/**
	 * return a V4 compatible expression
	 * @return
	 */
	public String prettyPrint() {
		return prettyPrint(null);
	}

	/**
	 * return a V4 compatible expression attached to the provided scope: that is the expression could be parsed in this scope
	 * @scope the parent scope or null for Universe
	 * @return
	 */
	public String prettyPrint(PrettyPrintOptions options) {
		if (metric!=null || definition==null) {
			String pp = getParent().prettyPrint(options);
			if (pp!="") {
				pp += ".";
			}
			String print_measure = prettyPrintObject(metric, options);
			if (originType==OriginType.COMPARETO) {
				return "compareTo("+pp+print_measure+")";
			} else if (originType==OriginType.GROWTH) {
				return "growth("+pp+print_measure+")";
			} else {
				return pp+print_measure;
			}
		} else {
			return definition.prettyPrint();
		}
	}

	/**
	 * utility method to pretty-print a object id
	 * @param object
	 * @param options
	 * @return
	 */
	protected static String prettyPrintObject(LzPersistentBaseImpl<? extends GenericPK> object, PrettyPrintOptions options) {
		if (object==null) return "{undefined metric}";
		if (options==null || options.getStyle()==ReferenceStyle.LEGACY) {
			return "["+AnalysisScope.MEASURE.getToken()+":'"+object.getName()+"']";
		} else if (options!=null && options.getStyle()==ReferenceStyle.NAME) {
			return PrettyPrintConstant.OPEN_IDENT
					+object.getName()
					+PrettyPrintConstant.CLOSE_IDENT;
		} else {// krkn-84: default is ID
			return PrettyPrintConstant.IDENTIFIER_TAG
					+PrettyPrintConstant.OPEN_IDENT
					+object.getOid()
					+PrettyPrintConstant.CLOSE_IDENT;
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (this==obj) {
			return true;
		}
		else if (obj instanceof Measure) {
			Measure measure = (Measure)obj;
			return this.getId().equals(measure.getId());
		}
		else
			return false;
	}

	@Override
	public int hashCode() {
		if (originType.equals(OriginType.COMPARETO) || originType.equals(OriginType.GROWTH)) {
			return (this.getId() + originType.toString()).hashCode();
		} else {
			return this.getId().hashCode();
		}
	}

	@Override
	public String toString() {
		return getName();
	}

	public boolean isAssociative() {
		IDomain image = getDefinitionSafe().getImageDomain();
		return AssociativeDomainInformation.isAssociative(image);
	}

	@Override
	public ExpressionAST getReference() {
		if (originType==OriginType.COMPARETO) {
			return ExpressionMaker.op(OperatorScope.getDefault().lookupByExtendedID("ext.compareTo.apply"), new MeasureExpression(this));
		} else {
			return new MeasureExpression(this);
		}
	}

}
