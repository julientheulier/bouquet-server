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
import com.squid.core.domain.associative.AssociativeDomainInformation;
import com.squid.core.domain.operators.OperatorScope;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.UndefinedExpression;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.scope.AnalysisScope;
import com.squid.kraken.v4.core.analysis.scope.MeasureExpression;
import com.squid.kraken.v4.core.model.domain.DomainDomain;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.ExpressionObject;
import com.squid.kraken.v4.model.Metric;

// Measure
public class Measure implements Property {

	private Space parent = null;
	private Metric metric;
	private String ID = "";
	private String name = null;
	
	private ExpressionAST definition = null;
	
	private OriginType originType = OriginType.USER;// default to User type
	
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
		} else {
            throw new ScopeException("Invalid expression: incompatible domain for "+definition.prettyPrint());
        }
		//
		this.metric = null;
		this.ID = (parent!=null?parent.getID()+"/":"")+definition.prettyPrint();
		setDefinition(definition);
	}
	
	private void setDefinition(ExpressionAST definition) throws ScopeException {
		// check the image domain
		IDomain image = definition.getImageDomain();
		if (!image.isInstanceOf(AggregateDomain.AGGREGATE)) {
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
			return "metric_"+definition.prettyPrint();
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
        if (metric!=null || definition==null) {
            String pp = getParent().prettyPrint();
            if (pp!="") {
                pp += ".";
            }
            if (originType==OriginType.COMPARETO) {
            	return "compareTo("+pp+"["+AnalysisScope.MEASURE.getToken()+":'"+(metric!=null?metric.getName():getName())+"'])";
            } else {
            	return pp+"["+AnalysisScope.MEASURE.getToken()+":'"+getName()+"']";
            }
        } else {
            return definition.prettyPrint();
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
		return this.getId().hashCode();
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
