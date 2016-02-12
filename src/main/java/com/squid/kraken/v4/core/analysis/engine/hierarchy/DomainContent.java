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
import java.util.Collection;
import java.util.List;

import com.squid.core.database.model.Table;
import com.squid.kraken.v4.caching.awsredis.RedisCacheManager;
import com.squid.kraken.v4.caching.awsredis.generationalkeysserver.RedisKey;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.ExpressionObject;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.Relation;

/**
 * The DomainContent class handles the list of DImensions & Metrics for the Domain
 * @author sergefantino
 *
 */
public class DomainContent {
	
    private RedisKey genKey;
    
    private Table table = null;// for now the Table is used when the domain is defined by a QueryExpression
	
	private List<Dimension> dimensions = null;
	private List<Metric> metrics = null;
	
	private List<Relation> relations = null;// allow to define relations locally

	public DomainContent(Domain domain) {
		this.genKey = RedisCacheManager.getInstance().getKey("DomainContent/"+domain.getId().toUUID(), domain.getId().toUUID());
		this.dimensions = new ArrayList<>();
		this.metrics = new ArrayList<>();
	}
	
	public DomainContent(DomainContent copy) {
		this.genKey = copy.genKey;
		this.table = copy.table;
		this.dimensions = copy.dimensions;
		this.metrics = copy.metrics;
		this.relations = copy.relations;
	}

	public DomainContent(Domain domain, List<Dimension> dimensions, List<Metric> metrics) {
		this.genKey = RedisCacheManager.getInstance().getKey("DomainContent/"+domain.getId().toUUID(), domain.getId().toUUID());
		this.dimensions = dimensions;
		this.metrics = metrics;
	}
	
	public List<Dimension> getDimensions() {
		return dimensions;
	}
	
	public boolean add(Dimension dimension) {
		return dimensions.add(dimension);
	}
	
	public List<Metric> getMetrics() {
		return metrics;
	}
	
	public boolean add(Metric metric) {
		return metrics.add(metric);
	}

	public void addAll(List<ExpressionObject<?>> objects) {
		for (ExpressionObject<?> object : objects) {
			if (object instanceof Dimension) {
				add((Dimension)object);
			} else if (object instanceof Metric) {
				add((Metric)object);
			}
			// ignore others
		}
	}
	
	public void setTable(Table table) {
		this.table = table;
	}
	
	public Table getTable() {
		return table;
	}
	
	/**
	 * return a list of Relations or NULL if none
	 * @return
	 */
	public List<Relation> getRelations() {
		return relations;
	}
	
	public void add(Relation relation) {
		if (relations==null) {
			relations = new ArrayList<>();
		}
		relations.add(relation);
	}
	
	public boolean isValid() {
		return RedisCacheManager.getInstance().isValid(genKey);
	}

	public Collection<ExpressionObject<?>> getExpressionObjects() {
		List<ExpressionObject<?>> result = new ArrayList<>();
		result.addAll(dimensions);
		result.addAll(metrics);
		return result;
	}

}
