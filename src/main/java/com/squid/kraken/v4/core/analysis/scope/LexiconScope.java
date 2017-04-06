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

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.database.model.Column;
import com.squid.core.database.model.Table;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchy;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchyManager;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.MetricExt;


public class LexiconScope extends SpaceScope {

	static final Logger logger = LoggerFactory.getLogger(LexiconScope.class);

	public LexiconScope(Space space) {
		super(space);
	}

	@Override
	public void buildDefinitionList(List<Object> definitions) {
		//axis
		super.buildDefinitionList(definitions);
		// metrics
		Domain domain = space.getDomain();
		try {
			DomainHierarchy domainHierarchy = DomainHierarchyManager.INSTANCE.getHierarchy(domain.getId().getParent(), space.getDomain(), true);
			for(MetricExt me : domainHierarchy.getMetricsExt(space.getUniverse().getContext())){
				definitions.add(space.M(me));
			}
		} catch (ComputingException | InterruptedException e) {
			logger.info("Could not load metrics");
		}
		// columns
		if (!(domain.getSubject()==null || domain.getSubject().getValue()==null)) {
			Table table;
			try {
				table = space.getUniverse().getTable(space.getDomain());
				if (table!=null && space.getParent()==null) {// list columns only for the home domain
					for (Column col : table.getColumns()) {
						definitions.add(col);
					}
				}
			} catch (ExecutionException | ScopeException e1) {
				logger.info("could not load columns");
			}
		}
	}
}
