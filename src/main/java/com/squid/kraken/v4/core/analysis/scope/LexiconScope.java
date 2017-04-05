package com.squid.kraken.v4.core.analysis.scope;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.database.model.Column;
import com.squid.core.database.model.Table;
import com.squid.core.expression.reference.ColumnReference;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchy;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchyManager;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.project.DynamicManager;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Metric;
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
					String prefix = "dyn_"+getSpace().getDomain().getId().toUUID()+"_dimension:";
					for (Column col : table.getColumns()) {
						ColumnReference ref = new ColumnReference(col);
						definitions.add(col);
					}
				}
			} catch (ExecutionException | ScopeException e1) {
				logger.info("could not load columns");
			}

		}



	}
}
