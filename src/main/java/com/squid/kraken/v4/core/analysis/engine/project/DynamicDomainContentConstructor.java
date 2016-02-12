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
package com.squid.kraken.v4.core.analysis.engine.project;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.text.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.database.model.Column;
import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.reference.ColumnReference;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.dimension.DimensionServiceBaseImpl;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainContent;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.reference.RelationReference;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.DimensionPK;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Expression;
import com.squid.kraken.v4.model.ExpressionObject;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.ValueType;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.DimensionDAO;
import com.squid.kraken.v4.persistence.dao.MetricDAO;

public class DynamicDomainContentConstructor {

	static final Logger logger = LoggerFactory
			.getLogger(DynamicDomainContentConstructor.class);
	
	private static final DimensionDAO dimensionDAO = ((DimensionDAO) DAOFactory.getDAOFactory().getDAO(Dimension.class));
	private static final MetricDAO metricDAO = ((MetricDAO) DAOFactory.getDAOFactory().getDAO(Metric.class));

	private Universe univ;
	private Space space;
	private Domain domain;
	private String prefix;
	
	private HashSet<Column> coverage = new HashSet<Column>();// list column already available through defined dimensions
	private HashSet<ExpressionAST> metricCoverage = new HashSet<ExpressionAST>();
	private HashSet<Space> neighborhood = new HashSet<Space>();
	private HashSet<String> checkName = new HashSet<String>();
	private HashSet<String> ids = new HashSet<String>();
	private boolean isPeriodDefined = false;
	
	public DynamicDomainContentConstructor(Space space) {
		this.space = space;
		this.domain = space.getDomain();
		this.univ = space.getUniverse();
		this.prefix = "dyn_"+space.getDomain().getId().toUUID()+"_dimension:";
	}
	
	public Dimension add(Dimension dimension) {
		String name = checkName(dimension.getName(),checkName);
		if (!name.equals(dimension.getName())) {
			dimension.setName(name);
		}
		return dimension;
	}
	
	public void store(Dimension dimension) {
		DimensionServiceBaseImpl.getInstance().store(univ.getContext(), dimension);
	}

	public Dimension createDimension(RelationReference ref) {
		checkName.add(ref.getReferenceName());
		String expr = ref.prettyPrint()+".$'SELF'";// add the SELF parameter
		DimensionPK id = new DimensionPK(domain.getId(), digest(prefix+expr));
		if (!ids.contains(id.getDimensionId())) {
			String name = ref.getReferenceName();
			name = checkName(">"+name,checkName);
			Dimension dim = new Dimension(id, name, Type.INDEX, new Expression(expr), true);
			dim.setValueType(ValueType.OBJECT);
			AccessRightsUtils.getInstance().setAccessRights(univ.getContext(), dim, domain);
			return dim;
		} else {
			return null;
		}
	}
	
	/**
	 * Populate this Domain content, for both dimensions and metrics. 
	 * The Space universe is expected to be associated to the root context - this is not context specific
	 * @param space
	 * @return
	 */
	public DomainContent loadStaticContent(Space space) {
		Universe univ = space.getUniverse();
		Domain domain = space.getDomain();
		//
		// add defined top-level dimensions and record coverage
		List<Dimension> dimensions = dimensionDAO.findByDomain(univ.getContext(), domain.getId());
        List<Metric> metrics = metricDAO.findByDomain(univ.getContext(), domain.getId());
        //
        DomainContent content = new DomainContent(domain, dimensions, metrics);
		//
		return content;
	}
	
	public void initStaticContent(List<ExpressionObject<?>> concrete) {
		String prefix = "dyn_"+space.getDomain().getId().toUUID()+"_dimension:";
		//
		// evaluate the concrete objects
		ArrayList<ExpressionObject<?>> scope = new ArrayList<ExpressionObject<?>>();// T446: must define the scope incrementally and override the universe
		//
		// sort by level (0 first, ...)
		Collections.sort(concrete, new LevelComparator<ExpressionObject<?>>());
		for (ExpressionObject<?> object : concrete) {
			if (object.getName()!=null) {
				checkName.add(object.getName());
			}
        	if (object instanceof Dimension) {
        		// handle Dimension
        		Dimension dimension = (Dimension)object;
        		try {
                	if (dimension.getId()!=null) {
                		ids.add(dimension.getId().getDimensionId());
                		// add also the canonical ID
                		if (dimension.getExpression()!=null && dimension.getExpression().getValue()!=null) {
                			ids.add(digest(prefix+dimension.getExpression().getValue()));
                		}
                		// add also the Axis ID
                		ids.add(space.A(dimension).getId());
                	}
    				ExpressionAST expr = parseResilient(univ, domain, dimension, scope);
    				scope.add(object);
    				IDomain image = expr.getImageDomain();
    				dimension.setValueType(computeValueType(image));
    				if (expr instanceof ColumnReference) {
    					ColumnReference ref = (ColumnReference)expr;
    					if (ref.getColumn()!=null) {
    						coverage.add(ref.getColumn());
    					}
    				} else if (image.isInstanceOf(IDomain.OBJECT)) {
    					// it's an sub-domain, we build the space to connect and will dedup for dynamics
    					Space path = space.S(expr);
    					neighborhood.add(path);
    				}
    				if (dimension.getType()==Type.CONTINUOUS && image.isInstanceOf(IDomain.TEMPORAL)) {
    					isPeriodDefined = true;
    				}
    			} catch (ScopeException e) {
    				// invalid expression, just keep it
    				if(logger.isDebugEnabled()){logger.debug(("Invalid Dimension '"+domain.getName()+"'.'"+dimension.getName()+"' definition: "+ e.getLocalizedMessage()));}
    			}
        	} else if (object instanceof Metric) {
        		// handle Metric
        		Metric metric = (Metric)object;
        		try {
		        	if (metric.getId()!=null) {
		        		ids.add(metric.getId().getMetricId());
		        	}
		        	if (metric.getExpression() != null) {
			        	ExpressionAST expr = parseResilient(univ, domain, metric, scope);
	    				scope.add(object);
			        	metricCoverage.add(expr);
		        	}
    			} catch (ScopeException e) {
    				// invalid expression, just keep it
    				if(logger.isDebugEnabled()){logger.debug(("Invalid Metric '"+domain.getName()+"'.'"+metric.getName()+"' definition: "+ e.getLocalizedMessage()));}
    			}
        	}
        }
	}

	private ExpressionAST parseResilient(Universe root, Domain domain, Dimension dimension, Collection<ExpressionObject<?>> scope) throws ScopeException {
		try {
			return root.getParser().parse(domain, dimension, dimension.getExpression().getValue(), scope, null);
		} catch (ScopeException e) {
			if (dimension.getExpression().getInternal()!=null) {
				try {
					ExpressionAST intern = root.getParser().parse(domain, dimension, dimension.getExpression().getInternal(), scope, null);
					String value = root.getParser().rewriteExpressionIntern(dimension.getExpression().getInternal(), intern);
					dimension.getExpression().setValue(value);
					return intern;
				} catch (ScopeException e2) {
					throw e;
				}
			}
			throw e;
		}
	}
	
	private ExpressionAST parseResilient(Universe root, Domain domain, Metric metric, Collection<ExpressionObject<?>> scope) throws ScopeException {
		try {
			return root.getParser().parse(domain, metric, metric.getExpression().getValue(), scope, null);
		} catch (ScopeException e) {
			if (metric.getExpression().getInternal()!=null) {
				try {
					ExpressionAST intern = root.getParser().parse(domain, metric, metric.getExpression().getInternal(), scope, null);
					String value = root.getParser().rewriteExpressionIntern(metric.getExpression().getInternal(), intern);
					metric.getExpression().setValue(value);
					return intern;
				} catch (ScopeException e2) {
					throw e;
				}
			}
			throw e;
		}
	}
	
	class LevelComparator<X extends ExpressionObject<?>> implements Comparator<X> {

		@Override
		public int compare(X o1, X o2) {
			return Integer.compare(o1.getExpression().getLevel(), o2.getExpression().getLevel());
		}
		
	}
	
	private String checkName(String nameToCheck,
			Set<String> existingNames) {
		if (existingNames.contains(nameToCheck)) {
			String newName = nameToCheck+" (copy)";
			int index=0;
			while (existingNames.contains(newName)) {
				newName = nameToCheck+" (copy "+(++index)+")";
			}
			return newName;
		} else {
			return nameToCheck;
		}
	}

	private ValueType computeValueType(IDomain image) {
    	if (image.isInstanceOf(IDomain.STRING)) {
    		return ValueType.STRING;
    	}
    	else if (image.isInstanceOf(IDomain.NUMERIC)) {
    		return ValueType.NUMERIC;
    	}
    	else if (image.isInstanceOf(IDomain.TEMPORAL)) {
    		return ValueType.DATE;
    	}
    	else if (image.isInstanceOf(IDomain.CONDITIONAL)) {
    		return ValueType.CONDITION;
    	}
    	else if (image.isInstanceOf(IDomain.OBJECT)) {
    		return ValueType.OBJECT;
    	}
    	else return ValueType.OTHER;
    }

	private String normalizeObjectName(String name) {
		return WordUtils.capitalizeFully(name,' ','-','_').replace('-',' ').replace('_', ' ');
	}
    
    public String digest(String data) {
    	return org.apache.commons.codec.digest.DigestUtils.sha256Hex(data);
    }

}
