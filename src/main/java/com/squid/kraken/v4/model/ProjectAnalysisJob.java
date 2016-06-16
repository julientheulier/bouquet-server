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
package com.squid.kraken.v4.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.mongodb.morphia.annotations.Index;
import org.mongodb.morphia.annotations.Indexes;

import com.squid.kraken.v4.model.visitor.ModelVisitor;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

/**
 * Analysis Job which will be processed by the computation engine.<br>
 * Note the following rules :
 * <ul>
 * <li>Dimensions are optional.</li>
 * <li>If no Metric is set, then at least one Domain shall be set and all its
 * metrics will be computed.</li>
 * <li>OrderBy attribute specifies the results set sorting by specifying a list
 * of columns indexes (zero-based, dimensions first, then metrics), or Expression (for instance if the sort criteria should not be selected)</li>
 * <li>RollUp attribute specifies how to compute sub-totals for intermediate levels (of Dimension). 
 * RollUp is specified as a list of columns (dimensions only) indexes (zero-based).
 * </ul>
 */
@XmlType(namespace = "http://model.v4.kraken.squid.com")
@XmlRootElement
@SuppressWarnings("serial")
@Indexes(@Index("id.customerId, id.projectId, id.analysisJobId"))
public class ProjectAnalysisJob extends JobBaseImpl<ProjectAnalysisJobPK, DataTable> {

	private List<DomainPK> domains;

	private List<DimensionPK> dimensions;

	private List<MetricPK> metrics;

	private List<Metric> metricList;
	
	private List<FacetExpression> facets;
	
	@ApiModelProperty(value = "compute rollup on the given dimensions. It is a list of indices that references the dimension in either dimensions or facets list. In order to compute a grand-total, use id=-1 (it should be the first in the list). If several levels are defined, the analysis will compute sub-total for: (level0), then (level0,level1)... If a rollup is specified, the resulting DataTable will have a new column 'GROUPING_ID' in first position that will return 0 if the row is the grand-total, 1 for the first level sub-total, ... and null if it is not a rollup row.")
	private List<RollUp> rollups;

	private FacetSelection selection;

	private List<OrderBy> orderBy;

	private Long offset;

	private Long limit;
	
	private List<Index> beyondLimit;

	private String redisKey;
	
	private Map<String, Object> optionKeys = null;
	
	@XmlTransient
	@JsonIgnore
	public String getRedisKey() {
		return redisKey;
	}
	@XmlTransient
	@JsonIgnore
	public void setRedisKey(String redisKey) {
		this.redisKey = redisKey;
	}

	/**
	 * Default constructor (required for jaxb).
	 */
	public ProjectAnalysisJob() {
		super(null);
	}

	public ProjectAnalysisJob(ProjectAnalysisJobPK id) {
		super(id);
	}

	public FacetSelection getSelection() {
		return selection;
	}

	public void setSelection(FacetSelection selection) {
		this.selection = selection;
	}

	public List<DimensionPK> getDimensions() {
		if (dimensions == null) {
			dimensions = new ArrayList<DimensionPK>();
		}
		return dimensions;
	}

	public void setDimensions(List<DimensionPK> dimensions) {
		this.dimensions = dimensions;
	}

	/**
	 * @Deprecated use getMetricList instead
	 */
	@Deprecated
	public List<MetricPK> getMetrics() {
		if (metrics == null) {
			metrics = new ArrayList<MetricPK>();
		}
		return metrics;
	}

	/**
	 * @Deprecated use setMetricList instead
	 */
	@Deprecated
	public void setMetrics(List<MetricPK> metrics) {
		this.metrics = metrics;
		if (metrics != null) {
			metricList = new ArrayList<Metric>();
			for (MetricPK m : metrics) {
				metricList.add(new Metric(m, null, null));
			}
		}
	}

	public List<DomainPK> getDomains() {
		if (domains == null) {
			domains = new ArrayList<DomainPK>();
		}
		return domains;
	}

	public void setDomains(List<DomainPK> domains) {
		this.domains = domains;
	}

	public List<Metric> getMetricList() {
		if (metricList == null) {
			metricList = new ArrayList<Metric>();
		}
		return metricList;
	}

	public void setMetricList(List<Metric> metricList) {
		this.metricList = metricList;
	}

	public List<FacetExpression> getFacets() {
		if (facets == null) {
		    facets = new ArrayList<FacetExpression>();
		}
		return facets;
	}

	public void setFacets(List<FacetExpression> facets) {
		this.facets = facets;
	}
	
	public List<RollUp> getRollups() {
        if (rollups == null) {
            rollups = new ArrayList<RollUp>();
        }
        return rollups;
    }

    public void setRollups(List<RollUp> rollups) {
        this.rollups = rollups;
    }

    public List<OrderBy> getOrderBy() {
		return orderBy;
	}

	/**
	 * OrderBy attribute specifies the results set sorting by specifying a list
	 * of columns (dimensions or metrics) indexes (zero-based)
	 */
	public void setOrderBy(List<OrderBy> orderBy) {
		this.orderBy = orderBy;
	}

	public Long getOffset() {
		return offset;
	}

	public void setOffset(Long offset) {
		this.offset = offset;
	}

	public Long getLimit() {
		return limit;
	}

	public void setLimit(Long limit) {
		this.limit = limit;
	}
	
	public List<Index> getBeyondLimit() {
		return beyondLimit;
	}
	
	public void setBeyondLimit(List<Index> noLimit) {
		this.beyondLimit = noLimit;
	}
	
	/**
	 * @return the optionKeys
	 */
	public Map<String, Object> getOptionKeys() {
		return optionKeys;
	}
	
	/**
	 * @param optionKeys the optionKeys to set
	 */
	public void setOptionKeys(Map<String, Object> optionKeys) {
		this.optionKeys = optionKeys;
	}

	@XmlTransient
	@JsonIgnore
	public List<Domain> readDomains(AppContext ctx) {
		List<Domain> domains = new ArrayList<Domain>();
		for (DomainPK domainId : getDomains()) {
			domains.add(DAOFactory.getDAOFactory().getDAO(Domain.class)
					.readNotNull(ctx, domainId));
		}
		return domains;
	}

	@XmlTransient
	@JsonIgnore
	public List<Dimension> readDimensions(AppContext ctx) {
		List<Dimension> l = new ArrayList<Dimension>();
		for (DimensionPK oid : getDimensions()) {
			l.add(DAOFactory.getDAOFactory().getDAO(Dimension.class)
					.readNotNull(ctx, oid));
		}
		return l;
	}

	/**
	 * Visitor.
	 */
	@XmlTransient
	@JsonIgnore
	public void accept(ModelVisitor visitor) {
		visitor.visit(this);
	}

	@Override
	public ProjectAnalysisJobPK getId() {
		return super.getId();
	}

	@Override
	public Persistent<?> getParentObject(AppContext ctx) {
		return DAOFactory
				.getDAOFactory()
				.getDAO(Project.class)
				.readNotNull(ctx,
						new ProjectPK(id.getCustomerId(), id.getProjectId()));
	}

	@Override
	public String toString() {
		return "ProjectAnalysisJob [id=" + id.getAnalysisJobId() + "]";
	}
	
	@ApiModel(description="a Index allows to identify a dimension by its position in the Analysis. The dimension is defined by its indice in the analysis job, starting at 0.")
    public static class Index implements Serializable {

    	private Integer col;
        
        public Index() {
            super();
        }
        
        public Index(int col) {
            super();
            this.col = col;
        }

        @ApiModelProperty(value="the indice of the dimension.",example="0")
        public Integer getCol() {
            return col;
        }

        public void setCol(Integer col) {
            this.col = col;
        }
        
    }
	
    static public enum Direction {
        ASC, DESC
    };
	
    static public enum Position {
        FIRST, LAST
    };

    /**
     * RollUp specification
     * 
     * The col is the index of the dimension to use as rollUp (zero-based).
     * 
     * In order to compute a grandTotal, use col=-1
     * 
     * @author sergefantino
     *
     */
    @ApiModel(description="a Rollup allows to specify which dimension to use for the sub-total level. The dimension is defined by its indice in the analysis job, starting at 0. In order to compute a grand-total, use indice -1. It is also possible to define how to sort sub-total using the position, default to FIRST.")
    public static class RollUp extends Index {
    	
    	private Position position = Position.FIRST;
        
        public RollUp() {
            super();
        }

        @ApiModelProperty(value="the indice of the dimension to rollup on, or -1 to compute a grand-total.",example="0")
        public Integer getCol() {
            return super.getCol();
        }
        
        @ApiModelProperty(value="define how to sort the sub-total, either before the detailled data (FIRST) or after (LAST)",example="FIRST",allowableValues="FIRST,LAST")
		public Position getPosition() {
			return position;
		}
        
        public void setPosition(Position position) {
			this.position = position;
		}
        
    }
	
    /**
     * Define an orderBy expression
     * @author sergefantino
     *
     */
	public static class OrderBy extends Index {
	    
		private Expression expression;
		private Direction direction;
		
		public OrderBy() {
			super();
		}
		
		public OrderBy(int col, Direction direction) {
			super(col);
			this.direction = direction;
		}
		
		public OrderBy(Expression expression, Direction direction) {
			super();
			this.expression = expression;
			this.direction = direction;
		}

		/**
		 * get the orderBy expression referenced by an index
		 * @return the index (from Axis+Measure list) or null if the expression is defined by a formula
		 */
		@ApiModelProperty(value="the indice of the expression to order-by, or null if it is defined by a expression.")
		public Integer getCol() {
			return super.getCol();
		}

		@ApiModelProperty(value="the direction to order-by",example="ASC",allowableValues="ASC,DESC")
		public Direction getDirection() {
			return direction;
		}

		public void setDirection(Direction direction) {
			this.direction = direction;
		}
		
		/**
		 * get the orderBy expression defined by an Expression formula
		 * @return
		 */
		@ApiModelProperty(value="the expression to order-by if it is not defined by an indice.")
		public Expression getExpression() {
			return expression;
		}
		
		public void setExpression(Expression expression) {
			this.expression = expression;
		}
		
	}

}
