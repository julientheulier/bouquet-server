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
package com.squid.kraken.v4.core.analysis.engine.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.database.model.Database;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.GroupingInterface.GroupingSet;
import com.squid.core.sql.db.features.IGroupingSetSupport;
import com.squid.core.sql.db.templates.SkinFactory;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.IOrderByPiece.ORDERING;
import com.squid.core.sql.render.ISelectPiece;
import com.squid.core.sql.render.OrderByPiece;
import com.squid.core.sql.render.RenderingException;
import com.squid.core.sql.render.SQLSkin;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.api.core.dimension.DimensionServiceBaseImpl;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RawRow;
import com.squid.kraken.v4.core.analysis.datamatrix.AxisValues;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.datamatrix.IndirectionRow;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.AttributeMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.ContinuousDimensionMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.DimensionMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.MeasureMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.SimpleMapping;
import com.squid.kraken.v4.core.analysis.model.IntervalleExpression;
import com.squid.kraken.v4.core.analysis.model.IntervalleObject;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.database.impl.DatabaseServiceImpl;
import com.squid.kraken.v4.core.sql.SelectUniversal;
import com.squid.kraken.v4.model.Attribute;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.persistence.AppContext;

/**
 * A custom Query to support Hierarchy computation (correlation)
 * Note that the Dimension Index is built using a different Query.
 * @author sfantino
 *
 */
public class HierarchyQuery extends BaseQuery {

	static final Logger logger = LoggerFactory
			.getLogger(HierarchyQuery.class);
	
	public enum Strategy {
		GROUPBY,
		GROUPINGSETS,
		MIXED// ticket:2994
	}
	
	private Strategy strategy = Strategy.GROUPBY;
	
	private ORDERING ordering = ORDERING.DESCENT;// legacy default to DESC
	
	private HierarchyQueryMixedStrategyHelper mixedSelect = null;// for mixed strategy

	protected List<DimensionMapping> dx_map = new ArrayList<DimensionMapping>();
	
	private float estimatedComplexity = 1;
	private int querySize = 0;

	public HierarchyQuery(Universe universe, Domain domain) throws ScopeException, SQLScopeException, DatabaseServiceException {
		super(universe,domain);
		Database database = DatabaseServiceImpl.INSTANCE.getDatabase(universe.getProject());
		SQLSkin skin = SkinFactory.INSTANCE.createSkin(database);
		if (skin.getFeatureSupport(IGroupingSetSupport.ID)==IGroupingSetSupport.IS_SUPPORTED) {
			// use mixed strategy
			this.strategy = Strategy.MIXED;
		}
		if (this.strategy==Strategy.MIXED) {
			mixedSelect = new HierarchyQueryMixedStrategyHelper(this.select);
		}
		getSelect().setSqlStyle(false, false);// T452
		getSelect().getStatement().addComment("Hierarchy Query for Domain '"+domain.getName()+"'");
	}
	
	public ORDERING getOrdering() {
        return ordering;
    }
	
	public void setOrdering(ORDERING ordering) {
        this.ordering = ordering;
    }
	
	public float getEstimatedComplexity() {
		return estimatedComplexity;
	}
		
	/**
	 * return the number of axes in the query
	 * @return
	 */
	public int getQuerySize() {
	    return querySize;
	}
	
	public SelectUniversal getSelect() {
	    if (this.strategy==Strategy.MIXED) {
	        return mixedSelect.getSelect();
	    } else {
	        return select;
	    }
	}
	
	@Override
	public String render() throws RenderingException {
	    // override to support the mixed strategy
	    // note that we do not support the rollup here
        if (this.strategy==Strategy.MIXED) {
            // if mixed, used the outer select!
            return mixedSelect.getSelect().render();
        } else {
            return super.render();
        }
	}
	
	public ContinuousDimensionMapping selectContinuous(Domain domain, DimensionIndex index) throws ScopeException, SQLScopeException {
        Axis axis = index.getAxis(); 
        Measure min = axis.getParent().M(ExpressionMaker.MIN(axis.getDefinition()));
        Measure max = axis.getParent().M(ExpressionMaker.MAX(axis.getDefinition()));
        MeasureMapping kxmin = select(min);
        kxmin.getPiece().addComment("lower bound for "+index.getDimensionName()+" (Dimension/Continuous)");
        MeasureMapping kxmax = select(max);
        kxmin.getPiece().addComment("upper bound for "+index.getDimensionName()+" (Dimension/Continuous)");
        return add(kxmin,kxmax,domain, index);
	}
	
	/**
	 * select the dimension associated to the index
	 * @param domain
	 * @param index
	 * @return
	 * @throws ScopeException
	 * @throws SQLScopeException
	 */
	public DimensionMapping select(Domain domain, DimensionIndex index) throws ScopeException, SQLScopeException {
		// update estimate
		Space dspace = getUniverse().S(domain);
		Axis axis = createAxis(dspace, index.getDimension());
		float estimate = axis.getEstimatedSize();
		if (estimate>0) estimatedComplexity = estimatedComplexity*estimate;
		querySize++;
		//
		ExpressionAST expr = getUniverse().getParser().parse(domain,index.getDimension());
		ISelectPiece piece = select.select(expr,index.getDimension().getName());
		piece.addComment(index.getDimensionName()+" (Dimension)");
        OrderByPiece orderBy = select.orderBy(piece);
        orderBy.setOrdering(getOrdering());
        //
        groupBy(index, piece);
        //
        if (this.strategy==Strategy.MIXED) {
        	piece = mixedSelect.select(piece);// mask the original piece
        	mixedSelect.groupingSets(index.getRoot(),piece);
        }
		//
		DimensionMapping dmap = new DimensionMapping(piece, domain, index);
		dx_map.add(dmap);
		return dmap;
	}

	/**
	 * select an dimension attribute
	 * @param domain
	 * @param index
	 * @param attr
	 * @return
	 * @throws ScopeException
	 * @throws SQLScopeException
	 */
	public AttributeMapping select(Domain domain, DimensionIndex index, Attribute attr) throws ScopeException, SQLScopeException {
		ExpressionAST expr = getUniverse().getParser().parse(domain,attr);
		ISelectPiece piece = select.select(expr);
		piece.addComment(index.getDimensionName()+"/"+attr.getName()+" (Attribute)");
		//
        groupBy(index, piece);
        //
        if (this.strategy==Strategy.MIXED) {
        	piece = mixedSelect.select(piece);// mask the original piece
        	mixedSelect.groupingSets(index.getRoot(),piece);
        }
        //
		AttributeMapping attrmap = new AttributeMapping(piece, attr);
		return attrmap;
	}

	// that code should move to the GrouingInterface...
	private HashMap<DimensionIndex, GroupingSet> groupingSet = new HashMap<DimensionIndex, GroupingSet>();
	
	/**
	 * manage group by: can use different strategies
	 * @param index
	 * @param piece
	 * @throws ScopeException
	 */
	protected void groupBy(DimensionIndex index, ISelectPiece piece) throws ScopeException {
        switch (this.strategy) {
        	case GROUPINGSETS:{
		        // manage the grouping-set?
		        if (index.getParent()==null) {
		        	// root, add a grouping set
		        	GroupingSet set = select.getGrouping().addGroupingSet();
		        	set.add(piece.getSelect());
		        	groupingSet.put(index, set);
		        } else {
		        	// add to the root
		        	GroupingSet set = groupingSet.get(index.getRoot());
		        	if (set==null) throw new ScopeException("Invalid hierarchy");
		        	set.add(piece.getSelect());
		        }}
	        break;
        	case GROUPBY:
        	case MIXED:
        		select.getGrouping().addGroupBy(piece.getSelect());
        		break;
        }
	}
	
	public MeasureMapping select(Measure measure) throws ScopeException, SQLScopeException {
		ExpressionAST expr = measure.getDefinition();
		ISelectPiece piece = select.select(expr);
		//
		if (this.strategy==Strategy.MIXED) {
			piece = mixedSelect.select(piece,expr);
		}
		//
		select.getScope().put(measure, expr);// register the measure in the select context
		MeasureMapping mapping = new MeasureMapping(piece, measure);
		return mapping;
	}

	public ContinuousDimensionMapping add(MeasureMapping min, MeasureMapping max, Domain domain, DimensionIndex index) {
		ContinuousDimensionMapping cx = new ContinuousDimensionMapping(domain, index, min, max);
		dx_map.add(cx);
		return cx;
	}

	private Axis createAxis(Space space, Dimension dimension) {
		if (dimension.getParentId()!=null) {
			AppContext rootUserContext = ServiceUtils.getInstance().getRootUserContext(dimension.getCustomerId());
			Dimension parent = DimensionServiceBaseImpl.getInstance().read(rootUserContext, dimension.getParentId());
			return createAxis(space, parent).A(dimension);
		} else {
			return space.A(dimension);
		}
	}

	public List<DimensionMapping> getDimensionMapping() {
	    return Collections.unmodifiableList(dx_map);
	}

    public DimensionMapping getDimensionMapping(DimensionIndex index) {
        for (DimensionMapping dm : dx_map) {
            if (dm.getDimensionIndex()==index) {
                return dm;
            }
        }
        // else
        return null;
    }
	 
	@Override	
	protected DataMatrix computeDataMatrix(Database database, RawMatrix rawMatrix) throws ScopeException{
		logger.info("compute datamatrix");
		DataMatrix matrix = new DataMatrix(database);
        matrix.setFromCache(rawMatrix.isFromCache());
        matrix.setExecutionDate(rawMatrix.getExecutionDate());
        //
//		logger.info(rawMatrix.toString());
		// init Mappings
		for (DimensionMapping m : dx_map) {
			// we must create the axis before reading the result...
			Space dspace = getUniverse().S(m.getDomain());
			Axis axis = createAxis(dspace, m.getDimensionIndex().getDimension());
			AxisValues axisData = new AxisValues(axis);
			matrix.add(axisData);
			m.setAxisData(axisData);
		}
		//metadata
		for (DimensionMapping m : dx_map) {
			if (m instanceof ContinuousDimensionMapping){
				//if(logger.isDebugEnabled()){logger.debug(("Continuous dimension"));}
				ContinuousDimensionMapping cdm = (ContinuousDimensionMapping) m;				
				int index = rawMatrix.getColNames().indexOf(cdm.getKmin().getPiece().getAlias());
				int type =  rawMatrix.getColTypes().get(index);
				cdm.getKmin().setMetadata(index, type);
				index = rawMatrix.getColNames().indexOf(cdm.getKmax().getPiece().getAlias());
				type =  rawMatrix.getColTypes().get(index);
				cdm.getKmax().setMetadata(index, type);	
			} 
			else{
				int index = rawMatrix.getColNames().indexOf(m.getPiece().getAlias());
				/*logger.info(m.toString());
				logger.info(m.getPiece().toString());
				logger.info(m.getPiece().getAlias());*/
				int type = rawMatrix.getColTypes().get(index);
				m.setMetadata(index, type);
				for (Attribute attr : m.getDimensionIndex().getAttributes()) {
					SimpleMapping s = m.getMapping(attr.getId().getAttributeId());
					index = rawMatrix.getColNames().indexOf(s.getPiece().getAlias());
					type = rawMatrix.getColTypes().get(index);
					s.setMetadata(index, type);
				}
			}
		}
		
		if(logger.isDebugEnabled()){logger.debug(("# axes " + dx_map.size() + " #columns in raw " + rawMatrix.getRows().size()));}

		int[] axesIndirection = new int[dx_map.size()];
			
		for (RawRow row : rawMatrix.getRows() ) {
			Object[] row_axis = new Object[dx_map.size()];
			int indexSrcRow = 0;
			int indexDstRow = 0;
			for (DimensionMapping m : dx_map) {
				Object unbox;
				if(m instanceof ContinuousDimensionMapping){
					Object min = row.data[indexSrcRow];
					indexSrcRow++;
					Object max=  row.data[indexSrcRow];
					indexSrcRow++;
					if (min==null && max==null) {
						unbox= null;
					} else if (min instanceof Comparable && max instanceof Comparable) {
						unbox = new IntervalleObject((Comparable<?>)min, (Comparable<?>)max);
					} else {
						unbox= new IntervalleExpression(min, max);
					}
					
				}else{
					unbox = row.data[indexSrcRow];
					indexSrcRow++;
				}
				if (unbox!=null) {
					DimensionMember member = m.getDimensionIndex().getMemberByID(unbox);
					if (!m.getDimensionIndex().getAttributes().isEmpty()) {
						for (int k=0; k<m.getDimensionIndex().getAttributes().size();k++) {
							Object aunbox = row.data[indexSrcRow];
							member.setAttribute(k,aunbox);
							indexSrcRow++;
						}
					}
					row_axis[indexDstRow] = member.getID();
				}
				axesIndirection[indexDstRow] = indexDstRow;
				indexDstRow++;
			}
			
			matrix.pushRow( new RawRow(row_axis));
		}
		matrix.setFullset(!rawMatrix.hasMoreData());
		return matrix;
	}


	
}
