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
package com.squid.kraken.v4.core.analysis.engine.query.mapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.expression.ExpressionAST;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;

/**
 * Implements mapping the high level object (Dimension, Attribute, KPI, Axis...) with low level objects (IPiece)
 * @author sfantino
 *
 */
public class QueryMapper {
	
	static final Logger logger = LoggerFactory.getLogger(QueryMapper.class);
	
	private List<MeasureMapping> kx_map = new ArrayList<MeasureMapping>();
	private List<AxisMapping> ax_map = new ArrayList<AxisMapping>();
	private HashMap<ExpressionAST, SimpleMapping> reverse_map = new HashMap<ExpressionAST, SimpleMapping>();
	
	public QueryMapper() {
		// TODO Auto-generated constructor stub
	}

	public QueryMapper(QueryMapper copy) {
		this.kx_map = new ArrayList<>(copy.kx_map);
		this.ax_map = new ArrayList<>(copy.ax_map);
		this.reverse_map = new HashMap<>(copy.reverse_map);
	}
	
	public void add(AxisMapping axis) {
	    ax_map.add(axis);
	    reverse_map.put(axis.getAxis().getReference(), axis);
	}
    
    public void add(int index, AxisMapping axis) {
        ax_map.add(index,axis);
        reverse_map.put(axis.getAxis().getReference(), axis);
    }
	
    public void add(MeasureMapping measure) {
        kx_map.add(measure);
        reverse_map.put(measure.getMapping().getReference(), measure);
    }
    
    public SimpleMapping find(ExpressionAST expression) {
    	return reverse_map.get(expression);
    }

	public AxisMapping find(Axis axis) {
		for (AxisMapping m : ax_map) {
			if (m.getAxis().equals(axis)) {
				return m;
			}
		}
		// else
		return null;
	}

	public MeasureMapping find(Measure mapping) {
		for (MeasureMapping m : kx_map) {
			if (m.getMapping().equals(mapping)) {
				return m;
			}
		}
		// else
		return null;
	}
    
    public List<MeasureMapping> getMeasureMapping() {
		return kx_map;
	}
    
    public List<AxisMapping> getAxisMapping() {
		return ax_map;
	}
    
    /**
     * return the total number of mappings (axis+measure)
     * @return
     */
    public int size() {
    	return ax_map.size()+kx_map.size();
    }

}
