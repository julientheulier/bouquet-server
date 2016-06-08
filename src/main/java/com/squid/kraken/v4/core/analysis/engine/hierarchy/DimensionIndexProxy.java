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

import java.util.List; 

import com.squid.kraken.v4.core.analysis.engine.index.IDimensionStore;
import com.squid.kraken.v4.core.analysis.universe.Axis;

public class DimensionIndexProxy extends DimensionIndex {

	
	
	private DimensionIndex sourceIndex;
	private Axis source = null;

	/**
	 * Proxy constructor
	 * @param source is the axis for the original sub-domain dimension
	 * @param parent
	 * @param axis
	 * @param proxy
	 */

	public DimensionIndexProxy(Axis source, DimensionIndex parent, Axis axis, DimensionIndex sourceIndex) {
		super();
	    _DimensionIndex(parent, axis);

	    this.sourceIndex = sourceIndex;
        this.source = source;
        this.sourceIndex.registerProxy(this);
	}
	
	public boolean isProxy() {
		return true;
	}
	
	public DimensionIndex getSourceIndex() {
		return sourceIndex;
	}
	
	
	@Override
	public IDimensionStore getStore() {
		if (this.sourceIndex == null){
			return null;
		}else{
			return this.sourceIndex.getStore();
		}   
	}	
	
	/**
	 * check if the index should be visible
	 * @return
	 */
	@Override
	public boolean isVisible() {
		if (sourceIndex!=null) {
			if (getDomain().isDynamic()) {
				return sourceIndex.isVisible();
			} else {
				return !source.getDimension().isDynamic() && sourceIndex.isVisible();// if explicit link and the target is visible
			}
		} else {
			return false;
		}
	}
	
	
	@Override	
    public void initCorrelationMapping(List<DimensionIndex> hierarchy) {
	}
	
	@Override
	public Status getStatus() {
	    if (sourceIndex==null) {
	    	return Status.STALE;
	    } else {
	        return sourceIndex.getStatus();
	    }
    }

	
    @Override
    public String toString() {
        return "DimensionIndex:["+getDimensionName()+"](sourceIndex)";
    }
}
