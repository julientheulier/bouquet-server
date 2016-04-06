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

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import com.squid.kraken.v4.api.core.JobResultBaseImpl;

@XmlRootElement
@SuppressWarnings("serial")
/**
 * The FacetSelection class defines a selection that can be use for performing Analysis & Facet jobs.
 * It is a list of @see Facet objects, where each facet defines the axis to filter on and a list of selected values.
 * 
 * Since 4.2.4, FacetSelection also support the definition of a compare selection. If it is set, the Analysis job will perform a compare query.
 * 
 * @author sergefantino
 *
 */
public class FacetSelection extends JobResultBaseImpl {

    private List<Facet> facets;
    
    private List<Facet> compareTo;

    public FacetSelection() {
    }

    public List<Facet> getFacets() {
        if (facets == null) {
            facets = new ArrayList<Facet>();
        }
        return facets;
    }

    public void setFacets(List<Facet> facets) {
        this.facets = facets;
    }
    
    public List<Facet> getCompareTo() {
        if (compareTo == null) {
        	compareTo = new ArrayList<Facet>();
        }
		return compareTo;
	}
    
    public void setCompareTo(List<Facet> compareFacets) {
		this.compareTo = compareFacets;
	}
    
	public boolean hasCompareFacets() {
		return compareTo!=null && !compareTo.isEmpty();
	}

    @Override
    public long getTotalSize() {
        int size = 0;
        for (Facet facet : getFacets()) {
            if (facet!=null) size += facet.getItems().size();
        }
        return size;
    }

    public void setTotalSize(int size) {
        // ignored - here just for jaxb binding
    }
    
    @Override
    public String toString() {
    	if (facets!=null) {
    		StringBuilder dump = new StringBuilder("FacetSelection"+facets.toString());
    		if (compareTo!=null && !compareTo.isEmpty()) {
    			dump.append("+CompareSelection"+compareTo);
    		}
    		return dump.toString();
    	} else {
    		return "FacetSelection[]";
    	}
    }

}
