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

import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.mongodb.morphia.annotations.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A Facet describes Dimension values as a list of {@link FacetMember}s (items) and a list of selected items.<br>
 * As this items list can be large, a Facet might only contain a subset of the Dimension values starting at a given
 * index.<br>
 */
@XmlType(namespace = "http://model.v4.kraken.squid.com")
@SuppressWarnings("serial")
public class Facet implements Serializable {

	@Transient
    private List<FacetMember> items;

    private List<FacetMember> selectedItems;

    private DimensionPK dimension;
    
    private String name;
    
    @Transient
    private Dimension dimensionObject;
    
    private String id;

    // done flag to support V2 api
    private boolean isDone = true;
    // hasMore flag to support V2 api
    private boolean hasMore = false;
    // error flag to support V2 api
    private boolean error = false;
    private String errorMessage = null;
    
    // if the facet is actually a proxy to another dimension
    private boolean isProxy = false;
    
    private boolean isCompositeName = false;
    
    private int size;

    /**
     * copy the given facet to keep only the selection
     * @param facet
     * @return
     */
    public static Facet createSelectionfacet(Facet facet) {
        Facet f = new Facet();
        f.setDimension(facet.getDimension());
        f.setId(facet.getId());
        f.setSelectedItems(facet.getSelectedItems());
        f.hasMore = facet.isHasMore();
        f.isDone = facet.isDone();
        return f;
    }

    public Facet() {
    }
    
    public Facet(Facet facet) {
        this.setDimension(facet.getDimension());
        this.setId(facet.getId());
        this.setItems(facet.getItems());
        this.setSelectedItems(facet.getSelectedItems());
        this.hasMore = facet.isHasMore();
        this.isDone = facet.isDone();
        this.setTotalSize(facet.getTotalSize());
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
    
    /**
     * If false, the facet computation is not finished yet.
     */
    public boolean isDone() {
        return isDone;
    }
    
    public void setDone(boolean isDone) {
        this.isDone = isDone;
    }

    public boolean isHasMore() {
		return hasMore;
	}

	public void setHasMore(boolean hasMore) {
		this.hasMore = hasMore;
	}
	
	public boolean isError() {
        return error;
    }

    public void setError(boolean error) {
        this.error = error;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    /**
     * List of selectable items.
     */
    public List<FacetMember> getItems() {
        if (items == null) {
            items = new ArrayList<FacetMember>();
        }
        return items;
    }

    public void setItems(List<FacetMember> items) {
        this.items = items;
    }

    /**
     * List of currently selected items.
     */
    public List<FacetMember> getSelectedItems() {
        if (selectedItems == null) {
            selectedItems = new ArrayList<FacetMember>();
        }
        return selectedItems;
    }

    public void setSelectedItems(List<FacetMember> selectedItems) {
        this.selectedItems = selectedItems;
    }

    /**
     * The Facet Dimension Id which is persisted but not exposed to the Rest API as a full Dimension object will be
     * exposed by {@link #getDimension()}.
     * 
     * @return
     */
    @XmlTransient
    @JsonIgnore
    public DimensionPK getDimensionId() {
        return dimension;
    }

    @XmlTransient
    @JsonIgnore
    public void setDimensionId(DimensionPK dimension) {
        this.dimension = dimension;
    }

    /**
     * The associated Dimension.
     * 
     * @return the full Dimension object.
     */
    public Dimension getDimension() {
        return dimensionObject;
    }

    public void setDimension(Dimension dimension) {
        if (dimension != null) {
            this.dimensionObject = dimension;
            this.dimension = dimension.getId();
            this.name = dimension.getName();
        }
    }
    
    public boolean isProxy() {
		return isProxy;
	}
    
    public void setProxy(boolean isProxy) {
		this.isProxy = isProxy;
	}
    
    /**
     * @Return True if the facet name is a composite name based on the proxy names, False if it is the name of the Dimension
	 *
	 */
	public boolean isCompositeName() {
		return isCompositeName;
	}
	
	
	public void setCompositeName(boolean isCompositeName) {
		this.isCompositeName = isCompositeName;
	}

    public int getTotalSize() {
        return size;
    }

    public void setTotalSize(int size) {
        this.size = size;
    }
    
    @Override
    public String toString() {
    	StringBuilder builder = new StringBuilder("Facet["+name+";id="+id);
    	if (selectedItems!=null && !selectedItems.isEmpty()) {
    		builder.append(";selections["+selectedItems.size()+"]="+selectedItems.toString());
    	}
    	if (items!=null && !items.isEmpty()) {
    		builder.append(";items["+items.size()+"]=["+items.get(0).toString()+(items.size()>1?",...]":"]"));
    	}
    	builder.append("]");
    	return builder.toString();
    }

}
