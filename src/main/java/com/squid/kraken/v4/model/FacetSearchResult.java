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

import javax.xml.bind.annotation.XmlType;

@XmlType(namespace = "http://model.v4.kraken.squid.com")
@SuppressWarnings("serial")
/**
 * A Facet search result.
 * Wraps a Facet to add the queried filter string back in API response.
 */
public class FacetSearchResult extends Facet implements Serializable {
	private String filter;
	
	public FacetSearchResult() {
	}
	
	public FacetSearchResult(Facet facet, String filter) {
		this.filter = filter;
		this.setCompositeName(facet.isCompositeName());
		this.setDimension(facet.getDimension());
		this.setDimensionId(facet.getDimensionId());
		this.setDone(facet.isDone());
		this.setError(facet.isError());
		this.setErrorMessage(facet.getErrorMessage());
		this.setHasMore(facet.isHasMore());
		this.setId(facet.getId());
		this.setItems(facet.getItems());
		this.setName(facet.getName());
		this.setProxy(facet.isProxy());
		this.setSelectedItems(facet.getSelectedItems());
		this.setTotalSize(facet.getTotalSize());
	}


	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

}