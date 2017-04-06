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

import com.fasterxml.jackson.annotation.JsonInclude;

public class DimensionOption implements Serializable {
	
	private DimensionOptionPK id;

	/**
	 * 
	 */
	private static final long serialVersionUID = 7203859790416372462L;

	public boolean mandatorySelection = false;

	public boolean singleSelection = false;

	@JsonInclude(JsonInclude.Include.ALWAYS)
	public Expression defaultSelection = null;

	public boolean unmodifiableSelection = false;

	public boolean hidden = false;

	@JsonInclude(JsonInclude.Include.ALWAYS)
	public List<String> groupFilter = new ArrayList<>();

	@JsonInclude(JsonInclude.Include.ALWAYS)
	public List<String> userFilter = new ArrayList<>();
	
	public DimensionOption() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * merge with option which has better precedence
	 * @param merge
	 * @param option
	 */
	public DimensionOption(DimensionOption merge, DimensionOption option) {
		this.mandatorySelection = merge.mandatorySelection || option.mandatorySelection;
		this.singleSelection = merge.singleSelection || option.singleSelection;
		this.unmodifiableSelection = merge.unmodifiableSelection || option.unmodifiableSelection;
		this.hidden = merge.hidden || option.hidden;
		if (option.defaultSelection!=null) this.defaultSelection = option.defaultSelection;
		// ignore the filters
	}
	
	/**
	 * @return the id
	 */
	public DimensionOptionPK getId() {
		return id;
	}
	
	/**
	 * @param id the id to set
	 */
	public void setId(DimensionOptionPK id) {
		this.id = id;
	}

	public boolean isMandatorySelection() {
		return mandatorySelection;
	}

	public void setMandatorySelection(boolean mandatorySelection) {
		this.mandatorySelection = mandatorySelection;
	}

	public boolean isSingleSelection() {
		return singleSelection;
	}

	public void setSingleSelection(boolean singleSelection) {
		this.singleSelection = singleSelection;
	}

	public Expression getDefaultSelection() {
		return defaultSelection;
	}

	public void setDefaultSelection(Expression defaultSelection) {
		this.defaultSelection = defaultSelection;
	}

	public boolean isUnmodifiableSelection() {
		return unmodifiableSelection;
	}

	public void setUnmodifiableSelection(boolean unmodifiableSelection) {
		this.unmodifiableSelection = unmodifiableSelection;
	}

	public boolean isHidden() {
		return hidden;
	}

	public void setHidden(boolean hidden) {
		this.hidden = hidden;
	}

	public List<String> getGroupFilter() {
		return groupFilter;
	}

	public void setGroupFilter(List<String> groupFilter) {
		this.groupFilter = groupFilter;
	}

	public List<String> getUserFilter() {
		return userFilter;
	}

	public void setUserFilter(List<String> userFilter) {
		this.userFilter = userFilter;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((defaultSelection == null) ? 0 : defaultSelection.hashCode());
		result = prime * result + ((groupFilter == null) ? 0 : groupFilter.hashCode());
		result = prime * result + (hidden ? 1231 : 1237);
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + (mandatorySelection ? 1231 : 1237);
		result = prime * result + (singleSelection ? 1231 : 1237);
		result = prime * result + (unmodifiableSelection ? 1231 : 1237);
		result = prime * result + ((userFilter == null) ? 0 : userFilter.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DimensionOption other = (DimensionOption) obj;
		if (defaultSelection == null) {
			if (other.defaultSelection != null)
				return false;
		} else if (!defaultSelection.equals(other.defaultSelection))
			return false;
		if (groupFilter == null) {
			if (other.groupFilter != null)
				return false;
		} else if (!groupFilter.equals(other.groupFilter))
			return false;
		if (hidden != other.hidden)
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (mandatorySelection != other.mandatorySelection)
			return false;
		if (singleSelection != other.singleSelection)
			return false;
		if (unmodifiableSelection != other.unmodifiableSelection)
			return false;
		if (userFilter == null) {
			if (other.userFilter != null)
				return false;
		} else if (!userFilter.equals(other.userFilter))
			return false;
		return true;
	}

}
