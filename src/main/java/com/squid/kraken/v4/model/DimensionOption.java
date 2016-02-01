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
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

public class DimensionOption implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7203859790416372462L;

	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	public boolean mandatorySelection = false;

	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	public boolean singleSelection = false;

	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	public Expression defaultSelection = null;

	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	public boolean unmodifiableSelection = false;

	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	public boolean hidden = false;

	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	public List<String> groupFilter = null;

	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	public List<String> userFilter = null;
	
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

}
