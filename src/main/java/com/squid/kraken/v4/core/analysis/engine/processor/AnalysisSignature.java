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
package com.squid.kraken.v4.core.analysis.engine.processor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;

import com.squid.kraken.v4.core.analysis.model.DashboardAnalysis;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.model.DomainSelection;
import com.squid.kraken.v4.core.analysis.model.ExpressionInput;
import com.squid.kraken.v4.core.analysis.model.GroupByAxis;
import com.squid.kraken.v4.core.analysis.model.MeasureGroup;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.Domain;

/**
 * Prototype: this class allow to define an analysis signature to intelligently reuse cached matrix
 * @author sergefantino
 *
 */
public class AnalysisSignature {
	
	private DashboardAnalysis analysis;
	private MeasureGroup measures;
	
	private String axesSignature = null;
	private String filtersSignature = null;
	private String hash;
	
	private Set<Axis> axes = null;
	private int rowCount;

	public AnalysisSignature(DashboardAnalysis analysis, MeasureGroup measures, String SQL) {
		super();
		this.analysis = analysis;
		this.measures = measures;
		this.hash = DigestUtils.sha256Hex(SQL);
	}
	
	/**
	 * @return the hash
	 */
	public String getHash() {
		return hash;
	}
	
	/**
	 * @return the analysis
	 */
	public DashboardAnalysis getAnalysis() {
		return analysis;
	}
	
	/**
	 * @return the measures
	 */
	public MeasureGroup getMeasures() {
		return measures;
	}
	
	/**
	 * @return the axesSignature
	 */
	public String getAxesSignature(Universe universe) {
		if (axesSignature==null) {
			axesSignature = computeConstantSignature(universe, analysis);
		}
		return axesSignature;
	}
	
	/**
	 * @return the axes
	 */
	public Set<Axis> getAxes() {
		return axes;
	}

	/**
	 * @param analysis 
	 * @return
	 */
	private String computeConstantSignature(Universe universe, DashboardAnalysis analysis) {
		//
		// add the project ID
		StringBuilder signature = new StringBuilder(universe.getProject().getOid());
		//
		// add the measure group domain
		Domain root = measures.getRoot();
		signature.append("##").append(root.getOid());
		//
		// add the axes
		signature.append("#");
		ArrayList<GroupByAxis> ordered = new ArrayList<>(analysis.getGrouping());
		Collections.sort(ordered, new Comparator<GroupByAxis>() {
			@Override
			public int compare(GroupByAxis o1, GroupByAxis o2) {
				return o1.getAxis().getId().compareTo(o2.getAxis().getId());
			}
		});
		axes = new HashSet<>();
		for (GroupByAxis axis : ordered) {
			String hash = DigestUtils.sha256Hex(axis.getAxis().getId());
			signature.append("#").append(hash);
			axes.add(axis.getAxis());
		}
		//
		// add the expression filters
		ArrayList<DomainSelection> domains = new ArrayList<>(analysis.getSelection().get());
		Collections.sort(domains, new Comparator<DomainSelection>() {
			@Override
			public int compare(DomainSelection o1, DomainSelection o2) {
				return o1.getDomain().getOid().compareTo(o2.getDomain().getOid());
			}
		});
		for (DomainSelection ds : domains) {
			// add domain
			signature.append("#");
			String domainId = ds.getDomain().getOid();
			ArrayList<ExpressionInput> inputs = new ArrayList<>(ds.getConditions());
			Collections.sort(inputs, new Comparator<ExpressionInput>() {
				@Override
				public int compare(ExpressionInput o1, ExpressionInput o2) {
					return o1.getInput().compareTo(o2.getInput());
				}
			});
			for (ExpressionInput input : inputs) {
				// hash the expression input
				String normalized = input.getExpression().prettyPrint();
				String hash = DigestUtils.sha256Hex(domainId+"!"+normalized);
				signature.append("#").append(hash);
			}
		}
		return signature.toString();
	}
	
	/**
	 * @return the axesSignature
	 */
	public String getFiltersSignature(Universe universe) {
		if (filtersSignature==null) {
			filtersSignature = computeFiltersSignature(universe, analysis.getSelection());
		}
		return filtersSignature;
	}
	
	protected String computeFiltersSignature(Universe universe, DashboardSelection selection) {
		if (selection.isEmpty()) {
			return "#EMPTY#";
		} else {
			return computeFiltersSignature(universe, selection.getFilters());
		}
	}
	
	protected String computeFiltersSignature(Universe universe, List<Axis> filters) {
		// sort the domains
		StringBuilder signature = new StringBuilder();
		Collections.sort(filters, new Comparator<Axis>() {
			@Override
			public int compare(Axis o1, Axis o2) {
				return o1.getId().compareTo(o2.getId());
			}
		});
		for (Axis axis : filters) {
			String hash = DigestUtils.sha256Hex(axis.getId());
			signature.append("#").append(hash);
		}
		//
		return signature.toString();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((hash == null) ? 0 : hash.hashCode());
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
		AnalysisSignature other = (AnalysisSignature) obj;
		if (hash == null) {
			if (other.hash != null)
				return false;
		} else if (!hash.equals(other.hash))
			return false;
		return true;
	}

	/**
	 * @param rowCount
	 */
	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}
	
	/**
	 * @return the rowCount
	 */
	public int getRowCount() {
		return rowCount;
	}

}
