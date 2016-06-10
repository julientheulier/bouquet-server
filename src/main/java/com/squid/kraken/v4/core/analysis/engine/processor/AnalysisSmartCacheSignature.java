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
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;

import com.squid.kraken.v4.core.analysis.model.DashboardAnalysis;
import com.squid.kraken.v4.core.analysis.model.DomainSelection;
import com.squid.kraken.v4.core.analysis.model.ExpressionInput;
import com.squid.kraken.v4.core.analysis.model.GroupByAxis;
import com.squid.kraken.v4.core.analysis.model.MeasureGroup;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.Domain;

/**
 * AnalysisSignature is an object that store information require to check if a new analysis can be derived from the cache.
 * For example it computes various signature (hash) that encode the analysis axes, measures, filters.
 * If two analysis signatures are compatible, that means it worth performing additional checks in order to know if there is a Match
 * @author sergefantino
 *
 */
public class AnalysisSmartCacheSignature {
	
	private DashboardAnalysis analysis;
	private MeasureGroup measures;
	
	private String axesSignature = null;
	private String filtersSignature = null;
	private String hash;
	
	private String SQL;
	
	private Set<Axis> axes = null;
	private int rowCount = -1;// default to -1 meaning the resultset is not yet available (being computed)

	public AnalysisSmartCacheSignature(DashboardAnalysis analysis, MeasureGroup measures, String SQL) {
		super();
		this.analysis = analysis;
		this.measures = measures;
		this.SQL = SQL;
		this.hash = DigestUtils.sha256Hex(SQL);
	}
	
	/**
	 * @return the sQL
	 */
	public String getSQL() {
		return SQL;
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
	public String getAxesSignature() {
		return axesSignature;
	}
	
	/**
	 * @return the axes
	 */
	public Set<Axis> getAxes() {
		return axes;
	}
	
	/**
	 * @param axesSignature the axesSignature to set
	 */
	public void setAxesSignature(Universe universe) {
		this.axesSignature = computeAxesSignature(universe);
	}

	/**
	 * @param analysis 
	 * @return
	 */
	protected String computeAxesSignature(Universe universe) {
		//
		// add the customer ID / project ID
		StringBuilder signature = new StringBuilder();
		signature.append(universe.getProject().getId().toUUID());
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
	public String getFiltersSignature() {
		return filtersSignature;
	}
	
	/**
	 * @param filtersSignature the filtersSignature to set
	 */
	public void setFiltersSignature(String filtersSignature) {
		this.filtersSignature = filtersSignature;
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
		AnalysisSmartCacheSignature other = (AnalysisSmartCacheSignature) obj;
		if (hash == null) {
			if (other.hash != null)
				return false;
		} else if (!hash.equals(other.hash))
			return false;
		return true;
	}

}
