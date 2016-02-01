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
package com.squid.kraken.v4.core.analysis.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.model.Domain;


/**
 * A group of measures defined on the same space
 * @author sfantino
 *
 */
public class MeasureGroup {

	private Measure master;
	private List<Measure> group;
	
	public MeasureGroup(Measure master) {
		this.master = master;
		this.group = new ArrayList<Measure>();
		this.group.add(master);
	}
	
	public Domain getRoot() {
	    return master.getParent().getRoot();
	}
	
	public Measure getMaster() {
		return master;
	}

	public List<Measure> getKPIs() {
		if (group==null) {
			return Collections.emptyList();
		} else {
			return group;
		}
	}
	
	/**
	 * merge the KPI with the group if it is possible;
	 * @param kpi
	 * @return true if merge is successful, false otherwise...
	 */
	public boolean merge(Measure measure) {
		if (master.getParent().getRoot().equals(measure.getParent().getRoot())) {
			return add(measure);
		}
		// else
		return false;
	}

	private boolean add(Measure measure) {
		if (group==null) {
			group = new ArrayList<Measure>();
		}
		return group.add(measure);
	}
	
}
