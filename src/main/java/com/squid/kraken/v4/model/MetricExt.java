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

@SuppressWarnings("serial")
public class MetricExt extends Metric {
	
	private String definition;
	
	private boolean visible;

	public MetricExt(Metric metric, String definition, boolean visible) {
		super(metric.getId());
		setAccessRights(metric.getAccessRights());
		setDynamic(metric.isDynamic());
		setExpression(metric.getExpression());
		setName(metric.getName());
		setObjectType(metric.getObjectType());
		setUserRole(metric.getUserRole());
		this.definition = definition;
		this.visible = visible;
	}
	
	/**
	 * this is a formula that the application can use to safely reference the metric
	 * @return
	 */
	public String getDefinition() {
		return definition;
	}
	
	/**
	 * check if the metric is visible for selection
	 * @return
	 */
	public boolean isVisible() {
		return visible;
	}
	
}
