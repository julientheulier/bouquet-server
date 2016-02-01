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

import javax.xml.bind.annotation.XmlRootElement;

@SuppressWarnings("serial")
@XmlRootElement
public class MetricPK extends DomainPK {

    private String metricId;

    public MetricPK() {
    }

    public MetricPK(DomainPK parent, String metricId) {
        this(parent.getCustomerId(), parent.getProjectId(), parent.getDomainId(), metricId);
    }

    public MetricPK(String customerId, String projectId, String domainId, String metricId) {
        super(customerId, projectId, domainId);
        this.metricId = metricId;
    }

    public String getMetricId() {
        return metricId;
    }

    public void setMetricId(String metricId) {
        this.metricId = metricId;
    }

    @Override
    public String getObjectId() {
        return metricId;
    }

    @Override
    public void setObjectId(String id) {
        this.metricId = id;
    }

    @Override
    public DomainPK getParent() {
        return new DomainPK(getCustomerId(), getProjectId(), getDomainId());
    }

    @Override
    public void setParent(GenericPK pk) {
        setCustomerId(((DomainPK) pk).getCustomerId());
        setProjectId(((DomainPK) pk).getProjectId());
        setDomainId(((DomainPK) pk).getDomainId());
    }

	@Override
	public String toString() {
		return "MetricPK [metricId=" + metricId + ", getDomainId()="
				+ getDomainId() + ", getProjectId()=" + getProjectId()
				+ ", getCustomerId()=" + getCustomerId() + "]";
	}

}
