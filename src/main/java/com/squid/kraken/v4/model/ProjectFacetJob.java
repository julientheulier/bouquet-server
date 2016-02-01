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
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.mongodb.morphia.annotations.Index;
import org.mongodb.morphia.annotations.Indexes;

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.model.visitor.ModelVisitor;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;

@XmlType(namespace = "http://model.v4.kraken.squid.com")
@XmlRootElement
@SuppressWarnings("serial")
@Indexes(@Index("id.customerId, id.projectId, id.facetJobId"))
public class ProjectFacetJob extends JobBaseImpl<ProjectFacetJobPK, FacetSelection> {

    private List<DomainPK> domains;

    private FacetSelection selection;
    
    private Integer engineVersion;

    /**
     * Default constructor (required for jaxb).
     */
    public ProjectFacetJob() {
        super(null);
    }

    public ProjectFacetJob(ProjectFacetJobPK id) {
        super(id);
    }

    public FacetSelection getSelection() {
        return selection;
    }

    public void setSelection(FacetSelection selection) {
        this.selection = selection;
    }

    public List<DomainPK> getDomains() {
        if (domains == null) {
            domains = new ArrayList<DomainPK>();
        }
        return domains;
    }

    public void setDomain(List<DomainPK> domains) {
        this.domains = domains;
    }
    
    @XmlTransient
    @JsonIgnore
    public List<Domain> readDomains(AppContext ctx) throws ScopeException {
        List<Domain> domains = new ArrayList<Domain>();
        for (DomainPK domainId : getDomains()) {
            domains.add(ProjectManager.INSTANCE.getDomain(ctx, domainId));
        }
        return domains;
    }

    public Integer getEngineVersion() {
		return engineVersion;
	}

	public void setEngineVersion(Integer engineVersion) {
		this.engineVersion = engineVersion;
	}

	/**
     * Visitor.
     */
    @XmlTransient
    @JsonIgnore
    public void accept(ModelVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public ProjectFacetJobPK getId() {
        return super.getId();
    }

    @Override
    public Project getParentObject(AppContext ctx) {
        return DAOFactory.getDAOFactory().getDAO(Project.class).readNotNull(ctx,
                new ProjectPK(id.getCustomerId(), id.getProjectId()));
    }

    @Override
    public String toString() {
        return "ProjectFacetJob [id=" + id.getFacetJobId() + "]";
    }

}
