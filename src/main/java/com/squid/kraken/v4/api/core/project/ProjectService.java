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
package com.squid.kraken.v4.api.core.project;

import java.util.List;

import com.squid.kraken.v4.api.core.GenericService;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.Report;

public interface ProjectService extends GenericService<Project, ProjectPK> {

    /**
     * Add (and create) a Domain to a Project.<br>
     * If the Domain already exists or does not exists, it'll be created.<br>
     * If the Domain is already added to this project, it will not be added once more.<br>
     * 
     * @param customerId
     * @param projectId
     * @param domain
     * @return the added domain Id
     */
    public String addDomain(String customerId, String projectId, Domain domain);

    public List<Domain> readDomains(String customerId, String projectId);

    /**
     * Compute an Analysis
     * 
     * @param projectId
     * @param dimensionIds
     * @param metricIds
     * @param filterIds
     * @return Report
     */
    public Report getReport(String customerId, String projectId, List<String> dimensionIds, List<String> metricIds,
            List<String> filterIds);

}
