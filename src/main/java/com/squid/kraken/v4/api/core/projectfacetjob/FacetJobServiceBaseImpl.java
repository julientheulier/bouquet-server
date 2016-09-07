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
package com.squid.kraken.v4.api.core.projectfacetjob;

import java.util.List; 
import java.util.concurrent.TimeoutException;

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.ComputingInProgressAPIException;
import com.squid.kraken.v4.api.core.EngineUtils;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.api.core.project.ProjectServiceBaseImpl;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchy;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.SegmentManager;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingService;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.FacetSearchResult;
import com.squid.kraken.v4.model.FacetSelection;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectFacetJob;
import com.squid.kraken.v4.model.ProjectFacetJobPK;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.dao.ProjectFacetJobDAO;

public class FacetJobServiceBaseImpl extends
		JobServiceBaseImpl<ProjectFacetJob, ProjectFacetJobPK, FacetSelection> {

	private static FacetJobServiceBaseImpl instance;

	public static FacetJobServiceBaseImpl getInstance() {
		if (instance == null) {
			instance = new FacetJobServiceBaseImpl();
		}
		return instance;
	}

	private FacetJobServiceBaseImpl() {
		// made private for singleton access
		super(ProjectFacetJob.class, new FacetJobComputer());
	}

	@Override
	protected FacetSelection paginateResults(FacetSelection results,
			Integer maxResults, Integer startIndex) {
		// no paging for FacetJobResults
		return results;
	}

	public List<ProjectFacetJob> readAll(AppContext app, String projectId) {
		return ((ProjectFacetJobDAO) factory.getDAO(ProjectFacetJob.class))
				.findByProject(app, new ProjectPK(app.getCustomerId(),
						projectId));
	}

	public FacetSearchResult readFacet(AppContext userContext, String projectId,
			String jobId, String facetId, String filter, Integer timeoutMs,
			Integer maxResults, Integer startIndex) {
		//
		Facet facet = null;
		//
		try {
			ProjectFacetJobPK jobPK = new ProjectFacetJobPK(
					userContext.getCustomerId(), projectId, jobId);
			//
			ProjectFacetJob job = read(userContext, jobPK);
			ProjectServiceBaseImpl projService = ProjectServiceBaseImpl
					.getInstance();
			String customerId = job.getCustomerId();

			FacetSelection selection = job.getSelection();

			ProjectPK projectPK = new ProjectPK(customerId, job.getId()
					.getProjectId());
			// make sure user can read the project
			projService.read(userContext, projectPK, true);

			// get the project using a root context since JDBC settings may not
			// be visible to the user
			Project project = projService.read(ServiceUtils.getInstance()
					.getRootUserContext(customerId), projectPK, true);
			Universe universe = new Universe(userContext, project);
			//
			List<Domain> domains = job.readDomains(userContext);
			DashboardSelection sel = EngineUtils.getInstance()
					.applyFacetSelection(userContext, universe, domains, selection);
			//
			if (SegmentManager.isSegmentFacet(facetId)) {
				for (Domain domain : domains) {
					DomainHierarchy hierarchy = universe
							.getDomainHierarchy(domain, true);
					return new FacetSearchResult(SegmentManager.createSegmentFacet(universe, hierarchy, domain,
							facetId, filter, maxResults, startIndex, sel), filter);
				}
			} else {
				Axis axis = EngineUtils.getInstance().getFacetAxis(userContext,
						universe, facetId);// universe.axis(facetId);
				Domain domain = axis.getParent().getTop().getDomain();
				//
				if (!domains.contains(domain)) {
					DimensionIndex index = axis.getIndex();
					if (index!=null) {
						throw new ScopeException("cannot list the facet for '"+index.getDimensionName()+"': not in the job scope");
					} else {
						throw new ScopeException("cannot list the facet for '"+axis.prettyPrint()+"': not in the job scope");
					}
				}
				
				facet = ComputingService.INSTANCE.glitterFacet(universe,
						domain, sel, axis, filter,
						startIndex != null ? startIndex : 0,
						maxResults != null ? maxResults : 500, timeoutMs);

				if (facet == null) {
					throw new ObjectNotFoundAPIException(
							"no facet found with id : " + facetId,
							userContext.isNoError());
				}
				// KRKN-53: if cannot compute the facet, just return with error informations
				/*
				if (facet.isError()) {
					throw new APIException(facet.getErrorMessage(),
							userContext.isNoError(), ApiError.COMPUTING_FAILED);
				}
				*/
			}
		} catch (InterruptedException | ScopeException | ComputingException e) {
			throw new APIException(e, userContext.isNoError());
		} catch (TimeoutException e) {
			throw new ComputingInProgressAPIException(null,
					userContext.isNoError(), null);
		}
		//
		return new FacetSearchResult(facet, filter);
	}

}
