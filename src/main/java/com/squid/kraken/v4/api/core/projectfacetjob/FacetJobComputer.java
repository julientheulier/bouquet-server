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

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeoutException;

import com.squid.kraken.v4.api.core.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.project.ProjectServiceBaseImpl;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingService;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.export.ExportSourceWriter;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.FacetSelection;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectFacetJob;
import com.squid.kraken.v4.model.ProjectFacetJobPK;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AppContext;

/**
 * Compute a ProjectFacetJob using the Engine.<br>
 */
public class FacetJobComputer implements
		JobComputer<ProjectFacetJob, ProjectFacetJobPK, FacetSelection> {

	static final Logger logger = LoggerFactory
			.getLogger(FacetJobComputer.class);

	@Override
	public FacetSelection compute(AppContext ctx, ProjectFacetJob job,
			Integer maxResults, Integer startIndex, boolean lazy) throws ComputingException,
			InterruptedException {
		long start = System.currentTimeMillis();
		logger.info("FacetJobComputer.compute(): start");
		ProjectServiceBaseImpl projService = ProjectServiceBaseImpl
				.getInstance();
		String customerId = job.getCustomerId();

		FacetSelection selection = job.getSelection();

		ProjectPK projectPK = new ProjectPK(customerId, job.getId()
				.getProjectId());
		logger.debug("FacetJobComputer.compute(): stepB in "
				+ ((new Date().getTime()) - start) + " ms");
		// make sure user can read the project
		projService.read(ctx, projectPK, true);
		logger.debug("FacetJobComputer.compute(): stepA in "
				+ ((new Date().getTime()) - start) + " ms");

		Project project = projService.read(ctx, projectPK, true);
		Universe universe = new Universe(ctx, project);

		// setup the selection
		DashboardSelection ds;
		List<Facet> result = new ArrayList<>();
		try {
			logger.debug("FacetJobComputer.compute(): step1 in "
					+ ((new Date().getTime()) - start) + " ms");
			//
			List<Domain> domains = job.readDomains(ctx);
			ds = EngineUtils.getInstance().applyFacetSelection(ctx,
					universe, domains, selection);
			logger.debug("FacetJobComputer.compute(): step2 in "
					+ ((new Date().getTime()) - start) + " ms");

			// setup the results
			for (Domain domain : domains) {
				result.addAll(glitterFacets(job, universe, domain, ds));
			}
			/*logger.info("FacetJobComputer.compute(): done in "
					+ ((new Date().getTime()) - start) + " ms");*/
			long duration = (new Date().getTime()-start);
			logger.info("task="+this.getClass().getName()+" method=FacetJobComputer.compute()"+" duration="+duration+ " error=false end");
			JobStats queryLog = new JobStats(job.getId().toString(),"FacetJobComputer", duration,job.getId().getProjectId());
			PerfDB.INSTANCE.save(queryLog);
			FacetSelection facetSelectionResult = new FacetSelection();
			facetSelectionResult.setFacets(result);
			// handling compareTo (T947)
			if (ds.hasCompareToSelection()) {
				// create a fresh seelction with the compareTo
				DashboardSelection compareDS = new DashboardSelection();
				Domain domain = ds.getCompareToSelection().getDomain();
				compareDS.add(ds.getCompareToSelection());
				ArrayList<Facet> facets = new ArrayList<>();
				for (Axis filter : ds.getCompareToSelection().getFilters()) {
					facets.add(ComputingService.INSTANCE.glitterFacet(universe, domain, compareDS, filter, null, 0, 100, null));
				}
				facetSelectionResult.setCompareTo(facets);
			}
			return facetSelectionResult;
		} catch (ScopeException e) {
			throw new ComputingException(e.getLocalizedMessage(), e);
		} catch (TimeoutException e) {
			throw new ComputingInProgressAPIException(null,
					ctx.isNoError(), null);
		}
	}
	
	protected Collection<Facet> glitterFacets(ProjectFacetJob job, Universe universe, Domain domain, DashboardSelection ds) throws ComputingException, InterruptedException, TimeoutException {
		if (job.getEngineVersion() == null) {
			return ComputingService.INSTANCE.glitterFacets(universe,
					domain, ds, job.getIncludeDynamic());
		} else {
			return ComputingService.INSTANCE.glitterFacets(universe,
					domain, ds, null, job.getIncludeDynamic());
		}
	}

	@Override
	public FacetSelection compute(AppContext ctx, ProjectFacetJob job,
			OutputStream outputStream, ExportSourceWriter writer, boolean lazy)
			throws ComputingException {
		throw new RuntimeException("not implemented");
	}

}
