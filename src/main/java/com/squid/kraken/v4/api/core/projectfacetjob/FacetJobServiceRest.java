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

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.StreamingOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.squid.kraken.v4.api.core.BaseServiceRest;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl.OutputCompression;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl.OutputFormat;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.DataTable;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.ProjectFacetJob;
import com.squid.kraken.v4.model.ProjectFacetJobPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.Authorization;

@Produces({ MediaType.APPLICATION_JSON })
@Api(value = "facetjobs", hidden = true, authorizations = { @Authorization(value = "kraken_auth", type = "oauth2") })
public class FacetJobServiceRest extends BaseServiceRest {

	private static final Logger logger = LoggerFactory
			.getLogger(FacetJobServiceRest.class);

	private final static String PARAM_NAME = "jobId";

	private FacetJobServiceBaseImpl delegate = FacetJobServiceBaseImpl
			.getInstance();

	public FacetJobServiceRest(AppContext userContext) {
		super(userContext);
	}

	@GET
	@Path("")
	@ApiOperation(value = "Gets all FacetJobs")
	public List<ProjectFacetJob> readFacetJobs(
			@PathParam("projectId") String projectId) {
		return delegate.readAll(userContext, projectId);
	}

	@DELETE
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Deletes a FacetJob")
	public boolean delete(@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String jobId) {
		return delegate.delete(userContext,
				new ProjectFacetJobPK(userContext.getCustomerId(), projectId,
						jobId));
	}

	/**
	 * Method to retrieve a single {@link Facet}.
	 * 
	 * @param projectId
	 * @param jobId
	 * @param facetId
	 * @param filter
	 *            a search query to filter out results
	 * @param timeoutMs
	 * @param maxResults
	 * @param startIndex
	 * @param waitComplete if true the call will wait for the facet to be complete (isDone=true). False by default.
	 * @return
	 */
	@GET
	@Path("{" + PARAM_NAME + "}"+"/results/{facetId}")
	@ApiOperation(value = "Gets a Facet")
	public Facet readFacet(@PathParam("projectId") String projectId,
			@PathParam("jobId") String jobId,
			@PathParam("facetId") String facetId,
			@QueryParam("filter") String filter,
			@QueryParam("timeout") Integer timeoutMs,
			@QueryParam("maxResults") Integer maxResults,
			@QueryParam("startIndex") Integer startIndex,
			@QueryParam("waitComplete") Boolean waitComplete
		) {
		return delegate.readFacet(userContext, projectId, jobId, facetId,
				filter, timeoutMs, maxResults, startIndex, waitComplete==null?false:waitComplete);
	}

	/**
	 * Get a {@link DataTable}.<br>
	 * When requested this resource might require computation of the associated
	 * job.<br>
	 * 
	 * @param projectId
	 * @param jobId
	 *            the Job Id
	 * @param timeout
	 *            response timeout in milliseconds in case the job is not yet
	 *            computed.
	 * @param maxResults
	 *            paging size
	 * @param startIndex
	 *            paging start index
	 * @param format
	 *            an output format : "json", "csv", "xls" (default is "json")
	 * @param compression
	 *            an output compression : "gzip" or "none" (default is "none")
	 * @return a DataTable or an ErrorResponse if job computing is in progress
	 *         or has failed.<br>
	 *         If format or compression are not json/none then the returned data
	 *         will be set as "Content-Disposition",
	 *         "attachment; filename=+[job-]+jobId.[extension]".
	 */
	@GET
	@Path("{" + PARAM_NAME + "}"+"/results")
	@ApiOperation(value = "Gets FacetJob' results as a DataTable")
	public Response getResults(@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String jobId,
			@QueryParam("timeout") Integer timeout,
			@QueryParam("maxResults") Integer maxResults,
			@QueryParam("startIndex") Integer startIndex,
			@QueryParam("format") String format,
			@QueryParam("compression") String compression) {

		final ProjectFacetJob job = new ProjectFacetJob(new ProjectFacetJobPK(
				userContext.getCustomerId(), projectId, jobId));
		final Integer finalTimeOut = timeout;
		final Integer finalMaxResults = maxResults;
		final Integer finalStartIndex = startIndex;

		final OutputFormat outFormat;
		if (format == null) {
			outFormat = OutputFormat.JSON;
		} else {
			outFormat = OutputFormat.valueOf(format.toUpperCase());
		}

		final OutputCompression outCompression;
		if (compression == null) {
			outCompression = OutputCompression.NONE;
		} else {
			outCompression = OutputCompression.valueOf(compression
					.toUpperCase());
		}

		StreamingOutput stream = new StreamingOutput() {
			@Override
			public void write(OutputStream os) throws IOException,
					WebApplicationException {
				// pass the ouputStream to the delegate
				try {
					delegate.writeResults(os, userContext, job, 1000,
							finalTimeOut, true, finalMaxResults,
							finalStartIndex,false, outFormat, outCompression, null);
				} catch (InterruptedException e) {
					throw new IOException(e);
				}
			}
		};

		// build the response
		ResponseBuilder response;
		String fileName = "job-" + jobId;
		String mediaType;
		switch (outFormat) {
		case CSV:
			mediaType = MediaType.APPLICATION_OCTET_STREAM_TYPE.toString();
			fileName += ".csv";
			break;
		case XLS:
			mediaType = "application/vnd.ms-excel";
			fileName += ".xls";
			break;
		default:
			mediaType = MediaType.APPLICATION_JSON_TYPE.toString();
			fileName += ".json";
		}

		switch (outCompression) {
		case GZIP:
			// Note that CXF will GZIP if filename ends with ".gz"
			mediaType = MediaType.APPLICATION_OCTET_STREAM_TYPE.toString();
			fileName += ".gz";
			break;
		default:
			// NONE
		}

		response = Response.ok(stream, mediaType);
		if ((format != null) || (compression != null)) {
			logger.info("returnin results as " + mediaType + ", fileName : "
					+ fileName);
			response.header("Content-Disposition", "attachment; filename="
					+ fileName);
		}

		return response.build();

	}

	/**
	 * Create, update and/or re-compute a {@link ProjectFacetJob}.<br>
	 * If a Job with the same Id already exist but its status is
	 * <code>null</code> then update it and re-compute.<br>
	 * If a Job with the same Id does not exist then create it and compute.<br>
	 * If a timeout is set, this method will return once the job is done or
	 * until the timeout is reached.
	 * 
	 * @param projectId
	 * @param dashboardId
	 * @param analysisJob
	 *            the job to create / update
	 * @param timeout
	 *            in milliseconds
	 * @return ProjectFacetJob
	 */
	@POST
	@Path("")
	@ApiOperation(value = "Create, update and/or re-compute a FacetJob")
	public ProjectFacetJob post(@PathParam("projectId") String projectId,
			@ApiParam(required = true) ProjectFacetJob job, @QueryParam("timeout") Integer timeout) {
		return delegate.store(userContext, job, timeout);
	}

	@PUT
	@Path("")
	@ApiOperation(value = "Create, update and/or re-compute a FacetJob")
	public ProjectFacetJob put(@PathParam("projectId") String projectId,
			@ApiParam(required = true) ProjectFacetJob job, @QueryParam("timeout") Integer timeout) {
		return delegate.store(userContext, job, timeout);
	}

	/**
	 * JSONP specific method which supports both read operation and store
	 * operation using GET.
	 * 
	 * @param projectId
	 * @param dashboardId
	 * @param jobId
	 * @param FacetJobJSON
	 * @param timeout
	 * @return ProjectFacetJob
	 */
	@GET
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "JSONP specific method which supports both read operation and store operation using GET")
	public ProjectFacetJob readOrStore(
			@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String jobId,
			@ApiParam(required = true) @QueryParam("facetJob") String facetJobJSON,
			@QueryParam("timeout") Integer timeout) {
		if (facetJobJSON == null) {
			return delegate.read(userContext,
					new ProjectFacetJobPK(userContext.getCustomerId(),
							projectId, jobId));
		} else {
			ObjectMapper mapper = new ObjectMapper();
			// mapper.setSerializationInclusion(Inclusion.NON_NULL);
			try {
				ProjectFacetJob facetJob = mapper.readValue(facetJobJSON,
						ProjectFacetJob.class);
				return delegate.store(userContext, facetJob, timeout);
			} catch (Exception e) {
				throw new WebApplicationException(e);
			}
		}
	}

	@Path("{jobId}/access")
	@GET
	public Set<AccessRight> readAccessRights(
			@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String jobId) {
		return delegate.readAccessRights(userContext, new ProjectFacetJobPK(
				userContext.getCustomerId(), projectId, jobId));
	}

	@Path("{jobId}/access")
	@POST
	public Set<AccessRight> storeAccessRights(
			@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String jobId, Set<AccessRight> accessRights) {
		return delegate.storeAccessRights(userContext, new ProjectFacetJobPK(
				userContext.getCustomerId(), projectId, jobId), accessRights);
	}

}
