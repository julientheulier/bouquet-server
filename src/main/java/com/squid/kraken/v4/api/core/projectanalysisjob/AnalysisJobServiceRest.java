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
package com.squid.kraken.v4.api.core.projectanalysisjob;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
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

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.squid.core.expression.scope.ScopeException;
import com.squid.core.sql.model.SQLScopeException;
import com.squid.core.sql.render.RenderingException;
import com.squid.kraken.v4.api.core.BaseServiceRest;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl.OutputCompression;
import com.squid.kraken.v4.api.core.JobServiceBaseImpl.OutputFormat;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.export.ExportSourceWriterVelocity;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.DataTable;
import com.squid.kraken.v4.model.ProjectAnalysisJob;
import com.squid.kraken.v4.model.ProjectAnalysisJobPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.Authorization;
import com.wordnik.swagger.annotations.AuthorizationScope;

@Produces({ MediaType.APPLICATION_JSON })
@Api(value = "analysisjobs", hidden = true, authorizations = { @Authorization(value = "kraken_auth", type = "oauth2", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
public class AnalysisJobServiceRest extends BaseServiceRest {

	private static final Logger logger = LoggerFactory
			.getLogger(AnalysisJobServiceRest.class);

	private final static String PARAM_NAME = "jobId";

	private AnalysisJobServiceBaseImpl delegate = AnalysisJobServiceBaseImpl
			.getInstance();

	public AnalysisJobServiceRest(AppContext userContext) {
		super(userContext);
	}

	@GET
	@Path("/")
	@ApiOperation(value = "Gets all AnalysisJobs")
	public List<ProjectAnalysisJob> readAnalysisJobs(
			@PathParam("projectId") String projectId) {
		return delegate.readAll(userContext, projectId);
	}

	@DELETE
	@Path("{" + PARAM_NAME + "}")
	@ApiOperation(value = "Deletes an AnalysisJobs")
	public boolean delete(@PathParam("projectId") String projectId,
						  @PathParam(PARAM_NAME) String jobId) {
		return delegate.delete(userContext, new ProjectAnalysisJobPK(
				userContext.getCustomerId(), projectId, jobId));
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
	 *            computed. If no timeout set, the method will return according
	 *            to current job status.
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
	@Path("{" + PARAM_NAME + "}" + "/results")
	@ApiOperation(value = "Gets an AnalysisJobs' results as a DataTable")
	public Response getResults(
			@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String jobId,
			@ApiParam(value = "response timeout in milliseconds in case the job is not yet computed. If no timeout set, the method will return according to current job status.") @QueryParam("timeout") Integer timeout,
			@ApiParam(value = "paging size") @QueryParam("maxResults") Integer maxResults,
			@ApiParam(value = "paging start index") @QueryParam("startIndex") Integer startIndex,
			@ApiParam(value = "if true, get the analysis only if already in cache", defaultValue="false") @QueryParam("lazy") boolean lazy,
			@ApiParam(value = "output format", allowableValues = "json,csv,vxls", defaultValue = "json") @QueryParam("format") String format,
			@ApiParam(value = "output compression", allowableValues = "gzip, none, null", defaultValue = "none") @QueryParam("compression") String compression) {
		logger.info("getResults : lazy?" + lazy );

		final ProjectAnalysisJobPK id = new ProjectAnalysisJobPK(
				userContext.getCustomerId(), projectId, jobId);
		final ProjectAnalysisJob job = new ProjectAnalysisJob(id);
		return getResults(projectId, job, timeout, maxResults, startIndex,lazy,
				format, compression, true);
	}

	@GET
	@Path("{" + PARAM_NAME + "}" + "/sql")
	@ApiOperation(value = "Get the SQL code used to compute the analysis")
	public Response viewSQL(
			@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String jobId,
			@ApiParam(value = "if true return the SQL code as a html page and apply a prettyfier (default to true)") @QueryParam("prettyfier") Boolean prettyfierFlag
	) throws ComputingException, InterruptedException, ScopeException, SQLScopeException, RenderingException
	{
		final ProjectAnalysisJobPK id = new ProjectAnalysisJobPK(
				userContext.getCustomerId(), projectId, jobId);
		boolean prettyfier = prettyfierFlag!=null?prettyfierFlag:true;
		String output = delegate.viewSQL(userContext, id, prettyfier);
		ResponseBuilder builder = Response.ok(output, prettyfier?"text/html":"text/sql");
		return builder.build();
	}

	@GET
	@Path("{" + PARAM_NAME + "}" + "/render")
	@ApiOperation(value = "Gets an AnalysisJobs' results decorated by a Velocity template")
	public Response render(
			@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String jobId,
			@ApiParam(value = "response timeout in milliseconds in case the job is not yet computed. If no timeout set, the method will return according to current job status.") @QueryParam("timeout") Integer timeout,
			@ApiParam(value = "paging size") @QueryParam("maxResults") Integer maxResults,
			@ApiParam(value = "paging start index") @QueryParam("startIndex") Integer startIndex,
			@ApiParam(value = "if true, get the analysis only if already in cache", defaultValue="false") @QueryParam("lazy") boolean lazy,
			@ApiParam(value = "response media type") @QueryParam("type") String type,
			@ApiParam(value = "Velocity template as a base64 String") @QueryParam("template") String template)
	{
		final ProjectAnalysisJobPK id = new ProjectAnalysisJobPK(
				userContext.getCustomerId(), projectId, jobId);
		final ProjectAnalysisJob job = new ProjectAnalysisJob(id);

		return this.renderTemplate(userContext, job, timeout, maxResults, startIndex, lazy, type, template, false);
	}

	/**
	 * Get a {@link DataTable} as a file.<br>
	 * When requested this resource might require computation of the associated
	 * job.<br>
	 *
	 * @param projectId
	 * @param jobId
	 *            the Job Id
	 * @param ext
	 *            the outpout file extension. Returned file format and
	 *            compression will follow the ext (e.g. .csv or .csv.gz)
	 * @param timeout
	 *            response timeout in milliseconds in case the job is not yet
	 *            computed. If no timeout set, the method will return according
	 *            to current job status.
	 * @param maxResults
	 *            paging size
	 * @param startIndex
	 *            paging start index
	 * @return a DataTable as a Stream or an ErrorResponse if job computing is
	 *         in progress or has failed."
	 */
	@GET
	@Path("{" + PARAM_NAME + "}" + ".{ext}")
	@ApiOperation(value = "Gets an AnalysisJobs' results as a file")
	public Response getResultsAsFile(
			@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String jobId,
			@PathParam("ext") String ext,
			@ApiParam(value = "response timeout in milliseconds in case the job is not yet computed. If no timeout set, the method will return according to current job status.") @QueryParam("timeout") Integer timeout,
			@ApiParam(value = "paging size") @QueryParam("maxResults") Integer maxResults,
			@ApiParam(value = "paging start index") @QueryParam("startIndex") Integer startIndex,
			@ApiParam(value = "if true, get the analysis only if already in cache", defaultValue="false") @QueryParam("lazy") boolean lazy,
			@ApiParam(value = "response media type (optional)") @QueryParam("type") String type,
			@ApiParam(value = "Velocity template as a base64 String (optional)") @QueryParam("template") String template) {
		return returnResultsAsFile(projectId, jobId, ext, timeout, maxResults, 
				startIndex, lazy, type, template, false);
	}


	/**
	 * Post a {@link DataTable} as a file (T458).<br>
	 * When requested this resource might require computation of the associated
	 * job.<br>
	 *
	 * @param projectId
	 * @param jobId
	 *            the Job Id
	 * @param ext
	 *            the outpout file extension. Returned file format and
	 *            compression will follow the ext (e.g. .csv or .csv.gz)
	 * @param timeout
	 *            response timeout in milliseconds in case the job is not yet
	 *            computed. If no timeout set, the method will return according
	 *            to current job status.
	 * @param maxResults
	 *            paging size
	 * @param startIndex
	 *            paging start index
	 * @return a DataTable as a Stream or an ErrorResponse if job computing is
	 *         in progress or has failed."
	 */
	@POST
	@Path("{" + PARAM_NAME + "}" + ".{ext}")
	@ApiOperation(value = "Gets an AnalysisJobs' results as a file")
	public Response postAndGetResultsAsFile(
			@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String jobId,
			@PathParam("ext") String ext,
			@ApiParam(value = "response timeout in milliseconds in case the job is not yet computed. If no timeout set, the method will return according to current job status.") @FormParam("timeout") Integer timeout,
			@ApiParam(value = "paging size") @FormParam("maxResults") Integer maxResults,
			@ApiParam(value = "paging start index") @FormParam("startIndex") Integer startIndex,
			@ApiParam(value = "if true, get the analysis only if already in cache", defaultValue="false") @QueryParam("lazy") boolean lazy,
			@ApiParam(value = "response media type (optional)") @FormParam("type") String type,
			@ApiParam(value = "Velocity template as a base64 String (optional)") @FormParam("template") String template) {
		return returnResultsAsFile(projectId, jobId, ext, timeout, maxResults,
				startIndex,lazy, type, template, true);
	}


	private Response returnResultsAsFile(
			String projectId,
			String jobId,
			String ext,
			Integer timeout,
			Integer maxResults,
			Integer startIndex,
			boolean lazy,
			String type,			
			String template,
			boolean setFileName) {

		if (template == null) {
			// normal rendering
			final ProjectAnalysisJobPK id = new ProjectAnalysisJobPK(
					userContext.getCustomerId(), projectId, jobId);
			final ProjectAnalysisJob job = new ProjectAnalysisJob(id);
			String[] split = ext.split("\\.");
			String format = null;
			if (split.length > 0) {
				format = split[0];
			}
			String compression = null;
			if (split.length > 1) {
				compression = split[1];
				if (compression.equals("gz")) {
					compression = "gzip";
				}
			}
			return getResults(projectId, job, timeout, maxResults, startIndex, lazy,
					format, compression, setFileName);
		} else {
			// template rendering
			final ProjectAnalysisJobPK id = new ProjectAnalysisJobPK(
					userContext.getCustomerId(), projectId, jobId);
			final ProjectAnalysisJob job = new ProjectAnalysisJob(id);
			return this.renderTemplate(userContext, job, timeout, maxResults, startIndex, lazy, type, template, setFileName);
		}
	}

	
	public Response renderTemplate(AppContext ctx, final ProjectAnalysisJob job,
			final Integer timeout, final Integer maxResults, final Integer startIndex,final boolean lazy,
			String type, String template, boolean setFileName) {

		
		final String decoded = new String(Base64.decodeBase64(template
				.getBytes()));

		StreamingOutput stream = new StreamingOutput() {
			@Override
			public void write(OutputStream os) throws IOException,
					WebApplicationException {				
				try {
					ExportSourceWriterVelocity writer = new ExportSourceWriterVelocity(decoded);
					delegate.writeResults(os, userContext, job, 1000, timeout, true, maxResults, startIndex, lazy, null, null, writer);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};

		ResponseBuilder response = Response.ok(stream);
		
		if (setFileName) {
			String fileName = "job-" + job.getId().getAnalysisJobId();
			switch (type) {
			case "text/csv":
				fileName += ".csv";
				break;
			case "application/vnd.ms-excel":
				fileName += ".xls";
				break;
			case "application/html":
				fileName += ".html";
				break;
			case "text/html":
				fileName += ".html";
				break;
			case "text/xml":
				fileName += ".xml";
				break;
			case "application/xml":
				fileName += ".xml";
				break;
			default:
				fileName += ".json";
			}
			logger.info("returning results as " + type + ", fileName : " + fileName);
			response.header("Content-Disposition",
						"attachment; filename=" + fileName);
		}
		return response.type(type).build();
		 
	}
	

	private Response getResults(String projectId, final ProjectAnalysisJob job,
								final Integer timeout, final Integer maxResults,
								final Integer startIndex, final boolean lazy,  String format, 
								String compression, boolean setFileName) {

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
					delegate.writeResults(os, userContext, job, 1000, timeout,
							true, maxResults, startIndex, lazy, outFormat,
							outCompression, null);
				} catch (InterruptedException e) {
					throw new IOException(e);
				}
			}
		};

		// build the response
		ResponseBuilder response;
		String fileName = "job-" + job.getOid();
		String mediaType;
		switch (outFormat) {
			case CSV:
				mediaType = "text/csv";
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
				// note : setting "Content-Type:application/octet-stream" should
				// disable interceptor's GZIP compression.
				mediaType = MediaType.APPLICATION_OCTET_STREAM_TYPE.toString();
				fileName += ".gz";
				break;
			default:
				// NONE
		}

		response = Response.ok(stream);
		response.header("Content-Type", mediaType);
		if (setFileName
				&& ((outFormat != OutputFormat.JSON) || (outCompression != OutputCompression.NONE))) {
			logger.info("returnin results as " + mediaType + ", fileName : "
					+ fileName);
			response.header("Content-Disposition", "attachment; filename="
					+ fileName);
		}

		return response.build();

	}

	/**
	 * JSONP specific method which supports both read operation and store
	 * operation using GET.
	 *
	 * @param projectId
	 * @param dashboardId
	 * @param jobId
	 * @param analysisJobJSON
	 * @param timeout
	 * @return ProjectAnalysisJob
	 */
	@GET
	@Path("{jobId}")
	@ApiOperation(value = "JSONP specific method which supports both read operation and store operation using GET")
	public ProjectAnalysisJob readOrStore(
			@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String jobId,
			@QueryParam("analysisJob") String analysisJobJSON,
			@QueryParam("timeout") Integer timeout) {
		if (analysisJobJSON == null) {
			ProjectAnalysisJob read = delegate.read(userContext,
					new ProjectAnalysisJobPK(userContext.getCustomerId(),
							projectId, jobId));
			return read;
		} else {
			ObjectMapper mapper = new ObjectMapper();
			try {
				ProjectAnalysisJob analysisJob = mapper.readValue(
						analysisJobJSON, ProjectAnalysisJob.class);
				return delegate.store(userContext, analysisJob, timeout);
			} catch (Exception e) {
				throw new WebApplicationException(e);
			}
		}
	}

	/**
	 * Create, update and/or re-compute an {@link ProjectAnalysisJob}.<br>
	 * If a job with the same Id already exist and job is not temporary, update
	 * it and re-compute it (if temporary, just re-compute).<br>
	 * If a Job with the same Id does not exist then create it and compute it.<br>
	 * If a timeout is set, this method will return once the job is done or
	 * until the timeout is reached.
	 *
	 * @param projectId
	 * @param analysisJob
	 *            the job to create / update
	 * @param timeout
	 *            response timeout in milliseconds in case the job is not yet
	 *            computed. If no timeout set, the method will return according
	 *            to current job status (and will not start computation)
	 * @param maxResults
	 *            paging size
	 * @param startIndex
	 *            paging start index
	 * @param format
	 *            an output format : "json", "csv", "xls" (default is "json")
	 * @param compression
	 *            an output compression : "gzip" or "none" (default is "none")
	 * @return a ProjectAnalysisJob or a DataTable or an ErrorResponse if job
	 *         computing is in progress or has failed.<br>
	 *         If format or compression are not json/none then the returned data
	 *         will be set as "Content-Disposition",
	 *         "attachment; filename=+[job-]+jobId.[extension]".
	 */
	@POST
	@Path("")
	@ApiOperation(value = "Create, update and/or re-compute an AnalysisJob")
	public Response store(
			@PathParam("projectId") String projectId,
			@ApiParam(required = true) ProjectAnalysisJob job,
			@QueryParam("timeout") Integer timeout,
			@ApiParam(value = "paging size") @QueryParam("maxResults") Integer maxResults,
			@ApiParam(value = "paging start index") @QueryParam("startIndex") Integer startIndex,
			@ApiParam(value = "if true, get the analysis only if already in cache", defaultValue = "false") @QueryParam("lazy") boolean lazy,
			@ApiParam(value = "output format", allowableValues = "json,csv,vxls", defaultValue = "json") @QueryParam("format") String format,
			@ApiParam(value = "output compression", allowableValues = "gzip, none", defaultValue = "none") @QueryParam("compression") String compression) {

		return createOrUpdate(projectId, job, timeout, maxResults, startIndex, lazy,
				format, compression);
	}

	@PUT
	@Path("")
	@ApiOperation(value = "Create, update and/or re-compute an AnalysisJob")
	public Response put(
			@PathParam("projectId") String projectId,
			@ApiParam(required = true) ProjectAnalysisJob job,
			@QueryParam("timeout") Integer timeout,
			@ApiParam(value = "paging size") @QueryParam("maxResults") Integer maxResults,
			@ApiParam(value = "paging start index") @QueryParam("startIndex") Integer startIndex,
			@ApiParam(value = "if true, get the analysis only if already in cache", defaultValue="false") @QueryParam("lazy") boolean lazy,
			@ApiParam(value = "output format", allowableValues = "json,csv,vxls", defaultValue = "json") @QueryParam("format") String format,
			@ApiParam(value = "output compression", allowableValues = "gzip, none", defaultValue = "none") @QueryParam("compression") String compression) {
		return createOrUpdate(projectId, job, timeout, maxResults, startIndex, lazy,
				format, compression);
	}

	private Response createOrUpdate(String projectId, ProjectAnalysisJob job,
									Integer timeout, Integer maxResults, Integer startIndex,
									boolean lazy,
									String format, String compression) {
		if (job.getId() == null) {
			job.setId(new ProjectAnalysisJobPK(userContext.getCustomerId(),
					projectId, null));
		}

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

		if ((outFormat == OutputFormat.JSON)
				&& (outCompression == OutputCompression.NONE)) {
			// return the analysis
			ProjectAnalysisJob store = delegate.store(userContext, job,
					timeout, maxResults, startIndex, lazy);
			return Response.ok(store).build();
		} else {
			// return the results export
			return getResults(projectId, job, timeout, maxResults, startIndex, lazy,
					format, compression, true);
		}
	}

	@Path("{jobId}/access")
	@GET
	public Set<AccessRight> readAccessRights(
			@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String jobId) {
		return delegate.readAccessRights(userContext, new ProjectAnalysisJobPK(
				userContext.getCustomerId(), projectId, jobId));
	}

	@Path("{jobId}/access")
	@POST
	public Set<AccessRight> storeAccessRights(
			@PathParam("projectId") String projectId,
			@PathParam(PARAM_NAME) String jobId, Set<AccessRight> accessRights) {
		return delegate.storeAccessRights(userContext,
				new ProjectAnalysisJobPK(userContext.getCustomerId(),
						projectId, jobId), accessRights);
	}

}
