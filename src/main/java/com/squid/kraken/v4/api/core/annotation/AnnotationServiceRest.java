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
package com.squid.kraken.v4.api.core.annotation;

import java.util.Set;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.squid.kraken.v4.api.core.BaseServiceRest;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.Annotation;
import com.squid.kraken.v4.model.AnnotationPK;
import com.squid.kraken.v4.persistence.AppContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;

/**
 * Contains WS path to manage annotations.
 * 
 * @author danhddv
 * 
 */
@Api(
		value = "annotations", 
		hidden = true,
		authorizations = { @Authorization(value = "kraken_auth", scopes = { @AuthorizationScope(scope = "access", description = "Access") }) })
@Produces( { MediaType.APPLICATION_JSON })
public class AnnotationServiceRest extends BaseServiceRest {

    private final static String ANNOTATION_PARAM_NAME = "annotationId";

    private final static String PROJECT_PARAM_NAME = "projectId";

    private AnnotationServiceBaseImpl delegate = AnnotationServiceBaseImpl.getInstance();

    public AnnotationServiceRest(AppContext userContext) {
        super(userContext);
    }

    /**
     * Read an annotation.
     * 
     * @param projectId
     *            project id
     * @param annotationId
     *            annotation id
     * @return annotation object
     */
    @GET
    public Annotation read(@PathParam(PROJECT_PARAM_NAME) String projectId,
            @PathParam(ANNOTATION_PARAM_NAME) String annotationId) {
        return delegate.read(userContext, new AnnotationPK(userContext.getCustomerId(), projectId, annotationId));
    }

    /**
     * Update an annotation.
     * 
     * @param projectId
     *            project id
     * @param annotationId
     *            annotation id
     * @param annotation
     *            annotation id
     * @return updated annotation
     */
    @PUT
    public Annotation store(@PathParam(PROJECT_PARAM_NAME) String projectId,
            @PathParam(ANNOTATION_PARAM_NAME) String annotationId, Annotation annotation) {
        return delegate.store(userContext, annotation);
    }

    /**
     * Delete an annotation.
     * 
     * @param projectId
     *            project id
     * @param annotationId
     *            annotation id
     * @return true --> delete ok, otherwise false
     */
    @DELETE
    public boolean delete(@PathParam(PROJECT_PARAM_NAME) String projectId,
            @PathParam(ANNOTATION_PARAM_NAME) String annotationId) {
        return delegate.delete (userContext, new AnnotationPK(userContext.getCustomerId(), projectId, annotationId));
    }
    
    @Path("/access")
    @GET
    public Set<AccessRight> readAccessRights(@PathParam("projectId") String projectId,
            @PathParam(ANNOTATION_PARAM_NAME) String annotationId) {
        return delegate.readAccessRights(userContext,
                new AnnotationPK(userContext.getCustomerId(), projectId, annotationId));
    }

    @Path("/access")
    @POST
    public Set<AccessRight> storeAccessRights(@PathParam("projectId") String projectId,
            @PathParam(ANNOTATION_PARAM_NAME) String annotationId, Set<AccessRight> accessRights) {
        return delegate.storeAccessRights(userContext, new AnnotationPK(userContext.getCustomerId(), projectId,
                annotationId), accessRights);
    }
}
