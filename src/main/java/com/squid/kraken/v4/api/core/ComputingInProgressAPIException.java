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
package com.squid.kraken.v4.api.core;

import java.net.URI;

@SuppressWarnings("serial")
public class ComputingInProgressAPIException extends APIException {
	
	private final Integer retryAfter;
	
	private final String queryID;
	
	private final URI statusLink;

	/**
	 * @param message
	 * @param noError
	 * @param retryAfter see https://tools.ietf.org/html/rfc2616#section-14.37
	 */
    public ComputingInProgressAPIException(String message, boolean noError, Integer retryAfter) {
        super(message, noError, ApiError.COMPUTING_IN_PROGRESS);
        this.retryAfter = retryAfter;
        this.queryID = null;
        this.statusLink = null;
    }

    public ComputingInProgressAPIException(String message, boolean noError, Integer retryAfter, String queryID) {
        super(message, noError, ApiError.COMPUTING_IN_PROGRESS);
        this.retryAfter = retryAfter;
        this.queryID = queryID;
        this.statusLink = null;
    }

    public ComputingInProgressAPIException(String message, boolean noError, Integer retryAfter, String queryID, URI statusLink) {
        super(message, noError, ApiError.COMPUTING_IN_PROGRESS);
        this.retryAfter = retryAfter;
        this.queryID = queryID;
        this.statusLink = statusLink;
    }

	@Override
    protected Integer getErrorCode() {
    	return 200;
    }

	public Integer getRetryAfter() {
		return retryAfter;
	}
	
	/**
	 * return the queryId of the job in progress
	 */
	public String getQueryID() {
		return queryID;
	}
	
	/**
	 * return the link to the status API that can provide information regarding the query identified by its queryID
	 */
	public URI getStatusLink() {
		return statusLink;
	}

}
