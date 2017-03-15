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

/**
 * @author sergefantino
 *
 */
public class Problem {
	
	public enum Severity {
		WARNING, ERROR
	}
	
	private Severity severity;
	
	private String subject;
	
	private String message;
	
	private Throwable error;

	public Problem(Severity severity, String subject, String message) {
		super();
		this.severity = severity;
		this.subject = subject;
		this.message = message;
	}

	public Problem(Severity severity, String subject, String message, Throwable error) {
		super();
		this.severity = severity;
		this.subject = subject;
		this.message = message;
		this.error = error;
	}

	public Severity getSeverity() {
		return severity;
	}

	public String getSubject() {
		return subject;
	}
	
	public String getMessage() {
		return message;
	}
	
	public Throwable getError() {
		return error;
	}

}
