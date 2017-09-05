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
package com.squid.kraken.v4.api.core.nlu;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.squid.kraken.v4.vegalite.VegaliteSpecs;

/**
 * @author sergefantino
 *
 */
public class CardInfo implements Serializable {
	
	enum Status { VALID, INCOMPLETE, ERROR}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -122869605941810955L;
	
	private String message;
	
	private Object parserOutput = null;
	
	private Throwable error = null;
	
	private List<String> followUp = new ArrayList<>();
	
	private VegaliteSpecs dataviz = null;
	
	private String state = null;
	
	private Status status = Status.VALID;
	
	public static final CardInfo valid(String message) {
		return new CardInfo(message);
	}
	
	public static final CardInfo incomplete(String message) {
		return new CardInfo(message, Status.INCOMPLETE);
	}
	
	/**
	 * 
	 */
	public CardInfo(String message) {
		this.message = message;
	}
	
	/**
	 * 
	 */
	public CardInfo(String message, Status status) {
		this.message = message;
		this.status = status;
	}
	
	/**
	 * @return the status
	 */
	public Status getStatus() {
		return status;
	}
	
	/**
	 * @return the message
	 */
	public String getMessage() {
		return message;
	}
	
	/**
	 * @param message the message to set
	 */
	public void setMessage(String message) {
		this.message = message;
	}
	
	/**
	 * @return the parserOutput
	 */
	public Object getParserOutput() {
		return parserOutput;
	}
	
	/**
	 * @param parserOutput the parserOutput to set
	 */
	public void setParserOutput(Object parserOutput) {
		this.parserOutput = parserOutput;
	}
	
	/**
	 * @return the error
	 */
	public Throwable getError() {
		return error;
	}
	
	/**
	 * @param error the error to set
	 */
	public void setError(Throwable error) {
		this.error = error;
	}

	/**
	 * 
	 */
	public void addFollowUp(String message) {
		this.followUp.add(message);
	}
	
	/**
	 * @param followUp the followUp to set
	 */
	public void setFollowUp(List<String> followUp) {
		this.followUp = followUp;
	}
	
	/**
	 * @return the followUp
	 */
	public List<String> getFollowUp() {
		return followUp;
	}
	
	/**
	 * @return the dataviz
	 */
	public VegaliteSpecs getDataviz() {
		return dataviz;
	}
	
	/**
	 * @param dataviz the dataviz to set
	 */
	public void setDataviz(VegaliteSpecs dataviz) {
		this.dataviz = dataviz;
	}
	
	/**
	 * @return the state
	 */
	public String getState() {
		return state;
	}

	/**
	 * @param state the state to set
	 */
	public void setState(String state) {
		this.state = state;
	}
	
}
