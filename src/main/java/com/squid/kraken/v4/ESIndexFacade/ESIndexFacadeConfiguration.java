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
package com.squid.kraken.v4.ESIndexFacade;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class ESIndexFacadeConfiguration {

	private boolean embedded;
	private String clustername;

	public ESIndexFacadeConfiguration() {
		this.embedded = true;
	}

	public ESIndexFacadeConfiguration(boolean embedded, String clustername) {
		this.embedded = embedded;
		this.clustername = clustername;
	}

	public boolean isEmbedded() {
		return embedded;
	}

	public void setEmbedded(boolean embedded) {
		this.embedded = embedded;
	}

	public String getClustername() {
		return clustername;
	}

	public void setClustername(String clustername) {
		this.clustername = clustername;
	}

	public String toJson() throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		String res = mapper.enable(SerializationFeature.INDENT_OUTPUT)
				.writeValueAsString(this);
		return res;
	}

	public String toString() {
		String res = this.clustername + " ";
		if (this.embedded) {
			res += "embedded";
		} else {
			res += "not embedded";
		}
		return res;

	}

	public static ESIndexFacadeConfiguration fromJson(String json) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.readValue(json, ESIndexFacadeConfiguration.class);
		} catch (IOException e) {
			return null;
		}
	}

}
