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

import java.io.IOException;
import java.io.StringReader;
import java.util.Map.Entry;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Simple pojo to hold the analytics /view query parameters
 * @author sergefantino
 *
 */
public class ViewQuery extends AnalyticsQueryImpl {
	
	private String x;
	
	private String y;
	
	private String color;
	
	private String size;
	
	private String column;
	
	private String row;
	
	private String options;
	
	/**
	 * 
	 */
	public ViewQuery() {
	}
	
	public ViewQuery(AnalyticsQuery query) {
		super(query);
	}
	
	public ViewQuery(ViewQuery query) {
		super(query);
		this.x = query.x;
		this.y = query.y;
		this.color = query.color;
		this.size = query.size;
		this.column = query.column;
		this.row = query.row;
		this.options = query.options;
	}

	public String getX() {
		return x;
	}

	public void setX(String x) {
		this.x = x;
	}

	public String getY() {
		return y;
	}

	public void setY(String y) {
		this.y = y;
	}

	public String getColor() {
		return color;
	}

	public void setColor(String color) {
		this.color = color;
	}

	public String getSize() {
		return size;
	}

	public void setSize(String size) {
		this.size = size;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getRow() {
		return row;
	}

	public void setRow(String row) {
		this.row = row;
	}

	public String getOptions() {
		return options;
	}
	
	public void setOptions(String options) {
		this.options = options;
	}
	
	public boolean hasOptons() {
		return this.options!=null && !this.options.equals("");
	}
	
	@JsonIgnore
	public Properties getOptionsAsProperties() throws IOException {
		Properties properties = new Properties();
		if (hasOptons()) properties.load(new StringReader(options.replaceAll(":","=").replaceAll(";", "\n")));
		return properties;
	}

	
	@JsonIgnore
	public Properties getOptionsAsPropertiesSafe() {
		try {
			return getOptionsAsProperties();
		} catch (IOException e) {
			return new Properties();
		}
	}
	
	@JsonIgnore
	public void setOptionsAsProperties(Properties options) throws IOException {
		StringBuilder writer = new StringBuilder();
		boolean first = true;
		for (Entry<Object, Object> entry : options.entrySet()) {
			if (!first) writer.append(";"); else first = false;
			writer.append(entry.getKey()+":"+entry.getValue());
		}
		this.options = writer.toString();
	}

}
