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
package com.squid.kraken.v4.core.analysis.engine.query.mapping;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.squid.core.jdbc.formatter.IJDBCDataFormatter;

/**
 * define a generic interface for mapping object.
 * <br>
 * The main goal is to make the mapping of ONE concept independant of the number of actual SQL pieces needed to fullfil it...
 * @author sfantino
 *
 */
public interface IMapping {
	
	/**
	 * take care of extracting metadata information from the resultSet metaData in order to speed up reading process
	 * @param result
	 * @param metadata
	 * @throws SQLException
	 */
	public void setMetadata(ResultSet result, ResultSetMetaData metadata) throws SQLException;
	
	/**
	 * Actually read the data from the resultSet. Must call setMetadata first. 
	 * @param result
	 * @throws SQLException
	 */
	public Object readData(IJDBCDataFormatter formatter, ResultSet result) throws SQLException;
	
}