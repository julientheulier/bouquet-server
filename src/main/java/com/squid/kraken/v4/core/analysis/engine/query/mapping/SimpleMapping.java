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
import com.squid.core.sql.render.IOrderByPiece.ORDERING;
import com.squid.core.sql.render.ISelectPiece;

public class SimpleMapping implements IMapping {
	
	private ISelectPiece piece;
	private int index;
	private int type;

	public SimpleMapping(ISelectPiece piece) {
		super();
		this.piece = piece;
	}

	public ISelectPiece getPiece() {
		return piece;
	}

	public void setPiece(ISelectPiece piece) {
		this.piece = piece;
	}
	
	public void setMetadata(int index, int type) {
		this.index = index;
		this.type = type;
	}

	public int getIndex() {
		return index;
	}

	public int getType() {
		return type;
	}

	public void setMetadata(ResultSet result, ResultSetMetaData metadata) throws SQLException {
		ISelectPiece piece = getPiece();
		int index = result.findColumn(piece.getAlias());
		int type = metadata.getColumnType(index);
		setMetadata(index, type);
	}
	
	@Override
	public Object readData(IJDBCDataFormatter formatter, ResultSet result) throws SQLException {
		Object value = result.getObject(index);
		Object unbox = formatter.unboxJDBCObject(value, type);
		return unbox;
	}

	/**
	 * set the mapping ordering based on the query requirement
	 * Must override to actually propagate the information
	 * @param ordering
	 */
	public void setOrdering(ORDERING ordering) {
		// default do nothing for now, need to propagate
	}
	
}