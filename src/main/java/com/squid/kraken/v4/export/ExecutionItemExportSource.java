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
package com.squid.kraken.v4.export;

import com.squid.core.csv.CSVSettingsBean;
import com.squid.core.jdbc.engine.IExecutionItem;
import com.squid.core.jdbc.vendor.IVendorSupport;
import com.squid.core.jdbc.vendor.VendorSupportRegistry;
import org.apache.avro.Schema;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.squid.core.export.IRawExportSource;

public class ExecutionItemExportSource implements IRawExportSource {
	

	private Schema schema;
	private int[] types ;
	
	private String[] columnNames ;
	
	private int columnCount ;
	private ResultSet rs ;
	
	public ExecutionItemExportSource(IExecutionItem item,  CSVSettingsBean settings) throws SQLException{
		
		rs = item.getResultSet();

		ResultSetMetaData metadata = rs.getMetaData();
		
		this.columnCount =  metadata.getColumnCount();
		
		this.columnNames = new String[columnCount];
		for (int i = 0; i < columnCount; i++) {
			columnNames[i] = metadata.getColumnName(i + 1);
		}
	
		IVendorSupport vendorSpecific = VendorSupportRegistry.INSTANCE
				.getVendorSupport(item.getDatabase());
		this.types = vendorSpecific
					.getVendorMetadataSupport().normalizeColumnType(rs);



	}

	@Override
	public Schema getSchema() {
		if(schema==null) {
			this.schema = SchemaAvro.constructAvroSchema("Name", types, columnNames);
		}
		return schema;
	}


	@Override
	public int getNumberOfColumns() {
		return this.columnCount;
	}

	@Override
	public String getColumnName(int pos) {
		return this.columnNames[pos].toLowerCase();
	}

	@Override
	public int getColumnType(int pos) {
		if (pos < columnCount){
			return this.types[pos];
		}else{
			return -1;
		}
	}
		

	@Override
	public Iterator<Object[]> iterator() {
		try {
			return new RowIterator();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	
	private class RowIterator implements Iterator<Object[]>{

		boolean hasMore ;
		
		public RowIterator() throws SQLException{
			hasMore= rs.next() ;
		}
		
		@Override
		public boolean hasNext() {
			return hasMore;
		}

		@Override
		public Object[] next() {
			try {
				if (hasMore){				
					Object[] nextLine = new Object[columnCount];
					for (int i = 0; i < columnCount; i++) {
						Object value = rs.getObject(i+1);
						nextLine[i] = value;
					}
					hasMore= rs.next() ;
					return nextLine;
				}else{
					throw new NoSuchElementException();
				}
			} catch (SQLException e) {
				e.printStackTrace();
				throw new NoSuchElementException();
			}

		}

		@Override
		public void remove() {
            throw new UnsupportedOperationException();			
		}		
	}


	@Override
	public int[] getColumnTypes() {
		return this.types;
	}

	@Override
	public String[] getColumnNames() {
		return this.columnNames;
	}

}
