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
import com.squid.kraken.v4.core.analysis.engine.query.mapping.AxisMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.MeasureMapping;

import org.apache.avro.Schema;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.squid.core.export.IRawExportSource;

public class ExecutionItemExportSource implements IRawExportSource {
	
	private Schema schema;
	
	private int columnCount; // number of actual column in the resultset
	
	// mapping info
	private int[] mappingTypes ;
	private String[] mappingNames ;
	private int mappingCount ;
	private HashMap<Integer, Integer> mappingOrder;
	private ResultSet rs ;
	
	public ExecutionItemExportSource(ExecuteAnalysisResult result,  CSVSettingsBean settings) throws SQLException{
		
		IExecutionItem item = result.getItem();
		rs = item.getResultSet();

		ResultSetMetaData metadata = rs.getMetaData();
		
		this.columnCount = metadata.getColumnCount();
		
		this.mappingCount = result.getMapper().size();
		this.mappingNames = new String[mappingCount];
		this.mappingTypes = new int[mappingCount];
		this.mappingOrder = new HashMap<>();
		
		// reverse SQL column mapping
		HashMap<String, Integer> positions = new HashMap<>();
		for (int i = 0; i < columnCount; i++) {
			String columnName = metadata.getColumnName(i + 1);
			positions.put(columnName, i);// use zero-base index
		}
		// map axis
		int position = 0;
		for (AxisMapping map : result.getMapper().getAxisMapping()) {
			// look for actual name and position
			String columnName = map.getPiece().getAlias();
			Integer lookup = positions.get(columnName);
			if (lookup==null) {
				throw new SQLException("cannot map "+map.getAxis().toString()+" from resultset");
			}
			mappingNames[position] = map.getAxis().getName();
			mappingOrder.put(lookup, position);
			position++;
		}
		// map measures
		for (MeasureMapping map : result.getMapper().getMeasureMapping()) {
			// look for actual name and position
			String columnName = map.getPiece().getAlias();
			Integer lookup = positions.get(columnName);
			if (lookup==null) {
				throw new SQLException("cannot map "+map.getMapping().toString()+" from resultset");
			}
			mappingNames[position] = map.getMapping().getName();
			mappingOrder.put(lookup, position);
			position++;
		}
	
		IVendorSupport vendorSpecific = VendorSupportRegistry.INSTANCE
				.getVendorSupport(item.getDatabase());
		int[] rawTypes = vendorSpecific
					.getVendorMetadataSupport().normalizeColumnType(rs);
		for (int i=0;i<columnCount;i++) {
			if (mappingOrder.containsKey(i)) {
				int j = mappingOrder.get(i);
				mappingTypes[j] = rawTypes[i];
			}
		}
	}

	@Override
	public Schema getSchema() {
		if(schema==null) {
			this.schema = SchemaAvro.constructAvroSchema("Name", mappingTypes, mappingNames);
		}
		return schema;
	}


	@Override
	public int getNumberOfColumns() {
		return this.mappingCount;
	}

	@Override
	public String getColumnName(int pos) {
		return this.mappingNames[pos].toLowerCase();
	}

	@Override
	public int getColumnType(int pos) {
		if (pos < mappingCount){
			return this.mappingTypes[pos];
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

		boolean hasMore;
		
		public RowIterator() throws SQLException{
			hasMore = rs.next() ;
			
		}
		
		@Override
		public boolean hasNext() {
			return hasMore;
		}

		@Override
		public Object[] next() {
			try {
				if (hasMore){				
					Object[] nextLine = new Object[mappingCount];
					for (int i = 0; i < columnCount; i++) {
						if (mappingOrder.containsKey(i)) {
							int j = mappingOrder.get(i);
							Object value = rs.getObject(i+1);
							nextLine[j] = value;
						} else {
							// 
						}
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
		return this.mappingTypes;
	}

	@Override
	public String[] getColumnNames() {
		return this.mappingNames;
	}

}
