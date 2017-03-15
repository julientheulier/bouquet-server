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

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.avro.Schema;

import com.squid.core.export.IRawExportSource;
import com.squid.kraken.v4.caching.redis.datastruct.RawRow;
import com.squid.kraken.v4.core.analysis.datamatrix.AxisValues;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.datamatrix.MeasureValues;
import com.squid.kraken.v4.core.analysis.universe.Measure;

public class DataMatrixExportSource implements IRawExportSource{
	
	private Schema schema;

	private DataMatrix matrix; 
	private int columnCount;
	
	private int[] columnTypes;
	
	private String[] columnNames;

	private int[] columnMapping;

	private boolean needReorder;

	public DataMatrixExportSource(DataMatrix matrix){
		this.matrix = matrix;
		this.columnCount= matrix.getKPIs().size() + matrix.getAxes().size();
		
		this.columnTypes= new int[columnCount];
		this.columnNames= new String[columnCount];
		
		this.columnMapping = new int[columnCount];
		this.needReorder = false;// default
		
		if (matrix.getRows().size()>0) {// if the matrix is empty, can't init and don't need it anyway
			int countAxes = matrix.getAxes().size();
			for(int i = 0; i <countAxes ; i++ ){		
				AxisValues av = matrix.getAxes().get(i);
				int originalPos = i;//axesIndirection[i];
				
				this.columnNames[originalPos]=  av.getAxis().getName();
						//matrix.getPropertyToAlias().get(av.getAxis()) ;
				this.columnTypes[originalPos] = matrix.getPropertyToInteger().get(av.getAxis());	
				
				// so we need the indirection
				this.columnMapping[originalPos] = matrix.getAxisIndirection(i);
				if (this.columnMapping[originalPos]!=originalPos) {
					this.needReorder = true;
				}
			}		
			for (int i = 0; i <matrix.getKPIs().size() ; i++ ){		
				MeasureValues mv = matrix.getKPIs().get(i);
				Measure m = mv.getMeasure();
				int originalPos = i+countAxes;//dataIndirection[i];
				this.columnNames[originalPos] = m.getName();
						//matrix.getPropertyToAlias().get(av) ;
				this.columnTypes[originalPos] = matrix.getPropertyToInteger().get(m);	
				
				// so we need the indirection
				this.columnMapping[originalPos] = matrix.getDataIndirection(i);
				if (this.columnMapping[originalPos]!=originalPos) {
					this.needReorder = true;
				}
			}
			
			if (columnCount<matrix.getRowSize()) {
				this.needReorder = true;
			}
		}
	}
	
	@Override
	public int getNumberOfColumns() {
		return this.columnCount;
	}

	@Override
	public String getColumnName(int pos) {
		return this.columnNames[pos];
	}

	@Override
	public int getColumnType(int pos) {
		return this.columnTypes[pos];
	}

	@Override
	public int[] getColumnTypes() {
		return this.columnTypes;
	}

	@Override
	public String[] getColumnNames() {
		return this.columnNames;
	}

	@Override
	public Iterator<Object[]> iterator() {
		return new RowIterator();
	}

	@Override
	public Schema getSchema() {
		if(schema==null) {
			this.schema = SchemaAvro.constructAvroSchema(matrix.getRedisKey().replace("-", ""), columnTypes, columnNames);
		}
		return schema;
	}

	private class RowIterator implements Iterator< Object[]>{
		private int cursor ;
		
		public RowIterator(){
			this.cursor = 0;
		}
		
		@Override
		public boolean hasNext() {
			return cursor < matrix.getRows().size() ;
		}

		@Override
		public Object[] next() {
			if (this.hasNext()){
				RawRow row = matrix.getRows().get(cursor++) ;
				Object[] data = row.data;
				if (needReorder) {
					// reorder
					Object[] reorder = new Object[columnCount];
					for (int i=0;i<columnCount;i++) {
						reorder[i] = data[columnMapping[i]];
					}
					return reorder;
				} else {
					return data;
				}
			}else{
				throw new NoSuchElementException();
			}			
		}

		@Override
		public void remove() {
            throw new UnsupportedOperationException();						
		}
		
	}

}
