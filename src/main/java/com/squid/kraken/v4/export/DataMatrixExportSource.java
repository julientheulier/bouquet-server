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

import com.squid.core.export.IRawExportSource;

import com.squid.kraken.v4.core.analysis.datamatrix.AxisValues;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.datamatrix.IndirectionRow;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import org.apache.avro.Schema;

public class DataMatrixExportSource implements IRawExportSource{
	
	private Schema schema;

	private DataMatrix matrix; 
	private int columnCount;
	
	private int[] columnTypes;
	
	private String[] columnNames;


	public DataMatrixExportSource(DataMatrix matrix){
		this.matrix = matrix;
		this.columnCount= matrix.getKPIs().size() + matrix.getAxes().size();
		
		this.columnTypes= new int[columnCount];
		this.columnNames= new String[columnCount];
		
		int[] axesIndirection = matrix.getRows().get(0).getAxesIndirection();	
		for(int i = 0; i <matrix.getAxes().size() ; i++ ){		
			AxisValues av = matrix.getAxes().get(i);
			int originalPos = axesIndirection[i];
			
			this.columnNames[originalPos]=  av.getAxis().getName();
					//matrix.getPropertyToAlias().get(av.getAxis()) ;
			this.columnTypes[originalPos] = matrix.getPropertyToInteger().get(av.getAxis());			
		}		
		
		int[] dataIndirection = matrix.getRows().get(0).getDataIndirection();
		for(int i = 0; i <matrix.getKPIs().size() ; i++ ){		
			Measure av = matrix.getKPIs().get(i);
			int originalPos = dataIndirection[i];
			this.columnNames[originalPos] = av.getName();
					//matrix.getPropertyToAlias().get(av) ;
			this.columnTypes[originalPos] = matrix.getPropertyToInteger().get(av);			
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
				IndirectionRow ir = matrix.getRows().get(cursor) ;
				cursor+=1;				
				return ir.getRawRow();			
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
