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

import com.google.common.primitives.Ints;
import com.squid.kraken.v4.caching.awsredis.datastruct.RawMatrix;
import org.apache.avro.Schema;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.squid.core.export.IRawExportSource;

public class RawMatrixExportSource implements IRawExportSource {
	

	private Schema schema;
	private RawMatrix matrix ;

	public RawMatrixExportSource(RawMatrix matrix){
		this.matrix = matrix ;


	}

	@Override
	public int getNumberOfColumns() {
		return matrix.getColNames().size();
	}

	@Override
	public String getColumnName(int pos) {
		return matrix.getColNames().get(pos).toLowerCase();
	}

	@Override
	public int getColumnType(int pos) {
		return matrix.getColTypes().get(pos);
	}

	@Override
	public 	Iterator<Object[]> iterator() {
		// TODO Auto-generated method stub
		return new RowIterator();
	}

	@Override
	public Schema getSchema() {
		if(schema==null) {
			this.schema = SchemaAvro.constructAvroSchema(matrix.getRedisKey(), Ints.toArray(matrix.getColTypes()), matrix.getColNames().toArray(new String[matrix.getColNames().size()]));
		}
		return schema;
	}

	private class RowIterator implements Iterator<Object[]>{

		private int cursor;
		
		@Override
		public boolean hasNext() {
			return cursor< matrix.getRows().size() ; 
		}

		@Override
		public Object[] next() {
			 if(this.hasNext()) {
				 Object[] res =matrix.getRows().get(cursor).getData();
				 cursor+=1;
				 return res;
			}else{
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
		int[] res = new int[matrix.getColTypes().size()] ;
		for (int i = 0; i < matrix.getColTypes().size(); i++){
				res[i] = matrix.getColTypes().get(i).intValue() ;
		}
		return res ;
	}

	@Override
	public String[] getColumnNames() {
		return (String[]) matrix.getColNames().toArray();
		
	} 
	
}
