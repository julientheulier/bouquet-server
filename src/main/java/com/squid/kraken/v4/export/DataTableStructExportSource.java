package com.squid.kraken.v4.export;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.squid.core.export.ICol;
import com.squid.core.export.IRow;
import com.squid.core.export.IStructExportSource;
import com.squid.kraken.v4.model.DataTable;
import com.squid.kraken.v4.model.DataTable.Col;
import com.squid.kraken.v4.model.DataTable.Row;

public class DataTableStructExportSource implements IStructExportSource {

	
	private ArrayList<ICol> cols;
	private WrappedRows rows;
	
	public DataTableStructExportSource(){		
	}

	public DataTableStructExportSource(DataTable source){
		this.cols = new ArrayList<ICol> ();
		for (Col c : source.getCols()){
			cols.add(new WrappedCol(c));
		}		
		this.rows= new WrappedRows(source.getRows());
	}

	public void setColumns(ArrayList<ICol> columns) {
		this.cols = columns;
	}

	public void setRows(WrappedRows rows) {
		this.rows = rows;
	}
	@Override
	public Iterable<IRow> getRows() {
		return this.rows;
	}


	@Override
	public List<ICol> getCols() {
		return this.cols;
	}
	
	public class WrappedCol implements ICol, Serializable {

		private Col column;
		
		public WrappedCol(Col c){
			this.column = c;
		}
		
		@Override
		public String getRole() {
			return column.getRole().name();
		}

		@Override
		public String getName() {
			return column.getName();
		}

		@Override
		public Object getPk() {
			return column.getPk();
		}
	}

	
	
	public class WrappedRow implements IRow, Serializable {

		private Row r;
		
		public Row getR() {
			return r;
		}

		public void setR(Row r) {
			this.r = r;
		}

		public WrappedRow(){
			
		}
		
		public WrappedRow(Row r){
			this.r = r;
		}
		
		@Override
		public String[] getV() {
			return r.getV();			
		}		
	}
	
	public class WrappedRows implements Iterable<IRow>, Serializable{
		
		private List<Row> rows;
		
		public List<Row> getRows() {
			return rows;
		}

		public void setRows(List<Row> rows) {
			this.rows = rows;
		}

		public WrappedRows(){
			
		}
		
		public WrappedRows (List<Row>  rows){
			this.rows = rows;
		}

		@Override
		public Iterator<IRow> iterator() {
			return new RowIterator(rows);			
		}
		
		private class RowIterator implements Iterator<IRow>{
			private Iterator<Row> innerIter;		
						
			public RowIterator(List<Row> r){
				innerIter = r.iterator();
			}
			
			@Override
			public boolean hasNext() {
				return innerIter.hasNext();
			}

			@Override
			public IRow next() {
				Row r = innerIter.next();
				if (r!= null){
					return new WrappedRow(r);
				}else{
					return null;					
				}
			}

			@Override
			public void remove() {
	            throw new UnsupportedOperationException();							
			}			
		}
		
	}
	
}
