package com.squid.kraken.v4.export;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.export.ICol;
import com.squid.core.export.IRow;
import com.squid.core.export.IStructExportSource;
import com.squid.core.jdbc.engine.IExecutionItem;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.AxisMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.MeasureMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.QueryMapper;
import com.squid.kraken.v4.core.analysis.universe.Axis;

public class ExecutionItemStructExportSource implements IStructExportSource {
	
	private ResultSet rs ;
	private QueryMapper queryMapper;
	
	private int[] indirectionRow;
	private ArrayList<ICol> cols;
	private int nbColsSource;
	private int nbColsResult;
	
	static final Logger logger = LoggerFactory
			.getLogger(ExecutionItemStructExportSource.class);
	
	public ExecutionItemStructExportSource(ExecuteAnalysisResult r) throws ComputingException, SQLException{
		this(r.getItem(), r.getMapper());		
	}
	
		
	public ExecutionItemStructExportSource(IExecutionItem source, QueryMapper qm) throws ComputingException, SQLException{
		this.cols = new ArrayList<ICol>();
		
		this.queryMapper = qm;
			
		
		this.rs = source.getResultSet();
		ResultSetMetaData metadata = rs.getMetaData();

		this.nbColsSource =metadata.getColumnCount();
		this.nbColsResult = queryMapper.getAxisMapping().size() + queryMapper.getMeasureMapping().size();
		

		
		this.indirectionRow = new int[this.nbColsSource];
		ArrayList<String> columnNames= new ArrayList<String>();
		for (int i =0; i<nbColsSource; i++){
			columnNames.add(metadata.getColumnName(i+1));
			this.indirectionRow[i] = -1;
		}
		
		//logger.info("columns names : " + columnNames.toString());
		
		int j = 0;
		for (AxisMapping am : queryMapper.getAxisMapping()){
			int index = columnNames.indexOf(am.getPiece().getAlias());
			this.indirectionRow[index]= j ;
			j++;
			Axis a= am.getAxis();
			Col next ; 
			if (a.getDimension() != null){
				next = new Col(am.getAxis().getName(), false, am.getAxis().getDimension().getId()) ;
			}else{
				next = new Col(am.getAxis().getName(), false, null) ;
			}
		
			this.cols.add(next );					

		}
		
		for (MeasureMapping m : queryMapper.getMeasureMapping()){
			
			int index = columnNames.indexOf(m.getPiece().getAlias());
			this.indirectionRow[index]= j ;
			j++;
			Col next;
			if(m.getMapping().getMetric()!=null){
			 next =new Col(m.getMapping().getName() , true, m.getMapping().getMetric().getId());
			}else{				
				next =new Col(m.getMapping().getName() , true, null);
			}	
			this.cols.add( next );
		}
		logger.debug("nb cols " + this.nbColsSource + " nbmappings "+ nbColsResult);
		logger.debug(cols.toString()) ;
		String indirStr = "";
		for (int i = 0; i < indirectionRow.length; i++){
			indirStr += indirectionRow[i] + " ";
		}
		logger.debug(indirStr);
	}

	@Override
	public Iterable<IRow> getRows() {
		return new WrappedRows();
	}

	@Override
	public ArrayList<ICol> getCols() {
		return this.cols;
	}

	public class Col implements ICol{

		private boolean isData;
		private String name;
		private Object pk;
		
		public Col(String name, boolean isData, Object pk){
			this.name = name ;
			this.isData= isData;
			this.pk = pk;
		}
		public String toString(){
			return this.name+" " +this.getRole() ;
			
		}
		
		@Override
		public String getRole() {
			if(isData){
				return "DATA";
			}
			else{
				return "DOMAIN";
			}
		}

		@Override
		public String getName() {
			return this.name;
		}

		@Override
		public Object getPk() {
			return this.pk;
		}
		

	}
	
	
	public class Row implements IRow{
				
		private String[] values;
		
		public Row(Object[] raw){
			this.values= new String[nbColsResult];			
			for (int i = 0; i<raw.length; i++){
				if (indirectionRow[i] != -1){
					Object o = raw[i];
					if (o == null){
						values[indirectionRow[i]]="";
					}else{
						values[indirectionRow[i]]= o.toString();
					}
				}
			}
			
		}

		@Override
		public String[] getV() {
			return this.values;
		}
		
		
	} 
	
	
	public class WrappedRows implements Iterable<IRow>{
		

		@Override
		public RowIterator iterator() {
			try {
				return new RowIterator();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
		
		private class RowIterator implements Iterator<IRow>{

			boolean hasMore = true ;
			int count;
			public RowIterator() throws SQLException{
				hasMore=rs.next();
				count =0;
			}
			
			@Override
			public boolean hasNext() {
				return hasMore;
			}
	
			
			@Override
			public IRow next() {
				try {
					if (hasMore){	
						if (count%10000 == 0) {
						logger.info(count +" lines processed");
						}
						count+=1;
						Object[] nextLine = new Object[nbColsSource];
						for (int i = 0; i < nbColsSource; i++) {
							Object value = rs.getObject(i+1);
							nextLine[i] = value;
						}
						hasMore= rs.next() ;

						return  new Row(nextLine);
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
	}
}
