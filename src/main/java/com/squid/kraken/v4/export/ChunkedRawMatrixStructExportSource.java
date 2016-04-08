package com.squid.kraken.v4.export;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.export.ICol;
import com.squid.core.export.IRow;
import com.squid.core.export.IStructExportSource;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.AxisMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.MeasureMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.QueryMapper;
import com.squid.kraken.v4.core.analysis.universe.Axis;

public class ChunkedRawMatrixStructExportSource extends ChunkedRawMatrixBaseSource implements IStructExportSource  {

	private int[] indirectionRow;
	int nbColsSource ;
	int nbColsResult;


	private QueryMapper mapper;

	static final Logger logger = LoggerFactory.getLogger(ChunkedRawMatrixStructExportSource.class);

	private ArrayList<ICol> cols;


	public ChunkedRawMatrixStructExportSource(RedisCacheValuesList refList, QueryMapper qm) throws InterruptedException, ExecutionException{		
		super(refList);		
		this.mapper = qm;
		this.cols = new ArrayList<ICol>();
		
		this.nbColsSource =this.currentChunk.getColTypes().size();
		this.nbColsResult = mapper.getAxisMapping().size() + mapper.getMeasureMapping().size();

		this.indirectionRow = new int[this.nbColsSource];
		ArrayList<String> columnNames= new ArrayList<String>();
		for (int i =0; i<nbColsSource; i++){
			columnNames.add(this.currentChunk.getColNames().get(i));
			this.indirectionRow[i] = -1;
		}

		int j = 0;
		for (AxisMapping am : mapper.getAxisMapping()){
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

		for (MeasureMapping m : mapper.getMeasureMapping()){

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
	public List<ICol> getCols() {
		return cols;
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
				e.printStackTrace();
				return null;
			}
		}

		private class RowIterator implements Iterator<IRow>{

			boolean done  = false ;
			int count = 0;
			int cursor = 0;

			public RowIterator() throws SQLException{
			}

			@Override
			public boolean hasNext() {
				return !done;
			}


			@Override
			public IRow next() {
				if (count%10000 == 0) {
					logger.info(count +" lines processed");
				}
				count+=1;
				Object[] rr = currentChunk.getRows().get(cursor).getData();
				cursor++;

				if (cursor>= currentChunk.getRows().size()){
					// get next Chunk
					RawMatrix next;
					try {
						next = processingQuery.get();
					} catch (InterruptedException | ExecutionException e) {
						logger.info("error");
						return null;
					}
					if (next == null){
						done = true;
						return null;
					}else{
						currentChunk = next;
						cursor =0 ;
						// launch chunk retrieval in parallel ;
						GetChunk getNextChunk = new GetChunk();
						processingQuery = (Future<RawMatrix>) executor.submit(getNextChunk);
					}				 
				}
				return  new Row(rr);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();				
			}


		}
	}


}
