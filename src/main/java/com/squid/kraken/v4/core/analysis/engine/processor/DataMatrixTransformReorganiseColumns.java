package com.squid.kraken.v4.core.analysis.engine.processor;

import java.util.ArrayList;
import java.util.List;

import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.datamatrix.AxisValues;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.datamatrix.MeasureValues;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.AxisMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.MeasureMapping;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.QueryMapper;

public class DataMatrixTransformReorganiseColumns implements DataMatrixTransform {

	QueryMapper destqm ;
	QueryMapper srcqm ;
	
	private int[] axesDestToSrc;
	private int[] measuresDestToSrc;
	
	
	public DataMatrixTransformReorganiseColumns( QueryMapper srcQueryMapper, QueryMapper destQueryMapper ){
		this.destqm = destQueryMapper;
		this.srcqm = srcQueryMapper;
		axesDestToSrc= new int[destQueryMapper.getAxisMapping().size()] ;
		for (int i=0; i<destQueryMapper.getAxisMapping().size(); i++){
			AxisMapping destAM = destQueryMapper.getAxisMapping().get(i);
			for (int j =0; j<srcQueryMapper.getAxisMapping().size(); j++){
				AxisMapping srcAM = srcQueryMapper.getAxisMapping().get(j);
				if (destAM.getAxis().equals(srcAM.getAxis())){
					axesDestToSrc[i]  = j;
					break; 					
				}
			}	
		}
		
		measuresDestToSrc= new int[destQueryMapper.getMeasureMapping().size()] ;

		for (int i =0; i<destQueryMapper.getMeasureMapping().size(); i++){
			MeasureMapping destMM = destQueryMapper.getMeasureMapping().get(i);
			for (int j =0; j<srcQueryMapper.getMeasureMapping().size(); j++){
				MeasureMapping srcMM  =srcQueryMapper.getMeasureMapping().get(j);
				if (destMM.getMapping().getMetric().equals(srcMM.getMapping().getMetric())){
					measuresDestToSrc[i]  = j;
					break; 					
				}
			}	
		}
				
	}
	
	
	@Override
	public DataMatrix apply(DataMatrix input) throws ScopeException {
				
	
		int[] newAxesIndirection = new int[axesDestToSrc.length];
		int[] newDataIndirection = new int[measuresDestToSrc.length];
		List<MeasureValues> newMeasures = new ArrayList<MeasureValues>();
		List<AxisValues> newAxes = new ArrayList<AxisValues>();

		for(int j=0; j<axesDestToSrc.length; j++ ){
			newAxesIndirection[j]= input.getAxesIndirection()[axesDestToSrc[j]];
			newAxes.add(input.getAxes().get(axesDestToSrc[j]));
		}
			
		for(int j=0; j<measuresDestToSrc.length; j++ ){
			newDataIndirection[j]= input.getDataIndirection()[measuresDestToSrc[j]];
			newMeasures.add(input.getKPIs().get(measuresDestToSrc[j]));
		}
		
		input.setAxesIndirection(newAxesIndirection);
		input.setDataIndirection(newDataIndirection);
		input.setMeasures(newMeasures);
		input.setAxes(newAxes);
		return input;
	}

}
