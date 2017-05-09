package com.squid.kraken.v4.core.analysis.engine.processor;

import java.util.Set;
import java.util.HashSet;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.ExpressionLeaf;
import com.squid.core.expression.Operator;
import com.squid.kraken.v4.core.analysis.scope.AxisExpression;
import com.squid.kraken.v4.core.analysis.universe.Axis;

public class AxisListExtractor {

	public AxisListExtractor(){
		
	}
	
	public Set<Axis> eval(ExpressionAST exp){
		if (exp instanceof Operator){
			return evalOperator((Operator) exp);			
		} 
		if (exp instanceof ExpressionLeaf){
			return evalLeaf((ExpressionLeaf) exp);	
		}
		return new HashSet<Axis>();
	}
	
	private Set<Axis> evalOperator(Operator operator){
		HashSet<Axis> res = new HashSet<Axis>();
		for (ExpressionAST exp : operator.getArguments()){
			res.addAll(eval(exp));			
		}				
		return res;
	}
	
	private Set<Axis> evalLeaf(ExpressionLeaf leaf){
		HashSet<Axis> res = new HashSet<Axis>();
		if (leaf instanceof AxisExpression) {
			res.add(((AxisExpression)leaf).getAxis());
		}  
		return res;		
	}
	
}
