package com.squid.kraken.v4.export;

import com.squid.core.jdbc.engine.IExecutionItem;
import com.squid.kraken.v4.core.analysis.engine.query.mapping.QueryMapper;

public class ExecuteAnalysisResult {

	private IExecutionItem item;
	private QueryMapper mapper;
	
	public ExecuteAnalysisResult(IExecutionItem item, QueryMapper mapper) {
		super();
		this.item = item;
		this.mapper = mapper;
	}
	
	public IExecutionItem getItem() {
		return item;
	}
	
	public void setItem(IExecutionItem item) {
		this.item = item;
	}
	
	public QueryMapper getMapper() {
		return mapper;
	}
	
	public void setMapper(QueryMapper mapper) {
		this.mapper = mapper;
	}

}
