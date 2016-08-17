package com.squid.kraken.v4.model;

import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.squid.kraken.v4.model.AnalysisQueryImpl.AnalysisFacetImpl;
import com.squid.kraken.v4.model.ProjectAnalysisJob.OrderBy;
import com.squid.kraken.v4.model.ProjectAnalysisJob.RollUp;

@JsonDeserialize(as = AnalysisQueryImpl.class)
public interface AnalysisQuery {

	public String getDomain();

	public void setDomain(String domain);

	public List<AnalysisFacet> getGroupBy();
	
	public void setGroupBy(List<AnalysisFacet> facets);
	
	public List<AnalysisFacet> getMetrics();

	public void setMetrics(List<AnalysisFacet> facets);
	
	/**
	 * get the period expression, used to filter the timeframe.
	 * This must be a valid date or timestamp expression.
	 * @return
	 */
	public String getPeriod();
	
	public void setPeriod(String expression);
	
	public String[] getTimeframe();
	
	public void setTimeframe(String[] timeframe);
	
	public String[] getCompareframe();
	
	public void setCompareframe(String[] compareframe);

	public List<String> getFilters();

	public void setFilters(List<String> filters);

	public List<OrderBy> getOrderBy();

	public void setOrderBy(List<OrderBy> orderBy);

	public List<RollUp> getRollups();

	public void setRollups(List<RollUp> rollups);

	public Long getOffset();

	public void setOffset(Long offset);

	public Long getLimit();

	public void setLimit(Long limit);
	
	public String getBookmarkId();

	public void setBookmarkId(String bookmarkId);
	
	@JsonDeserialize(as = AnalysisFacetImpl.class)
	static public interface AnalysisFacet {
		
		public String getName();
		
		public void setName(String name);
		
		public String getExpression();
		
		public void setExpression(String expression);
		
	}

}