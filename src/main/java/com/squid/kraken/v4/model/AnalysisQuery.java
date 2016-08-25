package com.squid.kraken.v4.model;

import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.squid.kraken.v4.model.AnalysisQueryImpl.AnalysisFacetImpl;
import com.squid.kraken.v4.model.NavigationQuery.Style;
import com.squid.kraken.v4.model.ProjectAnalysisJob.OrderBy;
import com.squid.kraken.v4.model.ProjectAnalysisJob.RollUp;

@JsonDeserialize(as = AnalysisQueryImpl.class)
public interface AnalysisQuery {
	
	public String getBBID();
	
	void setBBID(String BBID);

	public String getDomain();

	public void setDomain(String domain);

	public List<String> getGroupBy();
	
	public void setGroupBy(List<String> facets);
	
	public List<String> getMetrics();

	public void setMetrics(List<String> facets);
	
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
	
	void setStyle(Style style);

	Style getStyle();

	void setLazy(String lazy);

	String getLazy();

	void setStartIndex(Integer startIndex);

	Integer getStartIndex();

	void setMaxResults(Integer maxResults);

	Integer getMaxResults();

	void setFormat(String format);

	String getFormat();

	/**
	 * return an unique identifier based on the query value, i.e. two identical queries will have the same ID.
	 * @return
	 */
	String getQueryID();

	@JsonDeserialize(as = AnalysisFacetImpl.class)
	static public interface AnalysisFacet {
		
		public String getName();
		
		public void setName(String name);
		
		public String getExpression();
		
		public void setExpression(String expression);
		
	}

}