package com.squid.kraken.v4.model;

import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.squid.kraken.v4.model.ProjectAnalysisJob.OrderBy;
import com.squid.kraken.v4.model.ProjectAnalysisJob.RollUp;
import com.squid.kraken.v4.model.SimpleAnalysis.SimpleFacet;

@JsonDeserialize(as = SimpleAnalysis.class)
public interface Analysis {

	public abstract String getDomain();

	public abstract void setDomain(String domain);

	public abstract List<AnalysisFacet> getFacets();

	public abstract void setFacets(List<AnalysisFacet> facets);

	public abstract List<String> getFilters();

	public abstract void setFilters(List<String> filters);

	public abstract List<OrderBy> getOrderBy();

	public abstract void setOrderBy(List<OrderBy> orderBy);

	public abstract List<RollUp> getRollups();

	public abstract void setRollups(List<RollUp> rollups);

	public abstract Long getOffset();

	public abstract void setOffset(Long offset);

	public abstract Long getLimit();

	public abstract void setLimit(Long limit);
	
	public abstract String getBookmarkId();

	public abstract void setBookmarkId(String bookmarkId);
	
	@JsonDeserialize(as = SimpleFacet.class)
	static public interface AnalysisFacet {
		
		public abstract String getName();
		
		public abstract void setName(String name);
		
		public abstract String getExpression();
		
		public abstract void setExpression(String expression);
		
	}

}