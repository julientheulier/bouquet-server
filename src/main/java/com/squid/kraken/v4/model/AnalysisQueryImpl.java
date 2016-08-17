package com.squid.kraken.v4.model;

import java.util.List;

import com.squid.kraken.v4.model.ProjectAnalysisJob.OrderBy;
import com.squid.kraken.v4.model.ProjectAnalysisJob.RollUp;

public class AnalysisQueryImpl implements AnalysisQuery {

	private String domain;
	
	private List<AnalysisFacet> groupBy;
	
	private List<AnalysisFacet> metrics;
	
	private List<String> filters;
	
	private List<OrderBy> orderBy;
	
	private List<RollUp> rollups;
	
	private String period;
	
	private String[] timeframe;
	
	private String[] compareframe;

	private Long offset;

	private Long limit;
	
	private String bookmarkId;

	public AnalysisQueryImpl() {
	}
	
	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#getDomain()
	 */
	@Override
	public String getDomain() {
		return domain;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#setDomain(java.lang.String)
	 */
	@Override
	public void setDomain(String domain) {
		this.domain = domain;
	}
	
	public List<AnalysisFacet> getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(List<AnalysisFacet> groupBy) {
		this.groupBy = groupBy;
	}

	public List<AnalysisFacet> getMetrics() {
		return metrics;
	}

	public void setMetrics(List<AnalysisFacet> metrics) {
		this.metrics = metrics;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#getFilters()
	 */
	@Override
	public List<String> getFilters() {
		return filters;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#setFilters(java.util.List)
	 */
	@Override
	public void setFilters(List<String> filters) {
		this.filters = filters;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#getOrderBy()
	 */
	@Override
	public List<OrderBy> getOrderBy() {
		return orderBy;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#setOrderBy(java.util.List)
	 */
	@Override
	public void setOrderBy(List<OrderBy> orderBy) {
		this.orderBy = orderBy;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#getRollups()
	 */
	@Override
	public List<RollUp> getRollups() {
		return rollups;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#setRollups(java.util.List)
	 */
	@Override
	public void setRollups(List<RollUp> rollups) {
		this.rollups = rollups;
	}
	
	public String getPeriod() {
		return period;
	}

	public void setPeriod(String period) {
		this.period = period;
	}
	
	public String[] getTimeframe() {
		return timeframe;
	}

	public void setTimeframe(String[] timeframe) {
		this.timeframe = timeframe;
	}

	public String[] getCompareframe() {
		return compareframe;
	}

	public void setCompareframe(String[] compareframe) {
		this.compareframe = compareframe;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#getOffset()
	 */
	@Override
	public Long getOffset() {
		return offset;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#setOffset(java.lang.Long)
	 */
	@Override
	public void setOffset(Long offset) {
		this.offset = offset;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#getLimit()
	 */
	@Override
	public Long getLimit() {
		return limit;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#setLimit(java.lang.Long)
	 */
	@Override
	public void setLimit(Long limit) {
		this.limit = limit;
	}
	
	@Override
	public String getBookmarkId() {
		return bookmarkId;
	}

	@Override
	public void setBookmarkId(String bookmarkId) {
		this.bookmarkId = bookmarkId;
	}

	static public class AnalysisFacetImpl implements AnalysisFacet {

		private String name;
		
		private String expression;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getExpression() {
			return expression;
		}

		public void setExpression(String expression) {
			this.expression = expression;
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return expression;
		}
	
	}
	
}
