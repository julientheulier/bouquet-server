package com.squid.kraken.v4.model;

import java.util.ArrayList;
import java.util.List;

import com.squid.kraken.v4.model.NavigationQuery.Style;

public class AnalyticsQueryImpl extends AnalyticsSelectionImpl implements AnalyticsQuery {
	
	private String BBID;

	private String domain;
	
	private List<String> groupBy;
	
	private List<String> metrics;
	
	private List<String> orderBy;
	
	private List<String> rollups;

	private Long offset;

	private Long limit;
	
	private List<String> beyondLimit;
	
	private String bookmarkId;
	
	private Integer maxResults = null;
	
	private Integer startIndex = null;
	
	private String lazy = null;// false
	
	private Style style = Style.HUMAN;
	
	private List<Problem> problems;

	public AnalyticsQueryImpl() {
	}
	
	public AnalyticsQueryImpl(AnalyticsQuery copy) {
		super(copy);
		this.BBID = copy.getBBID();
		this.domain = copy.getDomain();
		this.groupBy = copy.getGroupBy();
		this.metrics = copy.getMetrics();
		this.orderBy = copy.getOrderBy();
		this.rollups = copy.getRollups();
		this.offset = copy.getOffset();
		this.limit = copy.getLimit();
		this.beyondLimit = copy.getBeyondLimit();
		this.bookmarkId = copy.getBookmarkId();
		this.maxResults = copy.getMaxResults();
		this.startIndex = copy.getStartIndex();
		this.lazy = copy.getLazy();
		this.style = copy.getStyle();
		this.problems = copy.getProblems();
	}

	@Override
	public String getBBID() {
		return BBID;
	}
	
	@Override
	public void setBBID(String BBID) {
		this.BBID = BBID;
	}
	
	@Override
	public String getDomain() {
		return domain;
	}
	
	@Override
	public void setDomain(String domain) {
		this.domain = domain;
	}
	
	public List<String> getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(List<String> groupBy) {
		this.groupBy = groupBy;
	}

	public List<String> getMetrics() {
		return metrics;
	}

	public void setMetrics(List<String> metrics) {
		this.metrics = metrics;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#getOrderBy()
	 */
	@Override
	public List<String> getOrderBy() {
		return orderBy;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#setOrderBy(java.util.List)
	 */
	@Override
	public void setOrderBy(List<String> orderBy) {
		this.orderBy = orderBy;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#getRollups()
	 */
	@Override
	public List<String> getRollups() {
		return rollups;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#setRollups(java.util.List)
	 */
	@Override
	public void setRollups(List<String> rollups) {
		this.rollups = rollups;
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
	
	public List<String> getBeyondLimit() {
		return beyondLimit;
	}

	public void setBeyondLimit(List<String> beyondLimit) {
		this.beyondLimit = beyondLimit;
	}

	@Override
	public String getBookmarkId() {
		return bookmarkId;
	}

	@Override
	public void setBookmarkId(String bookmarkId) {
		this.bookmarkId = bookmarkId;
	}

	@Override
	public Integer getMaxResults() {
		return maxResults;
	}

	@Override
	public void setMaxResults(Integer maxResults) {
		this.maxResults = maxResults;
	}

	@Override
	public Integer getStartIndex() {
		return startIndex;
	}

	@Override
	public void setStartIndex(Integer startIndex) {
		this.startIndex = startIndex;
	}

	@Override
	public String getLazy() {
		return lazy;
	}

	@Override
	public void setLazy(String lazy) {
		this.lazy = lazy;
	}

	@Override
	public Style getStyle() {
		return style;
	}

	@Override
	public void setStyle(Style style) {
		this.style = style;
	}

	@Override
	public String getQueryID() {
		return org.apache.commons.codec.digest.DigestUtils.sha256Hex(toString());
	}
	
	@Override
	public List<Problem> getProblems() {
		return problems;
	}

	@Override
	public void setProblems(List<Problem> problems) {
		this.problems = problems;
	}
	
	@Override
	public void add(Problem problem) {
		if (problems==null) problems = new ArrayList<>();
		problems.add(problem);
	}

	@Override
	public String toString() {
		return "AnalysisQueryImpl [BBID=" + BBID + ", domain=" + domain + ", groupBy=" + groupBy + ", metrics="
				+ metrics + ", filters=" + getFilters() + ", orderBy=" + orderBy + ", rollups=" + rollups + ", period="
				+ getPeriod() + ", timeframe=" + getTimeframe() + ", compareTo="
				+ getCompareTo() + ", offset=" + offset + ", limit=" + limit + ", bookmarkId="
				+ bookmarkId + ", maxResults=" + maxResults + ", startIndex=" + startIndex
				+ ", lazy=" + lazy + ", style=" + style + "]";
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
