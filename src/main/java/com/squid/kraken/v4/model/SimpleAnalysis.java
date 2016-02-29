package com.squid.kraken.v4.model;

import java.util.List;

import com.squid.kraken.v4.model.ProjectAnalysisJob.OrderBy;
import com.squid.kraken.v4.model.ProjectAnalysisJob.RollUp;

public class SimpleAnalysis implements Analysis {

	private String domain;
	
	private List<String> columns;
	
	private List<String> filters;
	
	private List<OrderBy> orderBy;
	
	private List<RollUp> rollups;

	private Long offset;

	private Long limit;

	public SimpleAnalysis() {
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

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#getColumns()
	 */
	@Override
	public List<String> getColumns() {
		return columns;
	}

	/* (non-Javadoc)
	 * @see com.squid.kraken.v4.model.Analysis#setColumns(java.util.List)
	 */
	@Override
	public void setColumns(List<String> columns) {
		this.columns = columns;
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
	
}
