package com.squid.kraken.v4.model;

import java.util.List;

import com.squid.kraken.v4.model.ProjectAnalysisJob.OrderBy;
import com.squid.kraken.v4.model.ProjectAnalysisJob.RollUp;

public class BookmarkConfig {
	
	private List<RollUp> rollups;
	
	private List<OrderBy> orderBy;
	
	private Long limit;
	
	private String domain;
	
	private FacetSelection selection;
	
	private String[] chosenDimensions;
	
	private String[] chosenMetrics;

	public List<RollUp> getRollups() {
		return rollups;
	}

	public void setRollups(List<RollUp> rollups) {
		this.rollups = rollups;
	}

	public List<OrderBy> getOrderBy() {
		return orderBy;
	}

	public void setOrderBy(List<OrderBy> orderBy) {
		this.orderBy = orderBy;
	}

	public Long getLimit() {
		return limit;
	}

	public void setLimit(Long limit) {
		this.limit = limit;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public FacetSelection getSelection() {
		return selection;
	}

	public void setSelection(FacetSelection selection) {
		this.selection = selection;
	}

	public String[] getChosenDimensions() {
		return chosenDimensions;
	}

	public void setChosenDimensions(String[] chosenDimensions) {
		this.chosenDimensions = chosenDimensions;
	}

	public String[] getChosenMetrics() {
		return chosenMetrics;
	}

	public void setChosenMetrics(String[] chosenMetrics) {
		this.chosenMetrics = chosenMetrics;
	}

}