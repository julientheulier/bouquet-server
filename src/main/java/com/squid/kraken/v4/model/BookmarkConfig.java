package com.squid.kraken.v4.model;

import java.util.List;
import java.util.Map;

import com.squid.kraken.v4.model.ProjectAnalysisJob.OrderBy;
import com.squid.kraken.v4.model.ProjectAnalysisJob.RollUp;

public class BookmarkConfig {
	
	public static final String TABLE_ANALYSIS = "tableAnalysis";
	public static final String BARCHART_ANALYSIS = "barAnalysis";
	public static final String TIMESERIES_ANALYSIS = "timeAnalysis";
	
	private List<RollUp> rollups;
	
	private List<OrderBy> orderBy;
	
	private Long limit;
	
	private String project;
	
	private String domain;
	
	private FacetSelection selection;
	
	private String[] chosenDimensions;
	
	private String[] chosenFacets;

	private String[] chosenMetrics;
	
	private String[] availableDimensions;
	
	private String[] availableMetrics;
	
	// handling timeframe
	private Map<String, String> period;
	
	private String timeUnit;
	
	// data-visualization part
	private String currentAnalysis;// this is the bookmark default dataviz
	
	/**
	 * 
	 */
	public BookmarkConfig() {
		// TODO Auto-generated constructor stub
	}

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
	
	/**
	 * @return the project
	 */
	public String getProject() {
		return project;
	}
	
	/**
	 * @param project the project to set
	 */
	public void setProject(String project) {
		this.project = project;
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

	public String[] getChosenFacets() {
		return chosenFacets;
	}

	public void setChosenFacets(String[] chosenFacets) {
		this.chosenFacets = chosenFacets;
	}

	public String[] getChosenMetrics() {
		return chosenMetrics;
	}

	public void setChosenMetrics(String[] chosenMetrics) {
		this.chosenMetrics = chosenMetrics;
	}

	public String[] getAvailableDimensions() {
		return availableDimensions;
	}

	public void setAvailableDimensions(String[] availableDimensions) {
		this.availableDimensions = availableDimensions;
	}

	public String[] getAvailableMetrics() {
		return availableMetrics;
	}

	public void setAvailableMetrics(String[] availableMetrics) {
		this.availableMetrics = availableMetrics;
	}

	public Map<String, String> getPeriod() {
		return period;
	}

	public void setPeriod(Map<String, String> period) {
		this.period = period;
	}

	public String getTimeUnit() {
		return timeUnit;
	}

	public void setTimeUnit(String timeUnit) {
		this.timeUnit = timeUnit;
	}

	public String getCurrentAnalysis() {
		return currentAnalysis;
	}

	public void setCurrentAnalysis(String currentAnalysis) {
		this.currentAnalysis = currentAnalysis;
	}

}