/*******************************************************************************
 * Copyright © Squid Solutions, 2016
 *
 * This file is part of Open Bouquet software.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation (version 3 of the License).
 *
 * There is a special FOSS exception to the terms and conditions of the
 * licenses as they are applied to this program. See LICENSE.txt in
 * the directory of this program distribution.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * Squid Solutions also offers commercial licenses with additional warranties,
 * professional functionalities or services. If you purchase a commercial
 * license, then it supersedes and replaces any other agreement between
 * you and Squid Solutions (above licenses and LICENSE.txt included).
 * See http://www.squidsolutions.com/EnterpriseBouquet/
 *******************************************************************************/
package com.squid.kraken.v4.core.analysis.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.squid.core.expression.ExpressionAST;
import com.squid.core.sql.render.IOrderByPiece.NULLS_ORDERING;
import com.squid.core.sql.render.IOrderByPiece.ORDERING;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Property;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.model.Expression;
import com.squid.kraken.v4.model.ProjectAnalysisJob.Position;

/**
 * This is the object used to compute a dashboard
 * @author sfantino
 *
 */
public class DashboardAnalysis extends Dashboard {

	private ArrayList<GroupByAxis> grouping = new ArrayList<GroupByAxis>();

	// list the rollup Axis
	private ArrayList<GroupByAxis> rollup = new ArrayList<>();
	private boolean rollupGrandTotal;// if true, add a grand total (no axis). It is OK if rollup.isEmpty()

	private ArrayList<OrderBy> orders = new ArrayList<OrderBy>();
	private Long limit = null;
	private Long offset = null;

	private List<GroupByAxis> beyondLimit = new ArrayList<>();
	// T0126 & T1042: in case of beyondLimit + compareTo we need to use a different selection for computing the limit subquery
	private DashboardSelection beyondLimitSelection = null;

	private Map<String, Object> optionKeys = null;
	public static final String COMPUTE_GROWTH_OPTION_KEY = "computeGrowth";// true|false flag to enable computing the growth percentage when performing a compareTo analysis

	private boolean lazy = false;

	private String jobId;


	public DashboardAnalysis(Universe universe) {
		super(universe);
	}

	public List<GroupByAxis> getGrouping() {
		return grouping;
	}

	public GroupByAxis findGrouping(Axis axis) {
		for (GroupByAxis groupBy : grouping) {
			if (groupBy.getAxis().equals(axis)) {
				return groupBy;
			}
		}
		// else
		return null;
	}

	public void orderBy(OrderBy order) {
		if (order instanceof OrderByGrowth){
			orders.add(new OrderByGrowth(orders.size(), order.getExpression(), order.getOrdering(), ((OrderByGrowth) order).expr));
		}else{
			orders.add(new OrderBy(orders.size(), order.getExpression(), order.getOrdering(), order.getNullsOrdering()));
		}
	}



	public void orderBy(ExpressionAST expr, ORDERING order, NULLS_ORDERING nulls) {
		orders.add(new OrderBy(orders.size(), expr, order, nulls));
	}

	public void orderBy(Property property, ORDERING order, NULLS_ORDERING nulls) {
		orders.add(new OrderBy(orders.size(), property.getReference(), order, nulls));
	}

	public void orderByGrowth( ExpressionAST exprAST, ORDERING order, Expression exp){
		orders.add(new OrderByGrowth(orders.size(), exprAST, order, exp ) );
	}

	public List<OrderBy> getOrders() {
		return orders;
	}

	public void setOrders(List<OrderBy> orders) {
		this.orders = new ArrayList<>(orders);
	}

	public boolean hasOrderBy() {
		return !orders.isEmpty();
	}

	public void noLimit() {
		this.limit = null;
	}

	public void limit(long limit) {
		this.limit = limit;
	}

	public void noOffset() {
		this.offset = null;
	}

	public Long getLimit() {
		return limit;
	}

	public boolean hasLimit() {
		return limit!=null;
	}

	public void beyondLimit(GroupByAxis axis) {
		beyondLimit.add(axis);
	}

	public List<GroupByAxis> getBeyondLimit() {
		return beyondLimit;
	}

	public void setBeyondLimit(List<GroupByAxis> beyondLimit) {
		this.beyondLimit = beyondLimit;
	}

	public boolean hasBeyondLimit() {
		return beyondLimit!=null && !beyondLimit.isEmpty();
	}

	/**
	 * reset the beyondLimit property
	 */
	public void resetBeyondLimit() {
		beyondLimit = null;
	}

	/**
	 * this is the alternative selection to use to compute the beyondLimit analysis. This analysis will define a sub segment of values
	 * where to execute the original analysis. This option is use by the compareTo feature to make sure that the application is computing
	 * the result on comparable population (for example if we compare evolution of the top 10 series on present period, we want to compare in the past from the very same series, not the top 10 from the past)
	 *
	 * @return
	 */
	public DashboardSelection getBeyondLimitSelection() {
		return beyondLimitSelection;
	}

	/**
	 * @param beyodLimitSelection
	 */
	public void setBeyondLimitSelection(DashboardSelection beyondLimitSelection) {
		this.beyondLimitSelection = beyondLimitSelection;
	}

	public void offset(long offset) {
		this.offset = offset;
	}

	public Long getOffset() {
		return offset;
	}

	public boolean hasOffset() {
		return offset!=null;
	}

	public Object getOption(String key) {
		return optionKeys!=null?optionKeys.get(key):null;
	}

	/**
	 * @param optionKeys the optionKeys to set
	 */
	public void setOptionKeys(Map<String, Object> optionKeys) {
		this.optionKeys = optionKeys;
	}

	public void lazy(boolean lazy){
		this.lazy = lazy;
	}

	public boolean isLazy(){
		return this.lazy;
	}

	public String getJobId(){
		return this.jobId;
	}

	public void setJobId(String jobId){
		this.jobId = jobId;
	}

	public GroupByAxis add(GroupByAxis slice) {
		grouping.add(slice);
		return slice;
	}

	public GroupByAxis add(Axis axis) {
		GroupByAxis slice = new GroupByAxis(axis);
		grouping.add(slice);
		return slice;
	}

	public GroupByAxis add(Axis axis, boolean rollup) {
		GroupByAxis slice = new GroupByAxis(axis,rollup);
		grouping.add(slice);
		if (rollup) {
			this.rollup.add(slice);
		}
		return slice;
	}

	public boolean hasRollup() {
		return !rollup.isEmpty() || rollupGrandTotal;
	}

	public void rollup(GroupByAxis axis, Position position) {
		axis.setRollup(true);
		axis.setRollupPosition(position);
		rollup.add(axis);
	}

	public void setRollup(List<GroupByAxis> rollup) {
		this.rollup = new ArrayList<>(rollup);
	}

	public List<GroupByAxis> getRollup() {
		return rollup;
	}

	public void setRollupGrandTotal(boolean flag) {
		this.rollupGrandTotal = flag;
	}

	public boolean isRollupGrandTotal() {
		return rollupGrandTotal;
	}

	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer(super.toString());
		buffer.append("\n"+this.jobId);
		if (!grouping.isEmpty()) {
			buffer.append("\nGroup By");
			for (GroupByAxis slice : grouping) {
				buffer.append(" ").append(slice.getAxis().toString());
			}
		}
		if (!orders.isEmpty()) {
			buffer.append("\nOrder By");
			for (OrderBy order : orders) {
				buffer.append(" ").append(order.getExpression().prettyPrint()+" "+order.getOrdering().toString());
			}
		}
		if (limit!=null) {
			buffer.append("\nLimit "+limit);
		}
		if (offset!=null) {
			buffer.append("\nOffset "+offset);
		}
		return buffer.toString();
	}

}
