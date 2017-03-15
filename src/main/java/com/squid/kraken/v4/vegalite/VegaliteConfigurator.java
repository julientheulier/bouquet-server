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
package com.squid.kraken.v4.vegalite;

import java.util.ArrayList;
import java.util.List;

import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.PrettyPrintOptions;
import com.squid.core.expression.PrettyPrintOptions.ReferenceStyle;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.datamatrix.TransposeConverter;
import com.squid.kraken.v4.core.analysis.scope.SpaceScope;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.AnalyticsQuery;
import com.squid.kraken.v4.model.AnalyticsQueryImpl;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Aggregate;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.ChannelDef;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.DataType;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.TimeUnit;

/**
 * Simple class to handle the vegalite configuration state
 * @author sergefantino
 *
 */
public class VegaliteConfigurator {

	private AnalyticsQuery query;
	private AnalyticsQueryImpl required;
	
	private Space space;
	private SpaceScope scope;
	private PrettyPrintOptions options;
	
	private boolean isTimeseries = false;// this is true if the user select a date for x axis
	private String timeseriesField = null;
	
	private boolean hasMetric = false;
	private boolean hasMetricSeries = false;
	private boolean hasMetricValue = false;

	public VegaliteConfigurator(Space space, AnalyticsQuery query) {
		this.query = query;
		//
		required = new AnalyticsQueryImpl();
		required.setGroupBy(new ArrayList<String>());
		required.setMetrics(new ArrayList<String>());
		required.setOrderBy(new ArrayList<String>());
		//
		this.space = space;
		scope = new SpaceScope(this.space);
		// add the period parameter if available
		if (query.getPeriod()!=null && !query.getPeriod().equals("")) {
			try {
				ExpressionAST period = scope.parseExpression(query.getPeriod());
				scope.addParam("__PERIOD", period);
			} catch (ScopeException e) {
				// ignore
			}
		}
		options = new PrettyPrintOptions(ReferenceStyle.NAME, this.space.getImageDomain());
	}

	/**
	 * @return
	 */
	public SpaceScope getScope() {
		return scope;
	}
	
	public ExpressionAST parse(String expression) throws ScopeException {
		return scope.parseExpression(expression);
	}
	
	public String prettyPrint(ExpressionAST expression) {
		return expression.prettyPrint(options);
	}
	
	/**
	 * @return the hasMetric
	 */
	public boolean isHasMetric() {
		return hasMetric;
	}
	
	/**
	 * @return the hasMetricSeries
	 */
	public boolean isHasMetricSeries() {
		return hasMetricSeries;
	}
	
	/**
	 * @return the hasMetricValue
	 */
	public boolean isHasMetricValue() {
		return hasMetricValue;
	}
	
	/**
	 * @return the required
	 */
	public AnalyticsQueryImpl getRequired() {
		return required;
	}
	
	public void addRequiredMetrics(List<String> metrics) {
		required.getMetrics().addAll(metrics);
	}
	
	/**
	 * @return the isTimeseries
	 */
	public boolean isTimeseries() {
		return isTimeseries;
	}
	
	/**
	 * @return the timeseriesPosition
	 */
	public int getTimeseriesPosition() {
		if (!isTimeseries) return -1;
		int pos = 0;
		for (String dimension : required.getGroupBy()) {
			if (dimension.equals(timeseriesField)) return pos;
			pos++;
		}
		// else
		return -1;
	}
	
	public ChannelDef createChannelDef(String channelName, String expr) throws ScopeException {
		if (expr!=null && !expr.equals("")) {
			ChannelDef channel = null;
			if (expr.equals(TransposeConverter.METRIC_SERIES_COLUMN)) {
				if (hasMetric) {
					// reset the hasMetric flag
					hasMetric = false;
				}
				// allow to use the metrics in many channels (to add color for example)
				//if (hasMetricSeries) {
				//	throw new ScopeException("invalid channel '"+channelName+"'="+expr+": there is already  a multi-metrics series defined");
				//}
				hasMetricSeries = true;
				channel = new ChannelDef();
				channel.type = DataType.nominal;
				channel.field = expr;
			} else if (expr.equals(TransposeConverter.METRIC_VALUE_COLUMN)) {
				if (hasMetric) {
					// reset the hasMetric flag
					hasMetric = false;
				}
				// allow to use the metrics in several channels (to add color/size for example)
				//if (hasMetricValue) {
				//	throw new ScopeException("invalid channel '"+channelName+"'="+expr+": there is already  a multi-metrics value defined");
				//}
				hasMetricValue = true;
				channel = new ChannelDef();
				channel.type = DataType.quantitative;
				channel.field = expr;
				channel.aggregate = Aggregate.sum;// add the sum in order so that VGL correctly stack values
				required.getMetrics().addAll(query.getMetrics());// add all query metrics
			} else {
				channel = parseChannelDef(channelName, expr);
			}
			return channel;
		} else {
			return null;
		}
	}
	
	private ChannelDef parseChannelDef(String channelName, String expr) throws ScopeException {
		ExpressionAST ast = parse(expr);
		ChannelDef channel = new ChannelDef();
		channel.type = computeDataType(ast);
		if (channel.type==DataType.temporal) {
			IDomain image = ast.getImageDomain();
			if (image.isInstanceOf(IDomain.YEARLY)) {
				channel.timeUnit = TimeUnit.year;
			} else if (image.isInstanceOf(IDomain.MONTHLY)) {
				channel.timeUnit = TimeUnit.yearmonth;
			} else if (image.isInstanceOf(IDomain.DATE)) {
				channel.timeUnit = TimeUnit.yearmonthdate;
			}
		}
		String name = ast.getName();
		if (name==null) {
			name = formatName(expr);
		} else {
			name = formatName(name);
		}
		// need to convert to lower-case because of CSV export...
		name = name.toLowerCase();
		ast.setName(name);
		channel.field = name;
		String namedExpression = ast.prettyPrint(options) + " as '" + name +"'";
		if (channel.type==DataType.quantitative) {
			channel.aggregate = Aggregate.sum;// add the sum in order so that VGL correctly stack values
			if (!hasMetricSeries && !hasMetricValue) {// series get precedence
				// it's a metric
				required.getMetrics().add(namedExpression);
				hasMetric = true;
			}
		} else {
			// it's a groupBy
			addRequiredGroubBy(namedExpression);
		}
		if (channel.type==DataType.temporal && channelName.equals("x")) {// only for x
			this.isTimeseries = true;
			this.timeseriesField = namedExpression;
		}
		return channel;
	}
	
	private boolean addRequiredGroubBy(String expression) {
		if (!required.getGroupBy().contains(expression)) {
			return required.getGroupBy().add(expression);
		} else {
			return false;
		}
	}

	private DataType computeDataType(ExpressionAST expr) {
		IDomain image = expr.getImageDomain();
		if (image.isInstanceOf(IDomain.AGGREGATE)) {
			// it's a metric
			return DataType.quantitative;
		} else {
			if (image.isInstanceOf(IDomain.TEMPORAL)) {
				return DataType.temporal;
			} else if (image.isInstanceOf(IDomain.NUMERIC)) {
				return DataType.ordinal;
			} else {
				return DataType.nominal;
			}
		}
	}

	private String formatName(String prettyPrint) {
		return prettyPrint.replaceAll("[(),.]>", " ").trim().replaceAll("[^ a-zA-Z_0-9]", "").replace(' ', '_');
	}

}
