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

import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.PrettyPrintOptions;
import com.squid.core.expression.PrettyPrintOptions.ReferenceStyle;
import com.squid.core.expression.scope.ExpressionScope;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.AnalysisQuery;
import com.squid.kraken.v4.model.AnalysisQueryImpl;
import com.squid.kraken.v4.model.ProjectAnalysisJob.OrderBy;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.*;

/**
 * Simple class to handle the vegalite configuration state
 * @author sergefantino
 *
 */
public class VegaliteConfigurator {

	private AnalysisQuery query;
	private AnalysisQueryImpl required;
	
	private PrettyPrintOptions options;
	
	private int pos = 0;
	private boolean isTimeseries = false;// this is true if the user select a date for x axis
	private int timeseriesPosition = 0;
	
	private VegaliteSpecs specs;

	public VegaliteConfigurator(Space space, AnalysisQuery query) {
		this.query = query;
		//
		required = new AnalysisQueryImpl();
		required.setGroupBy(new ArrayList<String>());
		required.setMetrics(new ArrayList<String>());
		required.setOrderBy(new ArrayList<OrderBy>());
		//
		options = new PrettyPrintOptions(ReferenceStyle.NAME, space.getImageDomain());
		//
		specs = new VegaliteSpecs();
		specs.encoding = new Encoding();
	}
	
	/**
	 * @return the specs
	 */
	public VegaliteSpecs getSpecs() {
		return specs;
	}
	
	/**
	 * @return the required
	 */
	public AnalysisQueryImpl getRequired() {
		return required;
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
		return timeseriesPosition;
	}
	
	public ChannelDef createChannelDef(String channelName, ExpressionScope scope, String expr) throws ScopeException {
		if (expr!=null && !expr.equals("")) {
			if (expr.equals("__PERIOD")) {
				expr = query.getPeriod();
				if (expr==null) throw new ScopeException("no period defined, you cannot use __PERIOD alias for channel "+channelName);
			}
			ExpressionAST ast = scope.parseExpression(expr);
			ChannelDef channel = createChannelDef(ast);
			if (channel.type==DataType.temporal && channelName.equals("x")) {// only for x
				this.isTimeseries = true;
				this.timeseriesPosition = pos;
			} else if (channel.type==DataType.quantitative) {
				// handling sort option
			}
			pos++;
			return channel;
		} else {
			return null;
		}
	}
	
	private ChannelDef createChannelDef(ExpressionAST expr) {
		ChannelDef channel = new ChannelDef();
		channel.type = computeDataType(expr);
		if (channel.type==DataType.temporal) {
			IDomain image = expr.getImageDomain();
			if (image.isInstanceOf(IDomain.YEARLY)) {
				channel.timeUnit = TimeUnit.year;
			} else if (image.isInstanceOf(IDomain.MONTHLY)) {
				channel.timeUnit = TimeUnit.yearmonth;
			} else if (image.isInstanceOf(IDomain.DATE)) {
				channel.timeUnit = TimeUnit.yearmonthdate;
			}
		}
		String name = expr.getName();
		if (name==null) {
			name = formatName(expr.prettyPrint());
		} else {
			name = formatName(name);
		}
		// need to convert to lower-case because of CSV export...
		name = name.toLowerCase();
		expr.setName(name);
		channel.field = name;
		String namedExpression = expr.prettyPrint(options) + " as '" + name +"'"; 
		if (channel.type==DataType.quantitative) {
			// it's a metric
			required.getMetrics().add(namedExpression);
		} else {
			// it's a groupBy
			required.getGroupBy().add(namedExpression);
		}
		return channel;
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
