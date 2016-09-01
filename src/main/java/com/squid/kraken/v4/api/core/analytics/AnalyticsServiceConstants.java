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
package com.squid.kraken.v4.api.core.analytics;

/**
 * @author sergefantino
 *
 */
public interface AnalyticsServiceConstants {

	public final static String BBID_PARAM_NAME = "REFERENCE";
	public final static String FACETID_PARAM_NAME = "FACETID";

	public static final String LAZY_PARAM = "lazy";
	public static final String START_INDEX_PARAM = "startIndex";
	public static final String MAX_RESULTS_PARAM = "maxResults";
	public static final String DATA_PARAM = "data";
	public static final String LIMIT_PARAM = "limit";
	public static final String ENVELOPE_PARAM = "envelope";
	public static final String ROLLUP_PARAM = "rollup";
	public static final String ORDERBY_PARAM = "orderby";
	public static final String COMPARETO_PARAM = "compareto";
	public static final String TIMEFRAME_PARAM = "timeframe";
	public static final String PERIOD_PARAM = "period";
	public static final String METRICS_PARAM = "metrics";
	public static final String GROUP_BY_PARAM = "groupBy";
	public static final String TIMEOUT_PARAM = "timeout";
	public static final String FILTERS_PARAM = "filters";
	public static final String STYLE_PARAM = "style";
	public static final String VISIBILITY_PARAM = "visibility";

}
