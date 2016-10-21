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

import java.util.Arrays;

import com.squid.kraken.v4.model.DataLayout;

/**
 * @author sergefantino
 *
 */
public interface AnalyticsServiceConstants {

	public final static String BBID_PARAM_NAME = "REFERENCE";
	public final static String FACETID_PARAM_NAME = "FACETID";
	
	public final static String ACCESS_TOKEN_PARAM = "access_token";

	public static final String PARENT_PARAM = "parent";
	public static final String LAZY_PARAM = "lazy";
	public static final String START_INDEX_PARAM = "startIndex";
	public static final String MAX_RESULTS_PARAM = "maxResults";
	public static final String DATA_PARAM = "data";
	public static final String DATA_PARAM_VALUES = Arrays.toString(DataLayout.values());
	public static final String APPLY_FORMATTING_PARAM = "applyFormatting";
	public static final String LIMIT_PARAM = "limit";
	public static final String BEYOND_LIMIT_PARAM = "beyondLimit";
	public static final String OFFSET_PARAM = "offset";
	public static final String ENVELOPE_PARAM = "envelope";
	public static final String ROLLUP_PARAM = "rollup";
	public static final String ORDERBY_PARAM = "orderBy";
	public static final String COMPARETO_PARAM = "compareTo";
	public static final String TIMEFRAME_PARAM = "timeframe";
	public static final String PERIOD_PARAM = "period";
	public static final String METRICS_PARAM = "metrics";
	public static final String GROUP_BY_PARAM = "groupBy";
	public static final String TIMEOUT_PARAM = "timeout";
	public static final String FILTERS_PARAM = "filters";
	public static final String STYLE_PARAM = "style";
	public static final String VISIBILITY_PARAM = "visibility";

	public static final String VIEW_X_PARAM = "x";
	public static final String VIEW_Y_PARAM = "y";
	public static final String VIEW_COLOR_PARAM = "color";
	public static final String VIEW_SIZE_PARAM = "size";
	public static final String VIEW_COLUMN_PARAM = "column";
	public static final String VIEW_ROW_PARAM = "row";

}
