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
	public static final String COMPARETO_COMPUTE_GROWTH_PARAM = "computeGrowth";
	public static final String TIMEFRAME_PARAM = "timeframe";
	public static final String PERIOD_PARAM = "period";
	public static final String METRICS_PARAM = "metrics";
	public static final String GROUP_BY_PARAM = "groupBy";
	public static final String TIMEOUT_PARAM = "timeout";
	public static final String FILTERS_PARAM = "filters";
	public static final String STYLE_PARAM = "style";
	public static final String VISIBILITY_PARAM = "visibility";
	
	public static final String FILTERS_DOC = "Define the filters to apply to results. A filter must be a valid conditional expression. If no filter is defined, the subject default filters will be applied. You can use the * token to extend the subject default filters instead of replacing.";
	public static final String PERIOD_DOC = "the period defines a dimension or expression of a type date that is used to restrict the timeframe. You can use the __PERIOD expression as a alias to it in other parameters (e.g.: "+GROUP_BY_PARAM+","+ORDERBY_PARAM+"...).";
	public static final String TIMEFRAME_DOC = "the timeframe defines the period range to filter. You can use an array of two dates for lower/upper bounds (inclusive). Or some alias like __ALL, __LAST_DAY, __LAST_7_DAYS, __CURRENT_MONTH, __PREVIOUS_MONTH, __CURRENT_YEAR, __PREVIOUS_YEAR";
	public static final String COMPARETO_DOC = "Activate and define the compare to period. You can use an array of two dates for lower/upper bounds (inclusive). Or some alias like __COMPARE_TO_PREVIOUS_PERIOD, __COMPARE_TO_PREVIOUS_MONTH, __COMPARE_TO_PREVIOUS_YEAR";
	public static final String COMPARETO_COMPUTE_GROWTH_DOC = "When set to true and using the compareTo feature, compute the growth between present and past periods (default is true)";
	
	public static final String GROUPBY_DOC = "Define the facets to agroup by the results. Facet can be defined using it's ID or any valid expression. If empty, the subject default parameters will apply. You can use the * token to extend the subject default parameters.";
	public static final String METRICS_DOC = "Define the metrics to compute. Metric can be defined using it's ID or any valid expression. If empty, the subject default parameters will apply. You can use the * token to extend the subject default parameters.";
	public static final String ROLLUP_DOC = "Optionaly you can compute rollup for any groupBy column. It must be a valid indexe of a groupBy column or the expression FIRST(N) or LAST(N) to set the rollup position. Index starts at zero. Special value of -1 can be used to compute a grand total.";
	public static final String ORDERBY_DOC = "Define how to sort the results. You can specify a colun either by it's index (starting at zero by groupBy, then metrics), or using an expression. Use the function DESC() and ASC() to modify the sort order. The expression must be a column, or at least a hierarchical parent of a column (in that case a groupBy may be added automatically to the query).";
	public static final String LIMIT_DOC= "limit the resultset size as computed by the database. Note that this is independant from the paging size defined by "+MAX_RESULTS_PARAM+".";
	public static final String OFFSET_DOC = "offset the resultset first row - usually used with limit to paginate the database. Note that this is independant from the paging defined by "+START_INDEX_PARAM+".";
	
	public static final String BEYOND_LIMIT_DOC = "Exclude a dimension from the limit";
	
	public static final String START_INDEX_DOC = "Pagination starting index. Index is zero-based, so use the #count of the last row to view the next page.";
	public static final String MAX_RESULTS_DOC = "Define the pagination size.";
	
	public static final String VIEW_X_PARAM = "x";
	public static final String VIEW_Y_PARAM = "y";
	public static final String VIEW_COLOR_PARAM = "color";
	public static final String VIEW_SIZE_PARAM = "size";
	public static final String VIEW_COLUMN_PARAM = "column";
	public static final String VIEW_ROW_PARAM = "row";

}
