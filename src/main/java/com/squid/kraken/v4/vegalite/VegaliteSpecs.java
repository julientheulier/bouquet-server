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

/**
 * Java mapping of the Vegalite specs, cf.
 * https://vega.github.io/vega-lite/docs/spec.html
 * 
 * @author sergefantino
 *
 */
public class VegaliteSpecs {

	public static class Encoding {

		public ChannelDef x, y, color, opacity, shape, size, text, column, row;

		public ChannelDef detail, order, path;
	}

	public static class ChannelDef {

		public String field;

		public DataType type;
		
		public TimeUnit timeUnit;

	}
	
	public enum TimeUnit {
		year, yearmonth, yearmonthday, yearmonthdate, yearday, yeardate, yearmonthdayhours, yearmonthdayhoursminutes,
		month, day, date, hours, minutes, seconds, milliseconds, hoursminutes, hoursminutesseconds, minutesseconds, secondsmilliseconds
	}

	public enum DataType {
		quantitative, temporal, ordinal, nominal
	}

	public enum Mark {
		point, circle, square, text, tick, bar, line, area
	}

	public static class Data {

		public Object[] values;

		public String url;

		public Format format;

	}

	public static class Format {

		public FormatType type;

		public Object parse;
	}
	
	public enum FormatType {
		json, csv, tsv, topojson// lowercase is mandatory
	}

	public Data data;

	public Mark mark;

	public Encoding encoding;

}
