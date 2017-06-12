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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriBuilderException;

import org.apache.commons.lang3.StringEscapeUtils;
import org.eclipse.xtext.ide.editor.contentassist.ContentAssistEntry;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.squid.core.domain.IDomain;
import com.squid.core.domain.operators.ListContentAssistEntry;
import com.squid.core.domain.operators.OperatorDefinition;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.FacetBuilder;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.scope.AxisExpression;
import com.squid.kraken.v4.core.analysis.scope.SpaceScope;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.expression.scope.ExpressionSuggestionHandler;
import com.squid.kraken.v4.model.AnalyticsQuery;
import com.squid.kraken.v4.model.AnalyticsReply;
import com.squid.kraken.v4.model.AnalyticsSelection;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.DataTable;
import com.squid.kraken.v4.model.DataTable.Col;
import com.squid.kraken.v4.model.DataTable.Row;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.model.ExpressionSuggestion;
import com.squid.kraken.v4.model.ExpressionSuggestionItem;
import com.squid.kraken.v4.model.NavigationItem;
import com.squid.kraken.v4.model.NavigationQuery;
import com.squid.kraken.v4.model.NavigationQuery.Style;
import com.squid.kraken.v4.model.NavigationResult;
import com.squid.kraken.v4.model.ObjectType;
import com.squid.kraken.v4.model.Problem;
import com.squid.kraken.v4.model.Problem.Severity;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.ResultInfo;
import com.squid.kraken.v4.model.ValueType;
import com.squid.kraken.v4.model.ViewQuery;
import com.squid.kraken.v4.model.ViewReply;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.vegalite.VegaliteSpecs;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.DataType;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Encoding;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Mark;
import com.squid.kraken.v4.vegalite.VegaliteSpecs.Stacked;

/**
 * This call is responsible for generating the HTML code for the API
 * @author sergefantino
 *
 */
public class AnalyticsServiceHTMLGenerator implements AnalyticsServiceConstants {

	private AnalyticsServiceBaseImpl service;

	/**
	 *
	 */
	public AnalyticsServiceHTMLGenerator(AnalyticsServiceBaseImpl service) {
		this.service = service;
	}

	private String getToken() {
		return service.getUserContext().getToken().getOid();
	}

	private void createHTMLtitle(AppContext userContext, StringBuilder html, String title, String BBID, Project project, Space space, URI backLink, String docAnchor, String method) {

		html.append("<div style='background-color: #ee7914;padding: 15px;'><img src='https://apps.openbouquet.io/release/obe/bouquet-logo-white.png' height='24' ></div>");
		html.append("</div>");
		html.append("<div class='overview' style='background-color:#E0E0E0;padding:5px;'>");
		html.append("<h3>");

		if (title!=null) {
			html.append(title);
		}
		html.append("<div class='pull-right'><a target='OB API DOC' href='https://api-docs.openbouquet.io/"+(docAnchor!=null?docAnchor:"")+"' ><span class=\"label label-info\"><i class=\"fa fa-book\" aria-hidden=\"true\"></i>&nbsp;API doc</span></a></div>");
		html.append("</h3>");
		if (space!=null) {
			URI projectLink = service.buildGenericObjectURI(userContext, space.getUniverse().getProject().getId());
			html.append("<p>project:&nbsp;<a href=\""+StringEscapeUtils.escapeHtml4(backLink.toString())+"\"><kbd>'"+space.getUniverse().getProject().getName()+"'</kbd></a>&nbsp;(id=<a href=\""+StringEscapeUtils.escapeHtml4(projectLink.toString())+"\"><kbd>@'"+space.getUniverse().getProject().getOid()+"'</kbd></a>)");
			URI domainLink = service.buildGenericObjectURI(userContext, space.getDomain().getId());
			html.append("&nbsp;/&nbsp;domain:&nbsp;<kbd>'"+space.getDomain().getName()+"'</kbd>&nbsp;(id=<a href=\""+StringEscapeUtils.escapeHtml4(domainLink.toString())+"\"><kbd>@'"+space.getDomain().getOid()+"'</kbd></a>)");
			if (space.getBookmark()!=null) {
				html.append("&nbsp;/&nbsp;bookmark:&nbsp;<a href=\""+StringEscapeUtils.escapeHtml4(backLink.toString())+"\"><kbd>'"+space.getBookmark().getName()+"'</kbd></a>&nbsp;(id=<kbd>@'"+space.getBookmark().getOid()+"'</kbd>)");
			}
			html.append("</p>");
		} else if (project!=null) {
			// just display the project
			URI projectLink = service.buildGenericObjectURI(userContext, project.getId());
			html.append("<p>project:&nbsp;<a href=\""+StringEscapeUtils.escapeHtml4(backLink.toString())+"\"><kbd>'"+project.getName()+"'</kbd></a>&nbsp;(id=<a href=\""+StringEscapeUtils.escapeHtml4(projectLink.toString())+"\"><kbd>@'"+project.getOid()+"'</kbd></a>)");
			html.append("</p>");
		}

		createHTMLAPIpanel(html, "scopeAnalysis");
		//
		html.append("</div>");//overview
	}

	private StringBuilder createHTMLHeader(String title) {
		StringBuilder html = new StringBuilder("<!DOCTYPE html><html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\"><head><meta charset='utf-8'><title>"+title+"</title>");
		// bootstrap & jquery
		html.append("<!-- Latest compiled and minified CSS -->\n" +
				"<link rel=\"stylesheet\" href=\"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css\">\n" +
				"\n" +
				"<!-- jQuery library -->\n" +
				"<script src=\"https://ajax.googleapis.com/ajax/libs/jquery/3.1.1/jquery.min.js\"></script>\n" +
				"\n" +
				"<!-- Latest compiled JavaScript -->\n" +
				"<script src=\"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js\"></script>\n");
		// datepicker
		html.append("<script src=\"https://cdnjs.cloudflare.com/ajax/libs/bootstrap-datepicker/1.6.4/css/bootstrap-datepicker.css\"></script>\n");
		html.append("<script src=\"https://cdnjs.cloudflare.com/ajax/libs/bootstrap-datepicker/1.6.4/js/bootstrap-datepicker.min.js\"></script>\n");
		// font
		html.append("<link rel=\"stylesheet\" href=\"https://use.fontawesome.com/1ea8d4d975.css\">");
		// style
		html.append("<style>"
				+ "* {font-family: \"Helvetica Neue\",Helvetica,Arial,sans-serif; color: #666; }"
				+ "table.data {border-collapse: collapse;width: 100%;}"
				+ "th, td {text-align: left;padding: 8px; vertical-align: top;}"
				+ ".data tr:nth-child(even) {background-color: #f2f2f2}"
				+ ".data th {background-color: grey;color: white;}"
				+ ".vega-actions a {margin-right:10px;}"
				+ "hr {border: none; "
				+ "color: Gainsboro ;\n" +
				"background-color: Gainsboro ;\n" +
				"height: 3px;}"
				+ "input[type=date], input[type=text], select {\n" +
				"    padding: 4px 4px;\n" +
				"    margin: 4px 0;\n" +
				"    display: inline-block;\n" +
				"    border: 1px solid #ccc;\n" +
				"    border-radius: 4px;\n" +
				"    box-sizing: border-box;\n" +
				"}\n" +
				"button[type=submit], .btn-orange {\n" +
				"	 font-size: 1.3em;" +
				"    width: 200px;\n" +
				"    background-color: #ee7914;\n" +
				"    color: white;\n" +
				"    padding: 14px 20px;\n" +
				"    margin: 0 auto;\n" +
				"    display: block;" +
				"    border: none;\n" +
				"    border-radius: 4px;\n" +
				"    cursor: pointer;\n" +
				"}\n" +
				"button[type=submit]:hover, .btn-orange:hover {\n" +
				"    background-color: #ab570e;\n" +
				"}\n" +
				"input[type=submit] {\n" +
				"	 font-size: 1.3em;" +
				"    width: 200px;\n" +
				"    background-color: #ee7914;\n" +
				"    color: white;\n" +
				"    padding: 14px 20px;\n" +
				"    margin: 0 auto;\n" +
				"    display: block;" +
				"    border: none;\n" +
				"    border-radius: 4px;\n" +
				"    cursor: pointer;\n" +
				"}\n" +
				"input[type=text].q {\n" +
				"    box-sizing: border-box;\n" +
				"    border: 2px solid #ccc;\n" +
				"    border-radius: 4px;\n" +
				"    font-size: 16px;\n" +
				"    background-color: white;\n" +
				"    background-position: 10px 10px; \n" +
				"    background-repeat: no-repeat;\n" +
				"    padding: 12px 20px 12px 40px;" +
				"}"+
				"input[type=submit]:hover {\n" +
				"    background-color: #ab570e;\n" +
				"}\n" +
				"fieldset {\n" +
				"    margin-top: 40px;\n" +
				"}\n" +
				"legend {\n" +
				"    font-size: 1.3em;\n" +
				"}\n" +
				"table.controls {\n" +
				"    border-collapse:separate; \n" +
				"    border-spacing: 0 20px;\n" +
				"}\n" +
				"body {\n" +
				"    margin-top: 0px;\n" +
				"    margin-bottom: 0px;\n" +
				"    margin-left: 5px;\n" +
				"    margin-right: 5px;\n" +
				"}\n" +
				".footer a, .footer a:hover, .footer a:visited {\n" +
				"    color: White;\n" +
				"}\n" +
				".footer {\n" +
				"    background: #5A5A5A;\n" +
				"    padding: 10px;\n" +
				"    margin: 20px -8px -8px -8px;\n" +
				"}\n" +
				".footer p {\n" +
				"    text-align: center;\n" +
				"    color: White;\n" +
				"    width: 100%;\n" +
				"}\n" +
				".header {\n" +
				"    margin: -8px 0 0 0;\n" +
				"}\n" +
				"h2 { margin-bottom:0px;}\n" +
				"h4 { margin-bottom:0px;}\n" +
				".logo span {\n" +
				"    line-height: 32px;\n" +
				"    vertical-align: middle;\n" +
				"    color: #5e5e5e;\n" +
				"    padding-left: 132px;\n" +
				"    font-size: 20px;\n" +
				"}\n" +
				".header hr {\n" +
				"    margin: 0 -8px 0 -8px;\n" +
				"}\n" +
				".period input {\n" +
				"    margin-left: 10px;\n" +
				"    margin-right: 10px;\n" +
				"}\n" +
				".instructions {\n" +
				"    font-size: 1.2em;\n" +
				"    font-style: italic;\n" +
				"}\n" +
				".tooltip-inner {\n" +
				"    min-width: 250px;n" +
				"    max-width: 500px;n" +
				"    background-color: #aaaaaa;" +
				"}" +
				// display the clear button
				"input[type=search]::-webkit-search-cancel-button {\n" + 
				"    -webkit-appearance: searchfield-cancel-button;\n" + 
				"}" +
				"</style>");
		// drag & drop support
		html.append("<script>\n" + 
				"function allowDrop(ev) {\n" + 
				"    ev.preventDefault();\n" + 
				"}\n" + 
				"function drag(ev, text) {\n" + 
				"    ev.dataTransfer.setData(\"text\", text);\n" + 
				"}\n" + 
				"function drop(ev) {\n" + 
				"    ev.preventDefault();\n" + 
				"    var data = ev.dataTransfer.getData(\"text\");\n" + 
				"    ev.target.value = data;\n" +
				"}\n" +
				"</script>");
		html.append("<link rel=\"icon\" type=\"image/png\" href=\"https://s3.amazonaws.com/openbouquet.io/favicon.ico\" />");
		html.append("</head><body>");
		return html;
	}


	private void createHTMLinputArray(StringBuilder html, String type, String name, List<? extends Object> values) {
		if (type.equals("text")) type="search";// use search type to have a clear button
		html.append("<div id='fields_"+name+"'>");
		if (values==null || values.isEmpty()) {
			html.append("<input type=\""+type+"\" style='width:100%;'  name=\""+name+"\" value=\"\" placeholder=\"type formula\"><br>");
		} else {
			boolean first = true;
			for (Object value : values) {
				if (!first) html.append("<br>"); else first=false;
				html.append("<input type=\""+type+"\" style='width:100%;'  name=\""+name+"\" value=\""+getFieldValue(value.toString())+"\">");
			}
			if (!first) html.append("<br>");
			//html.append("<input type=\""+type+"\" size=100 name=\""+name+"\" value=\"\" placeholder=\"type formula\">");
		}
		html.append("</div>");
		// add the add button
		html.append("<script type='text/javascript'>//<![CDATA[\n" +
				"\n" +
				"        function addField_"+name+"(ev){\n" +
				"            var container = document.getElementById(\"fields_"+name+"\");\n" +
				"            var input = document.createElement(\"input\");\n" +
				"            input.type = '"+type+"';\n" +
				"            input.style = 'width:100%;';\n" +
				"            input.name = '"+name+"';\n" +
				"            input.placeholder = 'type formula';\n" +
				"            if (ev!==undefined) {\n" +
				"    	     	ev.preventDefault();\n" + 
				"            	input.value = ev.dataTransfer.getData(\"text\");\n" +
				"            }\n" +
				"            container.appendChild(input);\n" +
				"            container.appendChild(document.createElement(\"br\"));\n" +
				"        }\n" +
				"//]]> \n" +
				"\n" +
				"</script>\n");
		html.append("<button ondragover='allowDrop(event)' ondrop='addField_"+name+"(event)' type='button' onclick='addField_"+name+"()'>Add <i class=\"fa fa-plus-circle\" aria-hidden=\"true\"></i></button>");
	}

	private void createHTMLpagination(StringBuilder html, AnalyticsQuery query, DataTable data) {
		if (data !=null) {
			long lastRow = (data.getStartIndex()+data.getRows().size());
			long firstRow = data.getRows().size()>0?(data.getStartIndex()+1):0;
			html.append("<div style='padding-top:5px;'>rows from "+firstRow+" to "+lastRow+" out of "+data.getTotalSize()+" records");
			if (data.getFullset()) {
				html.append(" (the query is complete)");
			} else {
				html.append(" (the query has more data)");
			}
			if (lastRow<data.getTotalSize()) {
				// go to next page
				HashMap<String, Object> override = new HashMap<>();
				override.put(START_INDEX_PARAM, lastRow);
				URI nextLink = service.buildAnalyticsQueryURI(service.getUserContext(), query, null, null, Style.HTML, override);
				html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(nextLink.toString())+"\">next</a>]");
			}
			html.append("</div><div>");
			if (data.isFromSmartCache()) {
				html.append("data from smart-cache, last computed "+data.getExecutionDate());
			} else if (data.isFromCache()) {
				html.append("data from cache, last computed "+data.getExecutionDate());
			} else {
				html.append("fresh data just computed at "+data.getExecutionDate());
			}
		} else {
			html.append("<div style='padding-top:5px;'>The query failed to execute, no data returned</div>");
		}
		// add links
		html.append("<div>");
		createHTMLdataLinks(html, query);
		html.append("</div>");
		html.append("</div><br>");
	}

	private void createHTMLdataLinks(StringBuilder html, AnalyticsQuery query) {
		// add links
		{ // for SQL
			URI sqlLink = service.buildAnalyticsQueryURI(service.getUserContext(), query, "SQL", null, Style.HTML, null);
			html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(sqlLink.toString())+"\">SQL</a>]");
		}
		{ // for JSON export
			URI jsonExport = service.buildAnalyticsQueryURI(service.getUserContext(), query, "TABLE", null, Style.HUMAN, null);
			html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(jsonExport.toString())+"\">JSON</a>]");
		}

	}

	private void createHTMLpagination(StringBuilder html, ViewQuery query, ResultInfo info) {
		if (info!=null) {
			long lastRow = (info.getStartIndex()+info.getPageSize());
			long firstRow = info.getTotalSize()>0?(info.getStartIndex()+1):0;
			html.append("<div style='padding-top:5px;'>rows from "+firstRow+" to "+lastRow+" out of "+info.getTotalSize()+" records");
			if (info.isComplete()) {
				html.append(" (the query is complete)");
			} else {
				html.append(" (the query has more data)");
			}
			if (lastRow<info.getTotalSize()) {
				// go to next page
				HashMap<String, Object> override = new HashMap<>();
				override.put(START_INDEX_PARAM, lastRow);
				URI nextLink = service.buildAnalyticsViewURI(service.getUserContext(), query, null, null, Style.HTML, override);
				html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(nextLink.toString())+"\">next</a>]");
			}
			html.append("</div>");
			if (info.isFromSmartCache()) {
				html.append("<div>data from smart-cache, last computed "+info.getExecutionDate()+"</div>");
			} else if (info.isFromCache()) {
				html.append("<div>data from cache, last computed "+info.getExecutionDate()+"</div>");
			} else {
				html.append("<div>fresh data just computed at "+info.getExecutionDate()+"</div>");
			}
		} else {
			html.append("<div style='padding-top:5px;'>No results</div>");
		}
	}

	/**
	 * Simple page with SQL prettyfied
	 * @param string
	 * @return
	 */
	public Response createHTMLsql(String sql) {
		StringBuilder html = new StringBuilder("<html><head>");
		html.append(
				"<script src='https://cdn.rawgit.com/google/code-prettify/master/loader/run_prettify.js?lang=sql'></script>");
		html.append("</head><body>");
		html.append(
				"<pre class='prettyprint lang-sql' style='white-space: pre-wrap;white-space: -moz-pre-wrap;white-space: -pre-wrap;white-space: -o-pre-wrap;word-wrap: break-word;padding:0px;margin:0px'>");
		html.append(StringEscapeUtils.escapeHtml4(sql));
		html.append("</pre>");
		html.append("</body></html>");
		return Response.ok(html.toString(),"text/html").build();
	}

	/**
	 * the navigation view
	 * @param ctx
	 * @param query
	 * @param result
	 * @return
	 */
	public Response createHTMLPageList(AppContext ctx, NavigationQuery query, NavigationResult result) {
		String title = (query.getParent()!=null && query.getParent().length()>0)?query.getParent():"Root";
		StringBuilder html = createHTMLHeader("OB Query Builder");
		// check if the parent is a project
		if (result.getParent()!=null && result.getParent().getType().equals(NavigationItem.PROJECT_TYPE) && result.getParent().getObjectID() instanceof ProjectPK) {
			ProjectPK id = (ProjectPK)result.getParent().getObjectID();
			try {
				Project project = ProjectManager.INSTANCE.getProject(ctx, id);
				createHTMLtitle(ctx, html, title, null, project, null, result.getParent().getUpLink(),"#list-available-content","listContent");
			} catch (ScopeException e) {
				// ignore
				createHTMLtitle(ctx, html, title, null, null, null, result.getParent().getUpLink(),"#list-available-content","listContent");
			}
		} else {
			createHTMLtitle(ctx, html, title, null, null, null, result.getParent().getUpLink(),"#list-available-content","listContent");
		}
		// form
		html.append("<form><table>");
		html.append("<tr><td><input size=50 class='q' type='text' name='q' placeholder='filter the list' value='"+(query.getQ()!=null?query.getQ():"")+"'></td>"
				+ "<td><input type=\"submit\" value=\"Filter\"></td></tr>");
		html.append("<input type='hidden' name='parent' value='"+(query.getParent()!=null?query.getParent():"")+"'>");
		if (query.getStyle()!=null)
			html.append("<input type='hidden' name='style' value='"+(query.getStyle()!=null?query.getStyle():"")+"'>");
		if (query.getVisibility()!=null)
			html.append("<input type='hidden' name='visibility' value='"+(query.getVisibility()!=null?query.getVisibility():"")+"'>");
		if (query.getHierarchy()!=null)
			html.append("<input type='hidden' name='hierarchy' value='"+query.getHierarchy()+"'>");
		html.append("<input type='hidden' name='access_token' value='"+ctx.getToken().getOid()+"'>");
		html.append("</table></form>");
		//
		// parent description
		if (result.getParent()!=null && result.getParent().getDescription()!=null && result.getParent().getDescription().length()>0) {
			html.append("<p><i>"+result.getParent().getDescription()+"</i></p>");
		}
		// content
		if (result.getChildren().isEmpty()) {
			html.append("<p><center>empty folder, nothing to show</center></p>");
		}
		html.append("<table style='border-collapse:collapse'>");
		for (NavigationItem item : result.getChildren()) {
			html.append("<tr>");
			html.append("<td valign='top'>"+item.getType()+"</td>");
			if (item.getLink()!=null) {
				html.append("<td valign='top'><a href=\""+StringEscapeUtils.escapeHtml4(item.getLink().toString())+"\">"+item.getName()+"</a>");
			} else {
				html.append("<td valign='top'>"+item.getName());
			}
			if (item.getObjectLink()!=null) {
				html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(item.getObjectLink().toString())+"\">info</a>]");
			}
			if (item.getViewLink()!=null) {
				html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(item.getViewLink().toString())+"\">view</a>]");
			}
			if (item.getDescription()!=null && item.getDescription().length()>0) {
				html.append("<br><i>"+(item.getDescription()!=null?item.getDescription():"")+"</i>");
			}
			html.append("</td>");
			if (item.getAttributes()!=null) {
				for (Entry<String, String> entry : item.getAttributes().entrySet()) {
					html.append("<td valign='top'>"+entry.getKey()+"="+entry.getValue()+"</td>");
				}
			}
		}
		html.append("</table>");
		createHTMLAPIpanel(html, "listContent");
		createFooter(html);
		html.append("</body></html>");
		return Response.ok(html.toString(),"text/html").build();
	}

	private static final DateFormat ISO8601_full = FacetBuilder.createUTCDateFormat(FacetBuilder.ISO8601_FULL_FORMAT);
	private static final DateFormat ISO8601_date = FacetBuilder.createUTCDateFormat("yyyy-MM-dd");

	private String formatDate(IDomain image, String iso8601) {
		if (image.isInstanceOf(IDomain.DATE)) {
			// reformat to a simple date
			try {
				Date date = ISO8601_full.parse(iso8601);
				return ISO8601_date.format(date);
			} catch (ParseException e) {
				return iso8601;
			}
		} else {
			return iso8601;
		}
	}

	/**
	 * The query view
	 * @param userContext
	 * @param dataTable
	 * @return
	 */
	public Response createHTMLPageTable(AppContext ctx, Space space, AnalyticsReply reply, DataTable data) {
		String title = space!=null?getPageTitle(space):null;
		String breadcrumbs =  space!=null?getBreadCrumbs(space):null;
		StringBuilder html ;
		if (title == null){
			html = createHTMLHeader("OB Query Builder");
		}else{
			html = createHTMLHeader(title+" - OB Query Builder");
		}
		AnalyticsQuery query = reply.getQuery();
		createHTMLtitle(ctx, html, breadcrumbs, query.getBBID(), null, space, getParentLink(space), "#query-a-bookmark-or-domain", "runAnalysis");
		html.append("<form id='queryForm' action='#results'>");

		// the parameters pannel
		html.append("<div>");
		html.append("<div style='width:50%;float:left;'>");
		html.append("<div style='padding-right:15px;'>");
		html.append("<h4 style='font-family:Helvetica Neue,Helvetica,Arial,sans-serif;'>Query Parameters</h4><hr>");
		html.append("<p id=\"query-instructions\"><i class=\"fa fa-info-circle fa-lg\" aria-hidden=\"true\"></i>&nbsp;You can either build expressions manually, or drag and drop objects from Query Scope to Query parameters.</p>");
		createHTMLfilters(html, query);
		html.append("<table style='width:100%;'>");
		html.append("<tr><td valign='top' style='width:100px;'><a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+GROUPBY_DOC+"\">groupBy:</a>");
		html.append("</td><td>");
		createHTMLinputArray(html, "text", "groupBy", query.getGroupBy());
		html.append("</td></tr>");
		html.append("<tr><td valign='top'><a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+METRICS_DOC+"\">metrics:</a>");
		html.append("</td><td>");
		createHTMLinputArray(html, "text", "metrics", query.getMetrics());
		html.append("</td></tr>");
		html.append("<tr><td valign='top'><a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+ROLLUP_DOC+"\">rollup:</a>");
		html.append("</td><td>");
		createHTMLinputArray(html, "text", "rollup", query.getRollups());
		html.append("</td></tr>");
		html.append("<tr><td valign='top'><a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+ORDERBY_DOC+"\">orderBy:</a>");
		html.append("</td><td>");
		createHTMLinputArray(html, "text", "orderBy", query.getOrderBy());
		html.append("</td></tr>");
		html.append("<tr><td valign='top'><a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+LIMIT_DOC+"\">limit:</a>");
		html.append("</td><td>");
		html.append("<input type=\"number\" required name=\"limit\" value=\""+getFieldValue(query.getLimit(),0)+"\">");
		html.append("</td></tr>");
		html.append("<tr><td valign='top'><a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+OFFSET_DOC+"\">offset:</a>");
		html.append("</td><td>");
		html.append("<input type=\"number\" required name=\"offset\" value=\""+getFieldValue(query.getOffset(),0)+"\">");
		html.append("</td></tr>");
		html.append("<tr><td valign='top'><a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+MAX_RESULTS_DOC+"\">maxResults:</a>");
		html.append("</td><td>");
		html.append("<input type=\"number\" required name=\"maxResults\" value=\""+getFieldValue(query.getMaxResults(),100)+"\">");
		html.append("</td></tr>");
		html.append("<tr><td valign='top'><a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+START_INDEX_DOC+"\">startIndex:</a>");
		html.append("</td><td>");
		html.append("<input type=\"number\" required name=\"startIndex\" value=\""+getFieldValue(query.getStartIndex(),0)+"\">");
		html.append("</td></tr>");
		html.append("<tr><td valign='top'><a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+BEYOND_LIMIT_PARAM+"\">"+BEYOND_LIMIT_PARAM+":</a>");
		html.append("</td><td>");
		createHTMLinputArray(html, "text", AnalyticsServiceConstants.BEYOND_LIMIT_PARAM, query.getBeyondLimit());
		html.append("</td></tr>");
		html.append("</table>"
				+ "<input type=\"hidden\" name=\"style\" value=\"HTML\">"
				+ "<input type=\"hidden\" name=\"access_token\" value=\""+ctx.getToken().getOid()+"\">");
		createHTMLswaggerLink(html, "runAnalysis");
		html.append("</form>");
		// the scope panel
		html.append("</div></div><div style='width:50%;float:left;'>");
		if (space!=null) createHTMLscope(ctx, html, space, query);
		html.append("</div>");
		html.append("</div>");
		createHTMLAPIpanel(html, "runAnalysis");

		//preview
		html.append("<table border=0><tr>");
		html.append("<td>");
		html.append("<div style='float:left;padding:5px'><button type='submit' value='Query'><i class=\"fa fa-refresh\" aria-hidden=\"true\"></i>&nbsp;Query</button></div><br><br><br>");
		html.append("</td>");
		html.append("<td>");
		createHTMLpagination(html, query, data);
		html.append("</td>");
		html.append("</tr></table>");

		boolean hasError = (data == null);
		createHTMLproblems(html, query.getProblems());		
		// query result
		html.append("<a id='results'></a>");
		if (!hasError) {
			html.append("<h4 style='font-family:Helvetica Neue,Helvetica,Arial,sans-serif;'>Query Result</h4><hr>");
			// display selection
			createHTMLQuerySelection(html, space, reply.getSelection());
			html.append("<div style='max-height:600px;overflow:scroll;'>");
			html.append("<table class='data'>");
			html.append("<table class='data'><thead><tr>");
			html.append("<th></th>");
			for (Col col : data.getCols()) {
				html.append("<th>"+col.getName()+"</th>");
			}
			html.append("</tr>");
			html.append("</thead><tbody>");
			int i=1;
			if (query.getStartIndex()!=null) {
				i=query.getStartIndex()+1;
			}
			for (Row row : data.getRows()) {
				html.append("<tr>");
				html.append("<td valign='top'>#"+(i++)+"</td>");
				for (Col col : data.getCols()) {
					Object value = row.getV()[col.getPos()];
					if (value!=null) {
						if (col.getFormat()!=null && col.getFormat().length()>0) {
							try {
								value = String.format(col.getFormat(), value);
							} catch (IllegalFormatException e) {
								// ignore
							}
						}
					} else {
						value="";
					}
					html.append("<td valign='top'>"+value+"</td>");
				}
				html.append("</tr>");
			}
			html.append("</tbody></table>");
			html.append("</div>");
		} else {
			html.append("<i>Result is not available, it's probably due to an error</i>");
			html.append("<div style='clear:both;'></div>");
		}

		if (!hasError) {
			{ // for View
				HashMap<String, Object> override = new HashMap<>();
				override.put(LIMIT_PARAM, null);
				override.put(MAX_RESULTS_PARAM, null);
				URI viewLink = service.buildAnalyticsViewURI(service.getUserContext(), new ViewQuery(query), null, "ALL", Style.HTML, override);//(userContext, query, "SQL", null, Style.HTML, null);
				html.append("<div style='padding:13px'><button type='submit' class='btn btn-lg' style='display:inline' value='Visualize' formaction=\""+StringEscapeUtils.escapeHtml4(viewLink.toString())+"\"><i class=\"fa fa-bar-chart\" aria-hidden=\"true\"></i>&nbsp;View</button>");
			}
			// save as bookmark using a modal
			String popupTitle;
			if (space.hasBookmark()) {
				popupTitle = "Update Bookmark";
			}else{
				popupTitle="Save as new Bookmark";
			}
			UriBuilder builder = service.getPublicBaseUriBuilder().
					path("/analytics/{"+BBID_PARAM_NAME+"}/bookmark");
			builder.queryParam("access_token", service.getUserContext().getToken().getOid());
			URI link = builder.build(query.getBBID());
			html.append("<!-- Button trigger modal -->\n" + 
					"<button type=\"submit\" class=\"btn btn-lg\" style='display:inline' data-toggle=\"modal\" data-target=\"#myModal\">\n" + 
					"<i class=\"fa fa-cloud-upload\" aria-hidden=\"true\"></i> Bookmark\n" + 
					"</button></div>\n" + 
					"<!-- Modal -->\n" + 
					"<div class=\"modal fade\" id=\"myModal\" tabindex=\"-1\" role=\"dialog\" aria-labelledby=\"myModalLabel\" aria-hidden=\"true\">\n" + 
					"  <div class=\"modal-dialog modal-lg\" role=\"document\">\n" + 
					"    <div class=\"modal-content\">\n" + 
					"      <div class=\"modal-header\">\n" + 
					"        <button type=\"button\" class=\"close\" data-dismiss=\"modal\" aria-label=\"Close\">\n" + 
					"          <span aria-hidden=\"true\">&times;</span>\n" + 
					"        </button>\n" + 
					"        <h4 class=\"modal-title\" id=\"myModalLabel\">"+
					popupTitle+
					"</h4>\n" + 
					"      </div>\n" + 
					"      <div class=\"modal-body\">\n");
			// body
			html.append("<label for=\"bookmark-name\" class=\"form-control-label\">Name:</label>\n" + 
					"              <input type=\"text\" class=\"form-control\" id=\"bookmark-name\" name=\"name\" value=\""+getBookmarkName(space)+"\">");
			html.append("<input  type='hidden' class=\"form-control\" id=\"bookmark-parent\" name=\"parent\" value=\"/SHARED\">");

			// footer
			html.append("      </div>\n" + 
					"      <div class=\"modal-footer\">\n" + 
					"        &nbsp;<button style='margin-left:10px;' type=\"button\" class=\"btn btn-secondary pull-right\" data-dismiss=\"modal\">Cancel</button>&nbsp;\n" + 
					"        &nbsp;<button style='margin-left:10px;' type=\"button\" class=\"btn btn-primary pull-right\" id='saveBoookmark'>Save</button>&nbsp;\n" + 
					"      </div>\n" + 
					"    </div>\n" + 
					"  </div>\n" + 
					"</div>");
			html.append("<script>");
			UriBuilder builder2 = service.getPublicBaseUriBuilder().
					path("/analytics/");

			html.append("var onSaveBM = function(data) { \n"
					+ "window.location.href='" + builder2.build() +"'+data.reference+'/query?style=HTML&access_token="+service.getUserContext().getToken().getOid()+"';"
					+ "\n}");
			html.append("\n");
			html.append("$('#saveBoookmark').on('click', function(){$.ajax({type : 'POST', url: \""
					+ StringEscapeUtils.escapeHtml4(link.toString())
					+ "\", dataType : 'json',"
					+ " success : onSaveBM,"
					+ " error : function(xhr) {alert('an error occurred');},"
					+ " data : $('#queryForm').serialize()})})");
			html.append("</script>");
		}
		createFooter(html);
		html.append("</body></html>");
		return Response.ok(html.toString(),"text/html").build();
	}

	/**
	 * @param html
	 * @param reply 
	 * @param space 
	 */
	private void createHTMLQuerySelection(StringBuilder html, Space space, AnalyticsSelection selection) {
		if (selection!=null) {
			String message = "";
			if (selection.getPeriod()!=null && !selection.getPeriod().equals("")) {
				SpaceScope scope = new SpaceScope(space);
				IDomain image = scope.parseExpressionSafe(selection.getPeriod()).getImageDomain();
				message += "results for the period <kbd>"+selection.getPeriod()+"</kbd> ";
				if (selection.getTimeframe()!=null && selection.getTimeframe().size()==2) {
					message += "&nbsp;from <kbd>"+formatDate(image, selection.getTimeframe().get(0))+"</kbd>&nbsp;to <kbd>"+formatDate(image, selection.getTimeframe().get(1))+"</kbd> ";
				} else if (selection.getTimeframe()!=null && selection.getTimeframe().size()==1) {
					message += "&nbsp;for the <kbd>"+formatDate(image, selection.getTimeframe().get(0))+"</kbd> ";
				}
				if (selection.getCompareTo()!=null && selection.getCompareTo().size()==2) {
					message += "&nbsp;compare to the <kbd>"+formatDate(image, selection.getCompareTo().get(0))+"</kbd> up to the <kbd>"+formatDate(image, selection.getCompareTo().get(1))+"</kbd> ";
				} else if (selection.getCompareTo()!=null && selection.getCompareTo().size()==1) {
					message += "&nbsp;compare to the <kbd>"+formatDate(image, selection.getCompareTo().get(0))+"</kbd> ";
				}
			}
			html.append("<p>"+message+"</p>");
		}
	}

	private String getBookmarkName(Space space) {
		if (space.getBookmark()==null) {
			return space.getDomain().getName()+" Bookmark";
		} else {
			return space.getBookmark().getName();
		}
	}
	
	public Response createHTMLPageViewSnippet(AppContext ctx, Space space, ViewQuery view, ResultInfo info, ViewReply reply) {
		String title = getPageTitle(space);
		StringBuilder html = createHTMLHeader(title);
		html.append("<script src=\"//d3js.org/d3.v3.min.js\" charset=\"utf-8\"></script>\n" +
				"<script src=\"https://cdnjs.cloudflare.com/ajax/libs/vega/2.5.0/vega.min.js\"></script>\n" +
				"  <script src=\"https://cdnjs.cloudflare.com/ajax/libs/vega-lite/1.0.7/vega-lite.min.js\"></script>\n" +
				"<script src=\"https://cdnjs.cloudflare.com/ajax/libs/vega-embed/2.2.0/vega-embed.min.js\" charset=\"utf-8\"></script>");
		html.append("<body>");
		// display selection
		//createHTMLQuerySelection(html, space, reply.getSelection());
		// vega lite preview
		html.append("<div>");
		Encoding channels = null;
		if (reply.getResult()!=null) {
			html.append("<div id=\"vis\"></div>\r\n\r\n<script>\r\nvar embedSpec = {\r\n  mode: \"vega-lite\", renderer:\"svg\",  spec:");
			html.append(writeVegalightSpecs(reply.getResult()));
			channels = reply.getResult().encoding;
			html.append("}\r\nvg.embed(\"#vis\", embedSpec, function(error, result) {\r\n  // Callback receiving the View instance and parsed Vega spec\r\n  // result.view is the View, which resides under the '#vis' element\r\n});\r\n</script>\r\n");
		} else {
			html.append("<p>No Result</p>");
			channels = new Encoding();// empty encoding
		}
		html.append("</body>\r\n</html>");
		return Response.ok(html.toString(), "text/html; charset=UTF-8").build();
	}
	
	private static final Mark[] MARKS = new Mark[]{Mark.bar, Mark.line, Mark.area, Mark.point ,Mark.circle, Mark.square, Mark.tick};

	/**
	 * the /view view (vegalite)
	 * @param userContext
	 * @param space
	 * @param view
	 * @param info
	 * @param reply
	 * @return
	 */
	public Response createHTMLPageView(AppContext ctx, Space space, ViewQuery view, ResultInfo info, ViewReply reply) {
		String title = getPageTitle(space);
		String breadcrumbs = getBreadCrumbs(space);
		StringBuilder html = createHTMLHeader( title + " - OB Query Viewer");
		html.append("<script src=\"//d3js.org/d3.v3.min.js\" charset=\"utf-8\"></script>\n" +
				"<script src=\"https://cdnjs.cloudflare.com/ajax/libs/vega/2.5.0/vega.min.js\"></script>\n" +
				"  <script src=\"https://cdnjs.cloudflare.com/ajax/libs/vega-lite/1.0.7/vega-lite.min.js\"></script>\n" +
				"<script src=\"https://cdnjs.cloudflare.com/ajax/libs/vega-embed/2.2.0/vega-embed.min.js\" charset=\"utf-8\"></script>");
		html.append("<body>");
		createHTMLtitle(ctx, html, breadcrumbs, view.getBBID(), null, space, getParentLink(space),"#view-a-bookmark-or-domain", "viewAnalysis");
		createHTMLproblems(html, reply.getQuery().getProblems());
		// vega lite preview
		html.append("<div>");
		html.append("<h4 style='font-family:Helvetica Neue,Helvetica,Arial,sans-serif;'>View Result</h4><hr>");
		Encoding channels = null;
		if (reply.getResult()!=null) {
			// display selection
			createHTMLQuerySelection(html, space, reply.getSelection());
			// toolbar
			html.append("<div class='btn-toolbar' role='toolbar' aria-label='vegalite settings'>");
			boolean separator = false;
			boolean group = false;
			{ // for View snippet
				HashMap<String, Object> override = new HashMap<>();
				URI viewLink = service.buildAnalyticsViewURI(service.getUserContext(), view, null, "ALL", Style.SNIPPET, override);//(userContext, query, "SQL", null, Style.HTML, null);
				html.append("<a class='btn btn-default' target='_blank' href=\""+StringEscapeUtils.escapeHtml4(viewLink.toString())+"\"><i class='fa fa-external-link' aria-hidden='true'></i>&nbsp;Snippet</a>");
				separator = true;
			}
			{// bin x ?
				if (reply.getResult().encoding.x!=null && reply.getResult().encoding.x.type==DataType.quantitative && reply.getResult().encoding.x.bin==false && reply.getResult().encoding.x.aggregate==null) {
					try {
						ViewQuery copy = new ViewQuery(view);
						Properties options = copy.getOptionsAsProperties();
						options.put("channel.x.bin","true");
						copy.setOptionsAsProperties(options);
						if (separator) html.append("<div class='btn-group' role='group'>");
						URI viewLink = service.buildAnalyticsViewURI(service.getUserContext(), copy, null, "ALL", Style.HTML, null);
						html.append("<a class='btn btn-default' href=\""+StringEscapeUtils.escapeHtml4(viewLink.toString())+"\">bin(X)</a>");
						separator = false;
						group = true;
					} catch (IOException e) {
						// ignore
					}
				}
				if (reply.getResult().encoding.x!=null && reply.getResult().encoding.x.type==DataType.quantitative && reply.getResult().encoding.x.bin==true) {
					try {
						ViewQuery copy = new ViewQuery(view);
						Properties options = copy.getOptionsAsProperties();
						options.put("channel.x.bin","false");
						copy.setOptionsAsProperties(options);
						if (separator) html.append("<div class='btn-group' role='group'>");
						URI viewLink = service.buildAnalyticsViewURI(service.getUserContext(), copy, null, "ALL", Style.HTML, null);
						html.append("<a class='btn btn-default' href=\""+StringEscapeUtils.escapeHtml4(viewLink.toString())+"\"><i class='fa fa-check' aria-hidden='true'></i> bin(X)</a>");
						separator = false;
						group = true;
					} catch (IOException e) {
						// ignore
					}
				}
			}
			{// x<->y
				if (reply.getResult().encoding.x!=null && reply.getResult().encoding.x.type!=DataType.temporal) {
					if (separator) html.append("<div class='btn-group' role='group'>");
					ViewQuery copy = new ViewQuery(view);
					copy.setX(view.getY());
					copy.setY(view.getX());
					URI viewLink = service.buildAnalyticsViewURI(service.getUserContext(), copy, null, "ALL", Style.HTML, null);
					html.append("<a class='btn btn-default' href=\""+StringEscapeUtils.escapeHtml4(viewLink.toString())+"\">X <i class='fa fa-refresh' aria-hidden='true'></i> Y</a>");
					separator = false;
					group = true;
				}
			}
			{// x<->color
				if ((reply.getResult().encoding.x!=null && reply.getResult().encoding.x.type!=DataType.temporal) && reply.getResult().encoding.color!=null) {
					if (separator)  html.append("<div class='btn-group' role='group'>");
					ViewQuery copy = new ViewQuery(view);
					copy.setX(view.getColor());
					copy.setColor(view.getX());
					URI viewLink = service.buildAnalyticsViewURI(service.getUserContext(), copy, null, "ALL", Style.HTML, null);
					html.append("<a class='btn btn-default' href=\""+StringEscapeUtils.escapeHtml4(viewLink.toString())+"\">X <i class='fa fa-refresh' aria-hidden='true'></i> color</a>");
					separator = false;
					group = true;
				}
			}
			{// color<->size
				if (reply.getResult().encoding.color!=null || reply.getResult().encoding.size!=null) {
					if (separator) html.append("<div class='btn-group' role='group'>");
					ViewQuery copy = new ViewQuery(view);
					copy.setColor(view.getSize());
					copy.setSize(view.getColor());
					URI viewLink = service.buildAnalyticsViewURI(service.getUserContext(), copy, null, "ALL", Style.HTML, null);
					html.append("<a class='btn btn-default' href=\""+StringEscapeUtils.escapeHtml4(viewLink.toString())+"\">color <i class='fa fa-refresh' aria-hidden='true'></i> size</a>");
					separator = false;
					group = true;
				}
			}
			{// color<->column
				if (reply.getResult().encoding.color!=null || reply.getResult().encoding.column!=null) {
					if (separator) html.append("<div class='btn-group' role='group'>");
					ViewQuery copy = new ViewQuery(view);
					copy.setColor(view.getColumn());
					copy.setColumn(view.getColor());
					URI viewLink = service.buildAnalyticsViewURI(service.getUserContext(), copy, null, "ALL", Style.HTML, null);
					html.append("<a class='btn btn-default' href=\""+StringEscapeUtils.escapeHtml4(viewLink.toString())+"\">color <i class='fa fa-refresh' aria-hidden='true'></i> column</a>");
					separator = false;
					group = true;
				}
			}
			{// column<->row
				if (reply.getResult().encoding.column!=null || reply.getResult().encoding.row!=null) {
					if (separator) html.append("<div class='btn-group' role='group'>");
					ViewQuery copy = new ViewQuery(view);
					copy.setColumn(view.getRow());
					copy.setRow(view.getColumn());
					URI viewLink = service.buildAnalyticsViewURI(service.getUserContext(), copy, null, "ALL", Style.HTML, null);
					html.append("<a class='btn btn-default' href=\""+StringEscapeUtils.escapeHtml4(viewLink.toString())+"\">column <i class='fa fa-refresh' aria-hidden='true'></i> row</a>");
					separator = false;
					group = true;
				}
			}
			if (group==true) {
				html.append("</div>");
				separator = true;
				group = false;
			}
			{ // set mark option
				for (Mark mark : MARKS) {
					ViewQuery copy = new ViewQuery(view);
					try {
						// line and area are ony valid with ordinal/quantitative
						if (mark.equals(Mark.line) || mark.equals(Mark.area)) {
							if (!(reply.getResult().encoding.x!=null && (reply.getResult().encoding.x.type.equals(DataType.ordinal) || reply.getResult().encoding.x.type.equals(DataType.temporal))
									&& reply.getResult().encoding.y!=null && reply.getResult().encoding.y.type.equals(DataType.quantitative))) {
								continue;
							}
						}
						Properties options = copy.getOptionsAsProperties();
						options.put("mark", mark.name());
						copy.setOptionsAsProperties(options);
						URI viewLink = service.buildAnalyticsViewURI(service.getUserContext(), copy, null, "ALL", Style.HTML, null);
						if (separator) html.append("<div class='btn-group' role='group'>");
						if (reply.getResult().mark.equals(mark)) {
							html.append("<button type='button' class='btn btn-default disabled'>");
							html.append("<i class='fa fa-check' aria-hidden='true'></i> "+mark.toString());
							html.append("</button>");
						} else {
							html.append("<a class='btn btn-default' href=\""+StringEscapeUtils.escapeHtml4(viewLink.toString())+"\">"+mark.toString()+"</a>");
						}
						separator = false;
						group = true;
					} catch (IOException e) {
						//
					}
				}
			}
			if (group==true) {
				html.append("</div>");
				separator = true;
				group = false;
			}
			// stacked options
			if ((reply.getResult().mark.equals(Mark.bar) || reply.getResult().mark.equals(Mark.area)) && reply.getResult().encoding.color!=null) {
				html.append("<div class='btn-group' role='group'>");
				createHTMLpageViewStackedOptions(html, view, reply);
				html.append("</div>");
			}
			{// reset
				if (true) {
					ViewQuery copy = new ViewQuery((AnalyticsQuery)view);
					URI viewLink = service.buildAnalyticsViewURI(service.getUserContext(), copy, null, "ALL", Style.HTML, null);
					html.append("<a class='btn btn-default' href=\""+StringEscapeUtils.escapeHtml4(viewLink.toString())+"\"><i class='fa fa-times' aria-hidden='true'></i> reset</a>");
				}
			}
			html.append("</div>");
			// vegalite
			html.append("<div id=\"vis\"></div>\r\n\r\n<script>\r\nvar embedSpec = {\r\n  mode: \"vega-lite\", renderer:\"svg\",  spec:");
			html.append(writeVegalightSpecs(reply.getResult()));
			channels = reply.getResult().encoding;
			html.append("}\r\nvg.embed(\"#vis\", embedSpec, function(error, result) {\r\n  // Callback receiving the View instance and parsed Vega spec\r\n  // result.view is the View, which resides under the '#vis' element\r\n});\r\n</script>\r\n");
		} else {
			html.append("<p>No Result</p>");
			channels = new Encoding();// empty encoding
		}
		html.append("<hr>");
		// refresh
		html.append("<form>");
		html.append("<div style='float:left;padding:5px;'><button type='submit' value='View'><i class=\"fa fa-bar-chart\" aria-hidden=\"true\"></i>&nbsp;View</button></div>");
		// data-link
		URI querylink = service.buildAnalyticsQueryURI(service.getUserContext(), reply.getQuery(), "RECORDS", "ALL", Style.HTML, null);
		html.append("<div style='float:left;padding:5px;'><button type='submit' value='Query' formaction=\""+StringEscapeUtils.escapeHtml4(querylink.toASCIIString())+"\"><i class=\"fa fa-refresh\" aria-hidden=\"true\"></i>&nbsp;Query</button></div>");
		createHTMLpagination(html, view, info);
		html.append("<div style='clear:both;'></div>");
		// parameters
		html.append("<div>");
		html.append("<div style='width:50%;float:left;'>");
		html.append("<div style='padding-right:15px;'>");
		html.append("<h4 style='font-family:Helvetica Neue,Helvetica,Arial,sans-serif;'>View Parameters</h4><hr>");
		html.append("<p id=\"query-instructions\"><i class=\"fa fa-info-circle fa-lg\" aria-hidden=\"true\"></i>&nbsp;You can either build expressions manually, or drag and drop objects from Query Scope to Query parameters.</p>");
		// view parameter
		html.append("<table style='width:100%;'>"
				+ "<tr><td style='width:100px;'>x</td><td><input type=\"search\" style='width:100%;' name=\"x\" value=\""+getFieldValue(view.getX())+"\"><br>"+(channels.x!=null?"as <b>"+channels.x.field+"</b>":"")+"</td></tr>"
				+ "<tr><td>y</td><td><input type=\"search\" style='width:100%;' name=\"y\" value=\""+getFieldValue(view.getY())+"\"><br>"+(channels.y!=null?"as <b>"+channels.y.field+"</b>":"")+"</td></tr>"
				+ "<tr><td>color</td><td><input type=\"search\" style='width:100%;' name=\"color\" value=\""+getFieldValue(view.getColor())+"\"><br>"+(channels.color!=null?"as <b>"+channels.color.field+"</b>":"")+"</td></tr>"
				+ "<tr><td>size</td><td><input type=\"search\" style='width:100%;' name=\"size\" value=\""+getFieldValue(view.getSize())+"\"><br>"+(channels.size!=null?"as <b>"+channels.size.field+"</b>":"")+"</td></tr>"
				+ "<tr><td>column</td><td><input type=\"search\" style='width:100%;' name=\"column\" value=\""+getFieldValue(view.getColumn())+"\"><br>"+(channels.column!=null?"as <b>"+channels.column.field+"</b>":"")+"</td></tr>"
				+ "<tr><td>row</td><td><input type=\"search\" style='width:100%;' name=\"row\" value=\""+getFieldValue(view.getRow())+"\"><br>"+(channels.row!=null?"as <b>"+channels.row.field+"</b>":"")+"</td></tr>");
		// options
		html.append("<tr><td>options</td><td><input type=\"search\" style='width:100%;' name=\"row\" value=\""+getFieldValue(view.getOptions())+"\"></td></tr>");
		html.append("</table>");
		// the other (query) parameters
		html.append("<h4 style='font-family:Helvetica Neue,Helvetica,Arial,sans-serif;'>Query Parameters</h4><hr>");
		html.append("<p>Those parameters are inherited from the /query API</p>");
		// filters
		createHTMLfilters(html, reply.getQuery());
		// metrics -- display the actual metrics
		html.append("<table style='width:100%;'>");
		html.append("<tr><td valign='top' style='width:100px;'><a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+GROUPBY_DOC+"\">groupBy:</a");
		html.append("</td><td>");
		createHTMLinputArray(html, "text", "groupBy", reply.getQuery().getGroupBy());
		html.append("</td></tr>");
		html.append("<tr><td valign='top' style='width:100px;'><a href=\"#\" data-html='true' data-toggle=\"tooltip\" data-placement=\"right\" title=\"Use the metrics parameters if you want to view multiple metrics on the same graph. Then you can use the <b>__VALUE</b> expression in channel to reference the metrics' value, and the <b>__METRICS</b> to get the metrics' name as a series.<br>If you need only a single metrics, you can directly define it in a channel, e.g. <code>y=count()</code>.<br>"+METRICS_DOC+"\">metrics:</a>");
		html.append("</td><td>");
		createHTMLinputArray(html, "text", "metrics", reply.getQuery().getMetrics());
		html.append("</td></tr>");
		html.append("<tr><td valign='top'><a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+ORDERBY_DOC+"\">orderBy:</a>");
		html.append("</td><td>");
		createHTMLinputArray(html, "text", "orderBy", reply.getQuery().getOrderBy());
		html.append("</td></tr>");
		// limits, maxResults, startIndex
		html.append("<tr><td valign='top'><a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+LIMIT_DOC+"\">limit:</a>");
		html.append("</td><td>");
		html.append("<input type=\"text\" name=\"limit\" value=\""+getFieldValue(reply.getQuery().getLimit(),0)+"\">");
		html.append("</td></tr>");
		html.append("<tr><td valign='top'><a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+OFFSET_DOC+"\">offset:</a>");
		html.append("</td><td>");
		html.append("<input type=\"text\" name=\"offset\" value=\""+getFieldValue(reply.getQuery().getOffset(),0)+"\">");
		html.append("</td></tr>");
		html.append("<tr><td valign='top'><a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+MAX_RESULTS_DOC+"\">maxResults:</a>");
		html.append("</td><td>");
		html.append("<input type=\"text\" name=\"maxResults\" value=\""+getFieldValue(reply.getQuery().getMaxResults(),100)+"\">");
		html.append("</td></tr>");
		html.append("<tr><td valign='top'><a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+START_INDEX_DOC+"\">startIndex:</a>");
		html.append("</td><td>");
		html.append("<input type=\"text\" name=\"startIndex\" value=\""+getFieldValue(reply.getQuery().getStartIndex(),0)+"\">");
		html.append("</td></tr>");
		html.append("<tr><td valign='top'><a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+BEYOND_LIMIT_PARAM+"\">"+BEYOND_LIMIT_PARAM+":</a>");
		html.append("</td><td>");
		createHTMLinputArray(html, "text", AnalyticsServiceConstants.BEYOND_LIMIT_PARAM, reply.getQuery().getBeyondLimit());
		html.append("</td></tr>");
		html.append("</table>"
				+ "<input type=\"hidden\" name=\"style\" value=\"HTML\">"
				+ "<input type=\"hidden\" name=\"access_token\" value=\""+space.getUniverse().getContext().getToken().getOid()+"\">"
				//+ "<input type=\"submit\" value=\"Refresh\">"
				+ "</form>");
		html.append("</div>");
		html.append("</div>");
		html.append("</div>");
		html.append("</div>");
		// scope
		html.append("<div style='width:50%;float:left;'>");
		createHTMLscope(ctx, html, space, reply.getQuery());
		html.append("</div>");
		html.append("</div>");
		html.append("<div style='clear:both;'></div>");
		createHTMLAPIpanel(html, "viewAnalysis");
		createFooter(html);
		html.append("</body>\r\n</html>");
		return Response.ok(html.toString(), "text/html; charset=UTF-8").build();
	}
	
	private void createHTMLpageViewStackedOptions(StringBuilder html, ViewQuery view, ViewReply reply) {
		// set stacked option
		boolean separator = false;
		if (reply.getResult().mark.equals(Mark.bar) || reply.getResult().mark.equals(Mark.area)) {
			Stacked selected = null;
			if (reply.getResult().config.mark!=null) selected = reply.getResult().config.mark.stacked;
			for (Stacked stacked : Stacked.values()) {
				separator = createHTMLpageViewSelectOption(html, separator, view, "mark.stacked", stacked.name(), stacked.toString(), stacked.equals(selected));
			}
		}
	}
	
	private boolean createHTMLpageViewSelectOption(StringBuilder html, boolean separator, ViewQuery view, String option, String value, String name, boolean selected) {
		ViewQuery copy = new ViewQuery(view);
		try {
			Properties options = copy.getOptionsAsProperties();
			if (value!=null) {
				options.put(option, value);
			} else {
				options.remove(option);
			}
			copy.setOptionsAsProperties(options);
			URI viewLink = service.buildAnalyticsViewURI(service.getUserContext(), copy, null, "ALL", Style.HTML, null);
			if (!selected) {
				html.append("<a class='btn btn-default' href=\""+StringEscapeUtils.escapeHtml4(viewLink.toString())+"\">"+name+"</a>");
			} else {
				createHTMLpageViewSelectOption(html, false, view, "mark.stacked", null, "<i class='fa fa-check' aria-hidden='true'></i> "+name, false);
			}
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	/**
	 * create a filter HTML snippet
	 * @param query
	 * @return
	 */
	private void createHTMLfilters(StringBuilder html, AnalyticsQuery query) {
		html.append("<table style='width:100%;'><tr><td style='width:100px;'>");
		// period
		html.append("<a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+PERIOD_DOC+"\">period:</a>");
		html.append("</td><td colspan=2>");
		html.append("<input type='text' style='width:100%;' name='period' value='"+getFieldValue(query.getPeriod())+"'>");
		html.append("</td></tr>");
		// timeframe
		html.append("<tr><td>");
		html.append("<a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+TIMEFRAME_DOC+"\">timeframe:</a>");
		html.append("</td><td>");
		html.append("from:&nbsp;<input type='text' id='timeframe-lower' style='width:100%;' name='timeframe' value='"+getDate(query.getTimeframe(),0)+"'>");
		html.append("<script type=\"text/javascript\">$('#timeframe-lower').datepicker({format: \"yyyy-mm-dd\", forceParse: false});</script>");
		html.append("</td><td>");
		html.append("to:&nbsp;<input type='text' id='timeframe-upper' style='width:100%;' name='timeframe' value='"+getDate(query.getTimeframe(),1)+"'>");
		html.append("<script type=\"text/javascript\">$('#timeframe-upper').datepicker({format: \"yyyy-mm-dd\", forceParse: false});</script>");
		html.append("</td></tr>");
		// compare
		html.append("<tr><td>");
		html.append("<a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+COMPARETO_DOC+"\">compareTo:</a>");
		html.append("</td><td>");
		html.append("from:&nbsp;<input type='text' id='compareTo-lower' style='width:100%;' name='compareTo' value='"+getDate(query.getCompareTo(),0)+"'>");
		html.append("<script type=\"text/javascript\">$('#compareTo-lower').datepicker({format: \"yyyy-mm-dd\", forceParse: false});</script>");
		html.append("</td><td>");
		html.append("to:&nbsp;<input type='text'  id='compareTo-upper' style='width:100%;' name='compareTo' value='"+getDate(query.getCompareTo(),1)+"'>");
		html.append("<script type=\"text/javascript\">$('#compareTo-upper').datepicker({format: \"yyyy-mm-dd\", forceParse: false});</script>");
		html.append("</td></tr>");
		// filters
		html.append("<tr><td>");
		html.append("<a href=\"#\" data-toggle=\"tooltip\" data-placement=\"right\" title=\""+FILTERS_DOC+"\">filters:</a>");
		html.append("</td><td colspan=2>");
		/*
		if (query.getFilters()!=null && query.getFilters().size()>0) {
			for (String filter : query.getFilters()) {
				html.append("<input type='text' size=50 name='filters' value='"+getFieldValue(filter)+"'>&nbsp;");
			}
		}
		html.append("<input type='text' size=50 name='filters' value='' placeholder='type formula'>");
		 */
		createHTMLinputArray(html, "text", "filters", query.getFilters());
		html.append("</td></tr></table>");
	}

	private static final String axis_style = "display: inline-block;border:1px solid;border-radius:5px;background-color:LavenderBlush ;margin:1px;";
	private static final String metric_style = "display: inline-block;border:1px solid;border-radius:5px;background-color:Lavender;margin:1px;";
	private static final String func_style = "display: inline-block;border:1px solid;border-radius:5px;background-color:ghostwhite;margin:1px;";
	private static final String other_style = "display: inline-block;border:1px solid;border-radius:5px;background-color:azure;margin:1px;";

	private void createHTMLscope(AppContext userContext, StringBuilder html, Space space, AnalyticsQuery query) {
		html.append("<h4 style='font-family:Helvetica Neue,Helvetica,Arial,sans-serif;'>Query Scope</h4><hr>");
		{
			UriBuilder builder = service.getPublicBaseUriBuilder().
					path("/analytics/{"+BBID_PARAM_NAME+"}/query");
			builder.queryParam(STYLE_PARAM, Style.HTML);
			builder.queryParam("access_token", userContext.getToken().getOid());
			URI uri = builder.build(space.getBBID(Style.ROBOT));
			html.append("<p>Query defined on domain: <a href=\""+StringEscapeUtils.escapeHtml4(uri.toString())+"\">"+StringEscapeUtils.escapeHtml4(space.getBBID(Style.HUMAN))+"</a>");
			URI scopeLink = service.getPublicBaseUriBuilder().path("/analytics/{reference}/scope").queryParam("style", Style.HTML).queryParam("access_token", getToken()).build(query.getBBID());
			html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(scopeLink.toASCIIString())+"\">/scope</a>]");
			html.append("</p>");
		}
		html.append("<div class=\"containerx\">");
		html.append("<ul class='nav nav-tabs'>");
		html.append("<li class=\"active\"><a data-toggle=\"tab\" href=\"#dimensions\">Dimensions</a></li>");
		html.append("<li><a data-toggle=\"tab\" href=\"#metrics\">Metrics</a></li>");
		html.append("<li><a data-toggle=\"tab\" href=\"#functions\">Functions</a></li>");
		html.append("<li><a data-toggle=\"tab\" href=\"#shortcuts\">Shortcuts</a></li>");
		html.append("</ul>");
		html.append("<div class=\"tab-content\">");
		html.append("<div id=\"dimensions\" class=\"tab-pane fade in active\">");
		//html.append("<div style='max-height:300px;overflow:scroll;'>");
		List<Axis> axisList = space.A(true);
		int pageSize = 25;
		int count = 0;
		int page = 1;
		StringBuilder dimensionContent = new StringBuilder();
		ArrayList<String> pageTitle = new ArrayList<>();
		HashMap<Space, List<Axis>> bySpace = new HashMap<>();
		ArrayList<Space> spaces = new ArrayList<>();
		HashMap<String, String> convertNames = new HashMap<>();
		for (Axis axis : axisList) {
			IDomain image = axis.getDefinitionSafe().getImageDomain();
			if (!image.isInstanceOf(IDomain.OBJECT)) {
				Space parent = axis.getParent();
				List<Axis> content = bySpace.get(parent);
				if (content==null) {
					content = new ArrayList<>();
					bySpace.put(parent, content);
					spaces.add(parent);
					/*
					try {
						DimensionIndex index = axis.getIndex();
						if (index instanceof DimensionIndexProxy) {
							DimensionIndexProxy proxy = (DimensionIndexProxy)index;
							Axis source = proxy.getSource();
							index = source.getIndex();
							convertNames.put(parent.prettyPrint(), index.getDimensionName());
						} else {
							convertNames.put(parent.prettyPrint(), space.getDomain().getName());
						}
					} catch (ComputingException | InterruptedException e) {
						convertNames.put(parent.prettyPrint(), parent.toString());
					}
					*/
					if (parent.equals(space)) {
						convertNames.put(parent.prettyPrint(), parent.toString());
					} else {
						String name = parent.toString();
						if (name.startsWith(space.toString())) {
							name = name.substring(space.toString().length()+1);
						}
						name = name.replace(".", " > ");
						convertNames.put(parent.prettyPrint(), name);
					}
				}
				content.add(axis);
			}
		}
		for (Space parent : spaces) {
			if (page>1) dimensionContent.append("</div>");
			dimensionContent.append("<div style='max-height:550px;overflow:scroll;' id='axis"+page+"' class='tab-pane fade "+(page==1?"in active'>":"'>"));
			page++;
			pageTitle.add(convertNames.get(parent.prettyPrint()));
			List<Axis> content = bySpace.get(parent);
			for (Axis axis : content) {// only print the visible scope
				try {
					IDomain image = axis.getDefinitionSafe().getImageDomain();
					if (!image.isInstanceOf(IDomain.OBJECT)) {
						//
						DimensionIndex index = axis.getIndex();
						dimensionContent.append("<span draggable='true' style='"+axis_style+"'");
						String name = index.getDimensionName().replaceAll("'", "&apos;").replaceAll("\"", "&quot;");
						dimensionContent.append(" ondragstart='drag(event,\"&apos;"+name+"&apos;\")'>");
						if (index.getErrorMessage()==null) {
							dimensionContent.append("&nbsp;"+index.getDimensionName()+"&nbsp;");
						} else {
							dimensionContent.append("&nbsp;<del>"+index.getDimensionName()+"</del>&nbsp;");
						}
						dimensionContent.append("</span>");
						// add description, type, error
						ExpressionAST expr = axis.getDefinitionSafe();
						dimensionContent.append(":"+getExpressionValueType(expr).toString());
						if (index.getErrorMessage()!=null) {
							dimensionContent.append("&nbsp;<b>Error:"+index.getErrorMessage()+"</b>");
						}
						if (axis.getDescription()!=null) {
							dimensionContent.append("&nbsp;"+axis.getDescription());
						}
						dimensionContent.append("<br>\n");
					}
				} catch (Exception e) {
					// ignore
				}
			}
		}
		/*
		pageTitle.add("1");
		for (Axis axis : axisList) {// only print the visible scope
			try {
				IDomain image = axis.getDefinitionSafe().getImageDomain();
				if (!image.isInstanceOf(IDomain.OBJECT)) {
					if (pages>1 && count % pageSize == 0) {
						if (page>1) dimensionContent.append("</div>");
						dimensionContent.append("<div id='axis"+page+"' class='tab-pane fade "+(page==1?"in active'>":"'>"));
						pageTitle.add(""+page);
						page++;
					}
					count++;
					//
					DimensionIndex index = axis.getIndex();
					dimensionContent.append("<span draggable='true' style='"+axis_style+"'");
					String name = index.getDimensionName().replaceAll("'", "&apos;").replaceAll("\"", "&quot;");
					dimensionContent.append(" ondragstart='drag(event,\"&apos;"+name+"&apos;\")'>");
					if (index.getErrorMessage()==null) {
						dimensionContent.append("&nbsp;"+index.getDimensionName()+"&nbsp;");
					} else {
						dimensionContent.append("&nbsp;<del>"+index.getDimensionName()+"</del>&nbsp;");
					}
					dimensionContent.append("</span>");
					// add description, type, error
					ExpressionAST expr = axis.getDefinitionSafe();
					dimensionContent.append(":"+getExpressionValueType(expr).toString());
					if (index.getErrorMessage()!=null) {
						dimensionContent.append("&nbsp;<b>Error:"+index.getErrorMessage()+"</b>");
					}
					if (axis.getDescription()!=null) {
						dimensionContent.append("&nbsp;"+axis.getDescription());
					}
					dimensionContent.append("<br>\n");
				}
			} catch (Exception e) {
				// ignore
			}
		}
		*/
		if (page>1) {
			if (page>1) dimensionContent.append("</div>");// closing last page
			dimensionContent.append("</div>");// closing pages
		}
		// add the header
		if (page>1) {
			html.append("<ul class='pagination'>");
			html.append("<li class='active'><a data-toggle='tab' href='#axis1'>"+pageTitle.get(0)+"</a></li>");
			for (int i=2;i<=pageTitle.size();i++) {
				html.append("<li><a data-toggle='tab' href='#axis"+i+"'>"+pageTitle.get(i-1)+"</a></li>");
			}
			html.append("</ul>");
			html.append("<div class='tab-content'>");
		}
		html.append(dimensionContent);
		html.append("</div>");
		html.append("<div id=\"metrics\" class=\"tab-pane fade\">");
		// add some space
		html.append("<ul class='pagination'></ul>");
		html.append("<div class='tab-content'>");
		for (Measure m : space.M()) {
			if (m.getMetric()!=null && !m.getMetric().isDynamic()) {
				html.append("<span draggable='true'  style='"+metric_style+"'");
				ExpressionAST expr = m.getDefinitionSafe();
				html.append(" title='"+getExpressionValueType(expr).toString()+": ");
				if (m.getDescription()!=null) {
					html.append(m.getDescription());
				}
				html.append("'");
				String name = m.getName().replaceAll("'", "&apos;").replaceAll("\"", "&quot;");
				html.append(" ondragstart='drag(event,\"&apos;"+name+"&apos;\")'");
				html.append(">&nbsp;"+m.getName()+"&nbsp;</span><br>");
			}
		}
		html.append("</div>");
		html.append("</div>");
		// -- functions
		html.append("<div id=\"functions\" class=\"tab-pane fade\">");// style='max-height:600px;overflow:scroll;'>");
		count = 0;
		int actualPages = 0;
		page = 1;
		pageTitle = new ArrayList<>();
		StringBuilder functionContent = new StringBuilder();
		try {
			SpaceScope scope = new SpaceScope(space);
			List<OperatorDefinition> ops = new ArrayList<>(scope.looseLookup(""));
			Collections.sort(ops, new Comparator<OperatorDefinition>() {
				@Override
				public int compare(OperatorDefinition o1, OperatorDefinition o2) {
					if (o1.getPosition()!=o2.getPosition()) {
						return o1.getPosition()-o2.getPosition();
					} else {
						return o1.getName().compareTo(o2.getName());
					}
				}
			});
			OperatorDefinition previous = null;
			HashMap<String, List<OperatorDefinition>> categories = new HashMap<>();
			ArrayList<String> keys = new ArrayList<>();
			for (OperatorDefinition opDef : ops) {
				String cat = opDef.getCategoryTypeName();
				List<OperatorDefinition> content = categories.get(cat);
				if (content==null) {
					content = new ArrayList<>();
					categories.put(cat, content);
					keys.add(cat);
				}
				content.add(opDef);
			}
			Collections.sort(keys);
			for (String cat : keys) {
				List<OperatorDefinition> content = categories.get(cat);
				if (page>1) functionContent.append("</div>");
				functionContent.append("<div style='max-height:550px;overflow:scroll;' id='fun"+page+"' class='tab-pane fade "+(page==1?"in active'>":"'>"));
				page++;
				pageTitle.add(cat);
				actualPages++;
				for (OperatorDefinition opDef : content) {
					//if (opDef.getPosition()!=OperatorDefinition.INFIX_POSITION) {
					ListContentAssistEntry listContentAssistEntry = opDef.getSimplifiedListContentAssistEntry();
					if (listContentAssistEntry != null) {
						if (listContentAssistEntry.getContentAssistEntries() != null) {
							for (ContentAssistEntry contentAssistEntry : listContentAssistEntry.getContentAssistEntries()) {
								count++;
								previous = opDef;
								String display = null;
								if (opDef.getPosition()==OperatorDefinition.INFIX_POSITION){
									display = opDef.getSymbol() ;
								}else{
									display =opDef.getSymbol() + "(" + contentAssistEntry.getLabel() + ")";
								}
								functionContent.append("<span draggable='true'  style='"+metric_style+"'");
								functionContent.append(" ondragstart='drag(event,\""+display+"\")'");
								functionContent.append(">&nbsp;"+display+"&nbsp;</span>: "+contentAssistEntry.getDescription()+"<br>");
							}
						}
					}
				}
			}
			/*
			for (OperatorDefinition opDef : ops) {
				//if (opDef.getPosition()!=OperatorDefinition.INFIX_POSITION) {
				ListContentAssistEntry listContentAssistEntry = opDef.getSimplifiedListContentAssistEntry();
				if (listContentAssistEntry != null) {
					if (listContentAssistEntry.getContentAssistEntries() != null) {
						if (count >= pageSize) {
							count = 0; // reset
						}
						if (count == 0) {
							if (page>1) functionContent.append("</div>");
							functionContent.append("<div id='fun"+page+"' class='tab-pane fade "+(page==1?"in active'>":"'>"));
							page++;
							if (previous!=null) {
								pageTitle.add(previous.getSymbol());
							}
							pageTitle.add(opDef.getSymbol());
							actualPages++;
						}
						for (ContentAssistEntry contentAssistEntry : listContentAssistEntry.getContentAssistEntries()) {
							count++;
							previous = opDef;
							String display = null;
							if (opDef.getPosition()==OperatorDefinition.INFIX_POSITION){
								display = opDef.getSymbol() ;
							}else{
								display =opDef.getSymbol() + "(" + contentAssistEntry.getLabel() + ")";
							}
							functionContent.append("<span draggable='true'  style='"+metric_style+"'");
							functionContent.append(" ondragstart='drag(event,\""+display+"\")'");
							functionContent.append(">&nbsp;"+display+"&nbsp;</span>: "+contentAssistEntry.getDescription()+"<br>");
						}
					}
				}
			}
			*/
		} catch (ScopeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (true) {
			if (page>1) functionContent.append("</div>");// closing last page
			functionContent.append("</div>");// closing pages
		}
		// add the header
		if (true) {
			html.append("<ul class='pagination'>");
			html.append("<li class='active'><a data-toggle='tab' href='#fun1'>"+pageTitle.get(0)+"</a></li>");
			for (int i=2;i<=actualPages;i++) {
				html.append("<li><a data-toggle='tab' href='#fun"+i+"'>"+pageTitle.get(i-1)+"</a></li>");
			}
			html.append("</ul>");
			html.append("<div class='tab-content'>");
		}
		html.append(functionContent);
		html.append("</div>");
		// -- Shortcuts
		html.append("<div id=\"shortcuts\" class=\"tab-pane fade in\">");
		// add some space
		html.append("<ul class='pagination'></ul>");
		html.append("<div class='tab-content'>");
		//
		appendItem(html, other_style, "__PERIOD", "the default period dimension");
		appendItem(html, other_style, "daily(__PERIOD)", "the default period in daily increment");
		appendItem(html, other_style, "weekly(__PERIOD)", "the default period in weekly increment");
		appendItem(html, other_style, "monthly(__PERIOD)", "the default period in monthly increment");
		appendItem(html, other_style, "yearly(__PERIOD)", "the default period in yearly increment");
		appendItem(html, other_style, "__LAST_7_DAYS", "set the timeframe to the last 7 days");
		appendItem(html, other_style, "__CURRENT_MONTH", "set the timeframe to the current month");
		appendItem(html, other_style, "__PREVIOUS_MONTH", "set the timeframe to the previous month");
		appendItem(html, other_style, "__CURRENT_YEAR", "set the timeframe to the current year");
		appendItem(html, other_style, "__PREVIOUS_YEAR", "set the timeframe to the previous year");
		appendItem(html, other_style, "__COMPARE_TO_PREVIOUS_PERIOD", "compare the results with the previous period");
		appendItem(html, other_style, "__COMPARE_TO_PREVIOUS_MONTH", "compare the results with the previous month");
		appendItem(html, other_style, "__COMPARE_TO_PREVIOUS_YEAR", "compare the results with the previous year");
		html.append("</div>");
		html.append("</div>");
		//---- container
		html.append("</div>");
	}
	
	private String displayRange(List<String> pages, int index) {
		int pageIndex = index*2;
		if (pageIndex+1<pages.size()) {
			return pages.get(pageIndex) + " - " + pages.get(pageIndex+1);
		} else {
			return pages.get(pageIndex);
		}
	}
	
	private void appendItem(StringBuilder html, String style, String display, String comment) {
		html.append("<span draggable='true'  style='"+style+"'");
		html.append(" ondragstart='drag(event,\""+display+"\")'");
		html.append(">&nbsp;"+display+"&nbsp;</span>");
		if (comment!=null && !comment.equals("")) html.append(":"+comment);
		html.append("<br>\n");
	}

	/**
	 * @param userContext
	 * @param space
	 * @param target
	 * @param suggestions
	 * @param values
	 * @param types
	 * @param expression
	 * @return
	 */
	public Response createHTMLPageScope(AppContext ctx, Space space, Space target, ExpressionSuggestion suggestions, String BBID, String value, ObjectType[] types, ValueType[] values) {
		String title = getPageTitle(space);
		String breadcrumbs = getBreadCrumbs(space);
		StringBuilder html = createHTMLHeader(title +" - OB Query Builder");
		createHTMLtitle(ctx, html, breadcrumbs, BBID, null, target, getParentLink(space), null, "scopeAnalysis");
		html.append("<form>");
		String value_value = getFieldValue(value);
		html.append("<p>Expression:<input type='text' id='value-param' name='value' size=100 value='"+value_value+"' placeholder='type expression to validate it or to filter the suggestion list'>&nbsp;offset=<input type='text' id='offset-param' name='offset' value='"+value_value.length()+"'</p>");
		//
		if (suggestions.getValueType()!=null) {
			if (suggestions.getValueType().equals(ValueType.ERROR)) {
				html.append("<p><span class=\"label label-danger\">Invalid Expression</span> the scope provides suggestions based on the partial evaluation and offset position</p>");
			} else {
				html.append("<p><span class=\"label label-success\">Valid Expression</span> Expression Type: "+suggestions.getValueType().toString()+"</p>");
			}
		}
		if (value!=null && value.length()>0 && suggestions.getValidateMessage()!=null && suggestions.getValidateMessage().length()>0) {
			createHTMLproblems(html, Collections.singletonList(new Problem(Severity.WARNING, value, suggestions.getValidateMessage())));
		}
		//
		html.append("<div class=\"clearfix\">");
		html.append("<div style='float:left;padding:5px'>");
		html.append("<input type=\"hidden\" name=\"style\" value=\"HTML\">"
				+ "<input type=\"hidden\" name=\"access_token\" value=\""+space.getUniverse().getContext().getToken().getOid()+"\">"
				+ "<input type=\"submit\" value=\"Refresh\">");
		if (target!=null) {
			html.append("<input type=\"hidden\" name=\"target\" value=\""+target.getBBID(Style.ROBOT)+"\">");
		}
		html.append("</div>");
		html.append("<p style='padding:5px'>Filter by expression type:");
		html.append("&nbsp;&nbsp;<input type='checkbox' name='types' value='"+ObjectType.DIMENSION+"'"+(checkObjectType(types,ObjectType.DIMENSION))+">&nbsp;"+ObjectType.DIMENSION);
		html.append("&nbsp;&nbsp;<input type='checkbox' name='types' value='"+ObjectType.COLUMN+"'"+(checkObjectType(types,ObjectType.COLUMN))+">&nbsp;"+ObjectType.COLUMN);
		html.append("&nbsp;&nbsp;<input type='checkbox' name='types' value='"+ObjectType.RELATION+"'"+(checkObjectType(types,ObjectType.RELATION))+">&nbsp;"+ObjectType.RELATION);
		html.append("&nbsp;&nbsp;<input type='checkbox' name='types' value='"+ObjectType.METRIC+"'"+(checkObjectType(types,ObjectType.METRIC))+">&nbsp;"+ObjectType.METRIC);
		html.append("&nbsp;&nbsp;<input type='checkbox' name='types' value='"+ObjectType.EXPRESSION+"'"+(checkObjectType(types,ObjectType.EXPRESSION))+">&nbsp;"+ObjectType.EXPRESSION);
		html.append("&nbsp;&nbsp;<input type='checkbox' name='types' value='"+ObjectType.FOREIGNKEY+"'"+(checkObjectType(types,ObjectType.FOREIGNKEY))+">&nbsp;"+ObjectType.FOREIGNKEY);
		html.append("&nbsp;&nbsp;<input type='checkbox' name='types' value='"+ObjectType.FUNCTION+"'"+(checkObjectType(types,ObjectType.FUNCTION))+">&nbsp;"+ObjectType.FUNCTION);
		html.append("<br>");
		html.append("Filter by expression value:");
		html.append("&nbsp;&nbsp;<input type='checkbox' name='values' value='"+ValueType.DATE+"'"+(checkValueType(values,ValueType.DATE))+">&nbsp;"+ValueType.DATE);
		html.append("&nbsp;&nbsp;<input type='checkbox' name='values' value='"+ValueType.STRING+"'"+(checkValueType(values,ValueType.STRING))+">&nbsp;"+ValueType.STRING);
		html.append("&nbsp;&nbsp;<input type='checkbox' name='values' value='"+ValueType.CONDITION+"'"+(checkValueType(values,ValueType.CONDITION))+">&nbsp;"+ValueType.CONDITION);
		html.append("&nbsp;&nbsp;<input type='checkbox' name='values' value='"+ValueType.NUMERIC+"'"+(checkValueType(values,ValueType.NUMERIC))+">&nbsp;"+ValueType.NUMERIC);
		html.append("&nbsp;&nbsp;<input type='checkbox' name='values' value='"+ValueType.AGGREGATE+"'"+(checkValueType(values,ValueType.AGGREGATE))+">&nbsp;"+ValueType.AGGREGATE);
		html.append("</p>");
		html.append("</div>");
		html.append("</form>");
		html.append("<p><i> This is the list of all available expressions and function in this scope. Relation expression can be composed in order to navigate the data model.</i></p>");
		html.append("<table>");
		for (ExpressionSuggestionItem item : suggestions.getSuggestions()) {
			html.append("<tr><td>");
			html.append(item.getObjectType()+"</td><td>");
			html.append(item.getValueType()+"</td><td>");
			String style = other_style;
			if (item.getObjectType()==ObjectType.DIMENSION) style = axis_style;
			if (item.getObjectType()==ObjectType.METRIC) style = metric_style;
			if (item.getObjectType()==ObjectType.FUNCTION) style = func_style;
			html.append("<span style='"+style+"'>&nbsp;"+item.getDisplay()+"&nbsp;</span>");
			if (item.getSuggestion()!=null) {
				UriBuilder builder = service.getPublicBaseUriBuilder().path("/analytics/{reference}/scope").queryParam("value", value+item.getSuggestion()).queryParam("style", Style.HTML).queryParam("access_token", getToken());
				if (target!=null) {
					builder.queryParam("target", target.getBBID(Style.ROBOT));
				}
				URI link = builder.build(BBID);
				html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(link.toASCIIString())+"\">+</a>]");
			}
			if (item.getExpression()!=null && item.getExpression() instanceof AxisExpression) {
				AxisExpression ref = (AxisExpression)item.getExpression();
				Axis axis = ref.getAxis();
				if (axis.getDimensionType()==Type.CATEGORICAL) {
					URI link = service.getPublicBaseUriBuilder().path("/analytics/{reference}/facets/{facetId}").queryParam("style", Style.HTML).queryParam("access_token", getToken()).build(BBID, item.getSuggestion());
					html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(link.toASCIIString())+"\">Indexed</a>]");
				} else if (axis.getDimensionType()==Type.CONTINUOUS) {
					URI link = service.getPublicBaseUriBuilder().path("/analytics/{reference}/facets/{facetId}").queryParam("style", Style.HTML).queryParam("access_token", getToken()).build(BBID, item.getSuggestion());
					html.append("&nbsp;[<a href=\""+StringEscapeUtils.escapeHtml4(link.toASCIIString())+"\">Period</a>]");
				}
			}
			if (item.getDescription()!=null && item.getDescription().length()>0) {
				html.append("<br><i>"+item.getDescription()+"</i>");
			}
			html.append("</td></tr>");
		}
		html.append("</table>");
		createHTMLAPIpanel(html, "scopeAnalysis");
		createFooter(html);
		// caret
		html.append("<script>\n" +
				"$(\"#value-param\").bind(\"keydown keypress mousemove\", function() {\n" +
				"  $(\"#offset-param\").get(0).value = $(\"#value-param\").get(0).selectionStart;\n" +
				"});"
				+
				"</script>");
		html.append("</body></html>");
		return Response.ok(html.toString(),"text/html").build();
	}

	/**
	 * @param values
	 * @param date
	 * @return
	 */
	private String checkValueType(ValueType[] values, ValueType type) {
		for (ValueType check : values) {
			if (check.equals(type)) {
				return " checked=true";
			}
		}
		// else
		return "";
	}

	/**
	 * @param types
	 * @param dimension
	 * @return
	 */
	private String checkObjectType(ObjectType[] types, ObjectType type) {
		for (ObjectType check : types) {
			if (check.equals(type)) {
				return " checked=true";
			}
		}
		// else
		return "";
	}

	private void createHTMLAPIpanel(StringBuilder html, String method) {
		html.append("<div style='clear:both;padding-top:15px;'>");
		//html.append("<h4 style='font-family:Helvetica Neue,Helvetica,Arial,sans-serif;'>Query Reference</h4><hr>");
		// compute the raw URI
		UriBuilder builder = service.getPublicBaseUriBuilder().path(service.getUriInfo().getPath());
		MultivaluedMap<String, String> parameters = service.getUriInfo().getQueryParameters();
		//parameters.remove(ACCESS_TOKEN_PARAM);
		parameters.remove(STYLE_PARAM);
		parameters.remove(ENVELOPE_PARAM);
		for (Entry<String, List<String>> parameter : parameters.entrySet()) {
			for (String value : parameter.getValue()) {
				if (value!=null && !value.equals("")) {
					builder.queryParam(parameter.getKey(), value);
				}
			}
		}
		html.append("</div>");
	}

	private void createFooter(StringBuilder html) {
		html.append("<div class=\"footer\"><p>Powered by <a href=\"http://openbouquet.io/\">Open Bouquet</a> <i style='color:white;'>the Analytics Rest API</i></p></div>\n");
		html.append("\n" +
				"<script>\n" +
				"$(document).ready(function(){\n" +
				"    $('[data-toggle=\"tooltip\"]').tooltip();   \n" +
				"});\n" +
				"</script>");
	}

	private void createHTMLswaggerLink(StringBuilder html, String method) {
		String baseUrl = "";
		try {
			baseUrl = "?url="+URLEncoder.encode(service.getPublicBaseUriBuilder().path("swagger.json").build().toString(),"UTF-8")+"";
		} catch (UnsupportedEncodingException | IllegalArgumentException | UriBuilderException e) {
			// default
		}
		html.append("<p>The Analytics API provides more parameters... check <a target='swagger' href='http://swagger.squidsolutions.com/"+baseUrl+"#!/analytics/"+method+"'>swagger UI</a> for details</p>");
	}

	/**
	 * displays query problems
	 * @param html
	 * @param query
	 */
	private void createHTMLproblems(StringBuilder html, List<Problem> problems) {
		if (problems!=null && problems.size()>0) {
			html.append("<div class='problems' style='border:1px solid red; background-color:lightpink;'>There are some problems with the query:");
			for (Problem problem : problems) {
				html.append("<li>"+problem.getSeverity().toString()+": "+problem.getSubject()+": "+problem.getMessage()+"</li>");
			}
			html.append("</div>");
		}
	}

	private String getPageTitle(Space space) {
		if (space.hasBookmark()) {
			return  space.getBookmark().getName() ;
		} else {
			return  space.getDomain().getName();
		}
	}
	private String getBreadCrumbs(Space space) {
		if (space.hasBookmark()) {
			String path = getBookmarkNavigationPath(space.getBookmark());
			return path+" > "+space.getBookmark().getName();
		} else {
			return space.getUniverse().getProject().getName()+" > "+space.getDomain().getName();
		}
	}

	private String getDate(List<String> dates, int pos) {
		if (dates!=null && !dates.isEmpty() && pos<dates.size()) {
			return formatDateForWeb(getFieldValue(dates.get(pos)));
		} else {
			return "";
		}
	}

	private ValueType getExpressionValueType(ExpressionAST expr) {
		IDomain image = expr.getImageDomain();
		return ExpressionSuggestionHandler.computeValueTypeFromImage(image);
	}


	private SimpleDateFormat htmlDateFormat = new SimpleDateFormat("yyyy-MM-dd");

	private String formatDateForWeb(String jsonFormat) {
		if (jsonFormat.startsWith("__")) return jsonFormat;// it's a shortcut
		try {
			Date date = ServiceUtils.getInstance().toDate(jsonFormat);
			return htmlDateFormat.format(date);
		} catch (ScopeException e) {
			return jsonFormat;
		}
	}

	/**
	 * @param result
	 * @return
	 */
	private String writeVegalightSpecs(VegaliteSpecs specs) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			mapper.setSerializationInclusion(Include.NON_NULL);
			return mapper.writeValueAsString(specs);
		} catch (JsonProcessingException e) {
			throw new APIException("failed to write vegalite specs to JSON", e, true);
		}
	}

	private String getFieldValue(Object var) {
		if (var==null) return ""; else return var.toString().replaceAll("\"", "&quot;").replaceAll("'", "&#x27;");
	}

	private String getFieldValue(Object var, Object defaultValue) {
		if (var==null) return defaultValue.toString(); else return var.toString().replaceAll("\"", "&quot;").replaceAll("'", "&#x27;");
	}

	protected URI getParentLink(Space space) {
		if (space==null) return null;
		if (space.hasBookmark()) {
			String path = getBookmarkNavigationPath(space.getBookmark());
			return service.getPublicBaseUriBuilder().path("/analytics").queryParam(PARENT_PARAM, path).queryParam(STYLE_PARAM, "HTML").queryParam("access_token", getToken()).build();
		} else {
			return service.getPublicBaseUriBuilder().path("/analytics").queryParam(PARENT_PARAM, "/PROJECTS/"+space.getUniverse().getProject().getName()).queryParam(STYLE_PARAM, "HTML").queryParam("access_token", getToken()).build();
		}
	}

	public String getBookmarkNavigationPath(Bookmark bookmark) {
		String path = bookmark.getPath();
		if (path.startsWith(Bookmark.SEPARATOR + Bookmark.Folder.USER)) {
			String[] elements = path.split(Bookmark.SEPARATOR);
			if (elements.length>=3) {
				String oid = elements[2];
				path = path.substring((Bookmark.SEPARATOR + Bookmark.Folder.USER + Bookmark.SEPARATOR + oid).length());
				if (oid.equals(service.getUserContext().getUser().getOid())) {
					path = AnalyticsServiceBaseImpl.MYBOOKMARKS_FOLDER.getSelfRef()+path;
				} else {
					path = AnalyticsServiceBaseImpl.SHAREDWITHME_FOLDER.getSelfRef()+path;
				}
			} else {
				path = AnalyticsServiceBaseImpl.MYBOOKMARKS_FOLDER.getSelfRef()+path.substring((Bookmark.SEPARATOR + Bookmark.Folder.USER).length());
			}
		} else if (path.startsWith(Bookmark.SEPARATOR + Bookmark.Folder.SHARED)) {
			path = AnalyticsServiceBaseImpl.SHARED_FOLDER.getSelfRef()+path.substring((Bookmark.SEPARATOR + Bookmark.Folder.SHARED).length());
		}
		if (path.endsWith("/")) path = path.substring(0, path.length()-1);
		return path;
	}

}
