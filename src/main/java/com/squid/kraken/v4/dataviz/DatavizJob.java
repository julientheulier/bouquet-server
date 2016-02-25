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
package com.squid.kraken.v4.dataviz;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ExpressionMaker;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.projectanalysisjob.AnalysisJobServiceBaseImpl;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.SegmentManager;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.scope.SpaceExpression;
import com.squid.kraken.v4.core.analysis.scope.UniverseScope;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.scope.DomainExpressionScope;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Expression;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.FacetExpression;
import com.squid.kraken.v4.model.FacetMemberString;
import com.squid.kraken.v4.model.FacetSelection;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectAnalysisJob;
import com.squid.kraken.v4.model.ProjectAnalysisJobPK;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AppContext;

/**
 * T886 prototype
 * - the object accept a JSON dataviz object as an input
 * - it will extract all reference to Bouquet metadata to build an AnalysisJob
 * - then it will modify the JSON definition to bind the AnalysisJob dataset
 * 
 * @author sergefantino
 *
 */
public class DatavizJob {

	private AppContext userContext;

	public DatavizJob(AppContext ctx) {
		this.userContext = ctx;
	}
	
	public Object proceed(String projectId, String input) throws ScopeException, JsonParseException, JsonMappingException, IOException 
	{
		ProjectPK projectPK = new ProjectPK(userContext.getCustomerId(),
				projectId);
		Project project = ProjectManager.INSTANCE.getProject(userContext, projectPK);
		AccessRightsUtils.getInstance().checkRole(userContext, project, AccessRight.Role.READ);
		Universe universe = new Universe(userContext, project);
		//
		ObjectMapper mapper = new ObjectMapper();
		Object value = mapper.readValue(input.toString(), new TypeReference<HashMap<String,Object>>() {});
		HashMap<String, Object> json = (HashMap<String, Object>) value;
		HashMap<String, Object> data = (HashMap<String, Object>) json.get("data");
		// read the domain reference
		String domainExpr = (String) data.get("domainExpr");
		if (domainExpr==null) {
			throw new ScopeException("incomplete specification, you must specify the data domain expression");
		}
		// -- using the universe scope for now; will change when merge with T821 to also support query
		UniverseScope scope = new UniverseScope(universe);
		ExpressionAST expr = scope.parseExpression(domainExpr);
		if (!(expr instanceof SpaceExpression)) {
			throw new ScopeException("invalid specification, the domain expression must resolve to a Space");
		}
		Space ref = ((SpaceExpression)expr).getSpace();
		Domain domain = ref.getDomain();
		AccessRightsUtils.getInstance().checkRole(userContext, domain, AccessRight.Role.READ);
		// the rest of the ACL is delegated to the AnalysisJob
		Space root = universe.S(domain);
		// check the callback url
		String url = (String) data.get("callback");
		if (url==null) {
			throw new ScopeException("incomplete specification, you must specify the API url");
		}
		if (!url.endsWith("/")) url+="/";
		if (!url.endsWith("/rs/")) {
			throw new ScopeException("invalid specification, this is not a valid API url, must end with /rs");
		}
		URL URL = new URL(url);
		//
		ProjectAnalysisJobPK pk = new ProjectAnalysisJobPK(projectPK, null);
		ProjectAnalysisJob analysis = new ProjectAnalysisJob(pk);
		analysis.setDomains(Collections.singletonList(domain.getId()));
		analysis.setMetricList(new ArrayList<Metric>());
		//
		HashMap<String, Object> encoding = (HashMap<String, Object>) json.get("encoding");
		//
		// handle channels
		// mark properties channels
		boolean hasX = handleChannel(root, analysis, encoding, "x");
		boolean hasY = handleChannel(root, analysis, encoding, "y");
		if (!(hasX || hasY)) {
			throw new ScopeException("incomplete specification, you must specify at least x or y");
		}
		handleChannel(root, analysis, encoding, "color");
		handleChannel(root, analysis, encoding, "size");
		handleChannel(root, analysis, encoding, "shape");
		handleChannel(root, analysis, encoding, "text");
		// detail channel
		handleChannel(root, analysis, encoding, "detail");
		// order channels
		handleChannel(root, analysis, encoding, "order");
		handleChannel(root, analysis, encoding, "path");
		// facet channels
		//boolean hasRow = 
				handleChannel(root, analysis, encoding, "row");
		//boolean hasColumn = 
				handleChannel(root, analysis, encoding, "column");
		//
		// select channel : this is used to change the dataset level of details without visual impact
		handleChannel(root, analysis, encoding, "select");
		// handle filters
		HashMap<String, Object> transform = (HashMap<String, Object>)json.get("transform");
		if (transform!=null && transform.containsKey("filterExpr")) {
			ExpressionAST filter = getExpression(root, transform, "filterExpr");
			if (!filter.getImageDomain().isInstanceOf(IDomain.CONDITIONAL)) {
				throw new ScopeException("invalid transform:filterExpr, must be a condition");
			}
			// my god, we have to use a open-filter...
			FacetSelection selection = new FacetSelection();
			Facet segment = SegmentManager.newSegmentFacet(domain);
            FacetMemberString openFilter = SegmentManager.newOpenFilter(filter, (String)transform.get("filterExpr"));
            segment.getSelectedItems().add(openFilter);
			selection.getFacets().add(segment);
			//selection.
			analysis.setSelection(selection);
		}
		// register the analysis
		analysis.setAutoRun(true);// run it so it will be in cache
		AnalysisJobServiceBaseImpl.getInstance().store(userContext, analysis,0, 1000, 0, false);
		//
		// append the job ID to the url
		String xxx = url+"projects/"+projectId+"/analysisjobs/"+analysis.getId().getAnalysisJobId()+"/results?format=csv&access_token="+userContext.getToken().getId().getTokenId();
		data.put("url", xxx);
		data.put("formatType", "csv");
		// json
		return json;
	}
	
	/**
	 * handle the channel if defined in the properties
	 * @param root
	 * @param encoding
	 * @param channel
	 * @return
	 * @throws ScopeException 
	 */
	private boolean handleChannel(Space root, ProjectAnalysisJob analysis, HashMap<String, Object> encoding, String channel) throws ScopeException {
		HashMap<String, Object> properties = (HashMap<String, Object>) encoding.get(channel);
		if (properties!=null) {
			ExpressionAST expr = getExpression(root, properties, "expr");
			if (expr!=null) {
				// the caller may directly request a VEGA computation
				genFieldDefinition(root, analysis, properties, expr);
			}
			return true;
		} else {
			return false;
		}
	}
	
	private ExpressionAST getExpression(Space root, HashMap<String, Object> properties, String property) throws ScopeException {
		if (properties!=null && properties.containsKey(property)) {
			String expr = properties.get(property).toString();
			DomainExpressionScope scope = new DomainExpressionScope(root.getUniverse(), root.getDomain());
			return scope.parseExpression(expr);
		} else {
			return null;
		}
	}
	
	private void genFieldDefinition(Space root, ProjectAnalysisJob analysis, HashMap<String, Object> properties, ExpressionAST expr) throws ScopeException {
		IDomain image = expr.getImageDomain();
		boolean aggr = image.isInstanceOf(IDomain.AGGREGATE);
		String field = properties.containsKey("field")?properties.get("field").toString():null;
		if (aggr) {
			properties.put("type", "quantitative");
			Measure m = root.getUniverse().asMeasure(expr);
			if (m==null) throw new ScopeException("cannot use expresion='"+expr.prettyPrint()+"'");
			Metric metric = new Metric();
			metric.setExpression(new Expression(m.prettyPrint()));
			analysis.getMetricList().add(metric);
			String name = field!=null?field:formatName(m.getName());
			metric.setName(name);
			properties.put("field", name);
		} else {
			if (image.isInstanceOf(IDomain.TEMPORAL)) {
				properties.put("type", "temporal");
			} else if (image.isInstanceOf(IDomain.NUMERIC)) {
				properties.put("type", "ordinal");
			} else {
				properties.put("type", "nominal");
			}
			Axis axis = root.getUniverse().asAxis(expr);
			if (axis==null) throw new ScopeException("cannot use expresion='"+expr.prettyPrint()+"'");
			ExpressionAST facet = ExpressionMaker.COMPOSE(new SpaceExpression(root), expr);
			String name = field!=null?field:formatName(axis.getDimension()!=null?axis.getName():axis.getDefinitionSafe().prettyPrint());
			analysis.getFacets().add(new FacetExpression(facet.prettyPrint(), name));
			properties.put("field", name);
			if (axis.getDimension()==null) {
				// better add an axis title...
				HashMap<String, Object> axisProp = new HashMap<>();
				axisProp.put("title", expr.prettyPrint());
				properties.put("axis", axisProp);
			}
		}
	}
	
	private String formatName(String prettyPrint) {
		return prettyPrint.replaceAll("[(),.]", " ").trim().replaceAll("[^ a-zA-Z_0-9]", "").replace(' ','_');
	}
	
}
