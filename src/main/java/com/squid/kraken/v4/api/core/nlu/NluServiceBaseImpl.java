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
package com.squid.kraken.v4.api.core.nlu;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.core.UriInfo;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.squid.core.domain.IDomain;
import com.squid.core.expression.PrettyPrintOptions;
import com.squid.core.expression.PrettyPrintOptions.ReferenceStyle;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.analytics.AnalyticsServiceCore;
import com.squid.kraken.v4.api.core.nlu.rasa.CommonExample;
import com.squid.kraken.v4.api.core.nlu.rasa.Entity;
import com.squid.kraken.v4.api.core.nlu.rasa.EntityExtraction;
import com.squid.kraken.v4.api.core.nlu.rasa.RasaNluData;
import com.squid.kraken.v4.api.core.nlu.rasa.RasaQuery;
import com.squid.kraken.v4.api.core.nlu.rasa.TrainingSet;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.AnalyticsQueryImpl;
import com.squid.kraken.v4.model.AnalyticsReply;
import com.squid.kraken.v4.model.AnalyticsResult;
import com.squid.kraken.v4.model.DataHeader.Column;
import com.squid.kraken.v4.model.DataHeader.Role;
import com.squid.kraken.v4.model.DataLayout;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.model.NavigationQuery.Style;
import com.squid.kraken.v4.persistence.AppContext;

/**
 * @author sergefantino
 *
 */
public class NluServiceBaseImpl extends AnalyticsServiceCore {
	
	private UriInfo uriInfo = null;
	private AppContext userContext = null;

	/**
	 * @param uriInfo 
	 * @param userContext
	 */
	public NluServiceBaseImpl(UriInfo uriInfo, AppContext userContext) {
		this.uriInfo = uriInfo;
		this.userContext = userContext;
	}

	/**
	 * @param userContext
	 * @param bBID
	 * @return
	 */
	public TrainingSet generateTrainingSet(String BBID) {
		// looking for the subject
		Space space = getSpace(userContext, BBID);
		String contextName = getContextName(space);
		//
		TrainingSet set = new TrainingSet();
		RasaNluData rasa_nlu_data = new RasaNluData();
		set.setRasa_nlu_data(rasa_nlu_data);
		ArrayList<CommonExample> common_examples =new ArrayList<>();
		rasa_nlu_data.setCommon_examples(common_examples);
		// what are the intents?
		// help: provide some hints regarding what you can expect
		common_examples.add(createExample("help", "Hello!"));
		common_examples.add(createExample("help", "What can you do for me?"));
		common_examples.add(createExample("help", "Help?"));
		common_examples.add(createExample("help", "What's your name?"));
		// bookmark_overview: display all kpis for the current period
		common_examples.add(createExample("bookmark_overview", "What are the numbers?"));
		common_examples.add(createExample("bookmark_overview", "What are the total numbers?"));
		common_examples.add(createExample("bookmark_overview", "What is the situation?"));
		common_examples.add(createExample("bookmark_overview", "What are the key metrics?"));
		common_examples.add(createExample("bookmark_overview", "What are the kpis?"));
		common_examples.add(createExample("bookmark_overview", "What are the key metrics look like?"));
		common_examples.add(createExample("bookmark_overview", "What are the kpis look like?"));
		common_examples.add(createExample("bookmark_overview", "How is my business going?"));
		common_examples.add(createExample("bookmark_overview", "How is my business doing?"));
		common_examples.add(createExample("bookmark_overview", "What are the "+contextName+"?"));
		common_examples.add(createExample("bookmark_overview", "What are the "+contextName+" kpis?"));
		//
		// bookmark_overall_metric: return a single metric for the current period, compare to previous period if set
		//
		for (Measure m : space.M()) {
			if (m.getMetric()!=null && !m.getMetric().isDynamic()) {
				String name = m.getName();
				String ID = m.prettyPrint(new PrettyPrintOptions(ReferenceStyle.IDENTIFIER, null));
				CommonExample example = createExample("bookmark_overall_metric", "What are the %metric ?");
				if (addEntity(example,"metric",name,ID)) common_examples.add(example);
				CommonExample example1 = createExample("bookmark_overall_metric", "What are the overall %metric ?");
				if (addEntity(example1,"metric",name,ID)) common_examples.add(example1);
				CommonExample example2 = createExample("bookmark_overall_metric", "What are the total %metric ?");
				if (addEntity(example2,"metric",name,ID)) common_examples.add(example2);
			}
		}
		//
		// bookmark_top_dimension: return the top 5 dimension, for some metric, for the current period
		//
		for (Axis axis : space.A(true)) {
			IDomain image = axis.getDefinitionSafe().getImageDomain();
			if (!image.isInstanceOf(IDomain.OBJECT)) {
				if (axis.getDimension().getType().equals(Type.CATEGORICAL)) {
					String dim_name = getAxisSHortName(axis);
					String dim_ID = axis.prettyPrint(new PrettyPrintOptions(ReferenceStyle.IDENTIFIER, null));
					{
						CommonExample example = createExample("bookmark_top_dimension", "What are the top %dimension ?");
						if (addEntity(example,"dimension",dim_name,dim_ID)) common_examples.add(example);
					}
					for (Measure m : space.M()) {
						if (m.getMetric()!=null && !m.getMetric().isDynamic()) {
							String name = m.getName();
							String ID = m.prettyPrint(new PrettyPrintOptions(ReferenceStyle.IDENTIFIER, null));
							CommonExample example = createExample("bookmark_top_dimension", "What are the top %dimension by %metric ?");
							if (addEntity(example,"dimension",dim_name,dim_ID) && addEntity(example,"metric",name,ID)) common_examples.add(example);
						}
					}
				}
			}
		}
		// bookmark_overview_for_dim_X: display all or specifc kpis for a given dimension value
		//
		return set;
	}
	
	private String getAxisSHortName(Axis axis) {
		return axis.getDimension()!=null?axis.getDimension().getName():axis.getName();
	}

	/**
	 * @param example
	 * @param string
	 * @param string2
	 * @param name
	 * @param iD
	 * @return 
	 */
	private boolean addEntity(CommonExample example, String entity, String text, String value) {
		String message = example.getText();
		int start = message.indexOf("%"+entity);
		if (start>=0) {
			message = message.replace("%"+entity, text);
			example.setText(message);
			Entity instance = new Entity();
			instance.setEntity(entity);
			instance.setValue(value);
			instance.setStart(start);
			instance.setEnd(start+text.length());
			example.getEntities().add(instance);
			return true;
		}
		// else
		return false;
	}

	/**
	 * @param space
	 * @return
	 */
	private String getContextName(Space space) {
		String contextName = space.getDomain().getName();
		if (space.getBookmark()!=null) {
			contextName = space.getBookmark().getName();
		}
		return contextName;
	}

	/**
	 * @param string
	 * @param string2
	 * @return
	 */
	private CommonExample createExample(String intent, String text) {
		CommonExample example = new CommonExample();
		example.setIntent(intent);
		example.setText(text);
		return example;
	}

	/**
	 * @param userContext
	 * @param bBID
	 * @param message
	 * @return
	 * @throws ScopeException 
	 */
	public CardInfo query(String BBID, String message) throws ScopeException {
		Space space = getSpace(userContext, BBID);
		//
		// read the json message
		RasaQuery query = readMessage(message);
		//
		if (query.getIntent()==null) {
			throw new ScopeException("404: no intent detected");
		}
		// bookmark_top_dimension
		if (query.getIntent().getName().equals("bookmark_top_dimension")) {
			String answer = "";
			EntityExtraction dimension = getEntity(query, "dimension");
			if (dimension!=null && dimension.getValue().startsWith("@")) {
				Axis axis = space.getUniverse().axis(dimension.getValue());
				if (!axis.getParent().getTop().equals(space)) {
					throw new ScopeException("invalid dimension request - you are using the wrong model");
				}
				answer = "Here are the top "+getAxisSHortName(axis);
				// query the bookmark with dimension
				AnalyticsQueryImpl aquery = new AnalyticsQueryImpl();
				aquery.setGroupBy(new ArrayList<>());
				aquery.getGroupBy().add(axis.prettyPrint(new PrettyPrintOptions(ReferenceStyle.IDENTIFIER, space.getImageDomain())));
				// look for a metric
				aquery.setMetrics(new ArrayList<>());
				aquery.setOrderBy(new ArrayList<>());
				aquery.setLimit(5L);
				EntityExtraction metric = getEntity(query, "metric");
				if (metric!=null && metric.getValue().startsWith("@")) {
					// ok, we found one
					Measure measure = space.getUniverse().measure(metric.getValue());
					if (!measure.getParent().getTop().equals(space)) {
						throw new ScopeException("invalid metric request - you are using the wrong model");
					}
					answer += " by "+measure.getName();
					aquery.getMetrics().add(measure.prettyPrint(new PrettyPrintOptions(ReferenceStyle.IDENTIFIER, space.getImageDomain())));
					aquery.getOrderBy().add("DESC("+measure.prettyPrint(new PrettyPrintOptions(ReferenceStyle.IDENTIFIER, space.getImageDomain()))+")");
				} else {
					aquery.getMetrics().add("count()");
					aquery.getOrderBy().add("DESC(count())");
				}
				return runQuery(space, aquery, answer+": ");
			} else {
				throw new ScopeException("I don't understand what dimension you are looking for. '"+dimension.getValue()+"' is not a valid dimension. You can try the following dimensions: NYI");
			}
		}
		// bookmark_overall_metric
		if (query.getIntent().getName().equals("bookmark_overall_metric")) {
			EntityExtraction entity = getEntity(query, "metric");
			if (entity==null) {
				query.getIntent().setName("bookmark_overview");// redirect
			}
			if (entity!=null && entity.getValue().startsWith("@")) {
				// ok, we found one
				Measure measure = space.getUniverse().measure(entity.getValue());
				if (!measure.getParent().getTop().equals(space)) {
					throw new ScopeException("invalid metric request - you are using the wrong model");
				}
				// query the bookmark without dimensions
				AnalyticsQueryImpl aquery = new AnalyticsQueryImpl();
				aquery.setGroupBy(new ArrayList<>());// empty - not sure it's going to work
				aquery.setMetrics(new ArrayList<>());
				aquery.getMetrics().add(measure.prettyPrint(new PrettyPrintOptions(ReferenceStyle.IDENTIFIER, space.getImageDomain())));
				return runQuery(space, aquery, "Here are the overall results for "+measure.getName()+": ");
			} else {
				throw new ScopeException("I don't understand what metric you are looking for. '"+entity.getValue()+"' is not a valid metric. You can try the following metrics: NYI");
			}
		}
		// bookmark_overview
		if (query.getIntent().getName().equals("bookmark_overview")) {
			// query the bookmark without dimensions
			AnalyticsQueryImpl aquery = new AnalyticsQueryImpl();
			aquery.setGroupBy(new ArrayList<>());// empty - not sure it's going to work
			return runQuery(space, aquery, "Here are the overall results for "+getContextName(space)+": ");
		}
		// else
		throw new ScopeException("404: intent not yet implemented");
	}
	
	private CardInfo runQuery(Space space, AnalyticsQueryImpl query, String successMsg) throws ScopeException {
		try {
			AnalyticsReply reply = runAnalysis(userContext, space.getBBID(Style.ROBOT), null, query, DataLayout.RECORDS, false, true, null);
			AnalyticsResult result = (AnalyticsResult)reply.getResult();
			Object[] data = (Object[])result.getData();
			if (result.getInfo().getTotalSize()>1) {
				String rowAsString = null;
				for (Object row : data) {
					if (rowAsString==null) rowAsString = ""; else rowAsString += "/n";
					HashMap<String, Object> map = (HashMap<String, Object>)row;
					for (Column col : result.getHeader().getColumns()) {
						rowAsString += map.get(col.getName())+" ";
					}
				}
				CardInfo info = new CardInfo(successMsg.trim()+" "+rowAsString);
				return info;
			} else if (result.getInfo().getTotalSize()==1) {
				HashMap<String, Object> row = (HashMap<String, Object>)data[0];
				String rowAsString = "";
				for (Column col : result.getHeader().getColumns()) {
					if (col.getRole().equals(Role.METRIC)) {
						rowAsString += col.getName()+" "+row.get(col.getName());
					}
				}
				CardInfo info = new CardInfo(successMsg.trim()+" "+rowAsString);
				return info;
			} else {
				return new CardInfo("Sorry I couldn't find the overall results for "+getContextName(space)+"...");
			}
		} catch (Throwable e) {
			throw new ScopeException("500: sorry, cannot run the analysis");
		}
	}
	
	/**
	 * @param query
	 * @param string
	 * @return
	 */
	private EntityExtraction getEntity(RasaQuery query, String entity) {
		if (query.getEntities()!=null) {
			for (EntityExtraction item : query.getEntities()) {
				if (item.getEntity().equals(entity)) {
					return item;
				}
			}
		}
		// else
		return null;
	}

	public RasaQuery readMessage(String message) throws ScopeException {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		try {
			RasaQuery config = mapper.readValue(message, RasaQuery.class);
			return config;
		} catch (Exception e) {
			throw new ScopeException("cannot read the message, maybe you should try to say Hello!", e);
		}
	}

}
