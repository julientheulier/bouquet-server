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

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.squid.core.database.impl.DatabaseServiceException;
import com.squid.core.domain.IDomain;
import com.squid.core.expression.PrettyPrintOptions;
import com.squid.core.expression.PrettyPrintOptions.ReferenceStyle;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.analytics.AnalyticsServiceCore;
import com.squid.kraken.v4.api.core.customer.StateServiceBaseImpl;
import com.squid.kraken.v4.api.core.nlu.CardInfo.Status;
import com.squid.kraken.v4.api.core.nlu.rasa.CommonExample;
import com.squid.kraken.v4.api.core.nlu.rasa.Entity;
import com.squid.kraken.v4.api.core.nlu.rasa.EntityExtraction;
import com.squid.kraken.v4.api.core.nlu.rasa.RasaNluData;
import com.squid.kraken.v4.api.core.nlu.rasa.RasaQuery;
import com.squid.kraken.v4.api.core.nlu.rasa.TrainingSet;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingService;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Measure;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.AnalyticsQueryImpl;
import com.squid.kraken.v4.model.AnalyticsReply;
import com.squid.kraken.v4.model.AnalyticsResult;
import com.squid.kraken.v4.model.BookmarkConfig;
import com.squid.kraken.v4.model.DataHeader.Column;
import com.squid.kraken.v4.model.DataHeader.Role;
import com.squid.kraken.v4.model.DataLayout;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.FacetMember;
import com.squid.kraken.v4.model.FacetMemberString;
import com.squid.kraken.v4.model.ViewQuery;
import com.squid.kraken.v4.model.ViewReply;
import com.squid.kraken.v4.model.NavigationQuery.Style;
import com.squid.kraken.v4.model.State;
import com.squid.kraken.v4.model.StatePK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.vegalite.VegaliteSpecs;

/**
 * @author sergefantino
 *
 */
public class NluServiceBaseImpl extends AnalyticsServiceCore {
	
	/**
	 * 
	 */
	private static final String DIALOG_HELP = "help";
	/**
	 * 
	 */
	private static final String ITEM_ENTITY_PREFIX = "item:";
	/**
	 * 
	 */
	private static final String DIMENSION_ENTITY = "dimension";
	/**
	 * 
	 */
	private static final String METRIC_ENTITY = "metric";
	/**
	 * 
	 */
	private static final String BOOKMARK_DRILL_DOWN = "bookmark_drill_down";
	/**
	 * 
	 */
	private static final String BOOKMARK_TOP_DIMENSION = "bookmark_top_dimension";
	/**
	 * 
	 */
	private static final String BOOKMARK_OVERALL_METRIC = "bookmark_overall_metric";
	/**
	 * 
	 */
	private static final String BOOKMARK_OVERVIEW = "bookmark_overview";
	

	private static final ReferenceStyle defaultStyle = ReferenceStyle.NAME;
	
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
	
	public Response runTest(String BBID) {
		String result = "";
		TrainingSet set = generateTrainingSet(BBID, false);
		String previous_intent = null;
		int count = 0;
		int error = 0;
		for (CommonExample example : set.getRasa_nlu_data().getCommon_examples()) {
			// if no intent is set, use the previous one
			if (example.getIntent()!=null) {
				previous_intent = example.getIntent();
			} else {
				example.setIntent(previous_intent);
			}
			try {
				RasaQuery query = parseMessage(example.getText());
				if (query.getIntent().getName().equals(example.getIntent())) {
					for (Entity entity : example.getEntities()) {
						EntityExtraction extractor = getEntity(query, entity.getEntity());
						if (extractor!=null) {
							if (extractor.getValue().equalsIgnoreCase(entity.getValue())) {
								// no message
								count++;
							} else {
								if (("\""+extractor.getValue()+"\"").equalsIgnoreCase(entity.getValue())) {
									count++;// ok this is the same
								} else {
									String lookup = lookupEntityValue(BBID, query.getText(), extractor);
									if (lookup!=null && ("\""+lookup+"\"").equalsIgnoreCase(entity.getValue())) {
										result += "<li>warning: looking up '"+extractor.getValue()+"' to be in fact '"+lookup+"'";
										count++;
									} else {
										error++;
										result += "<li>failed to detect value of entity '"+entity.getEntity()+"' for message='"+example.getText()+"': found:"+extractor.getValue()+", expecting:"+entity.getValue();
									}
								}
							}
						} else {
							extractor = getEntityByPrefix(query, ITEM_ENTITY_PREFIX);
							if (extractor==null) {
								error++;
								result += "<li>failed to detect entity '"+entity.getEntity()+"' for message='"+example.getText()+"'";
							} else {
								result += "<li>ambiguius entity detection for message='"+example.getText()+"', expecting '"+entity.getEntity()+"' but found '"+extractor.getEntity()+"'";
							}
						}
					}
				} else {
					error++;
					result += "<li>failed to detect intent for message='"+example.getText()+"': found:"+query.getIntent().getName()+", expecting:"+example.getIntent();
				}
			} catch (ScopeException | IOException e) {
				error++;
				result += "<li>failed to parse message='"+example.getText()+"': "+e.getMessage();
			}
		}
		result += "<li> "+count+" tests are successfull, "+error+" errors, "+((count-error)*100.0/count)+" % match";
		return Response.ok(result,"text/html").build();
	}
	
	/**
	 * @param message 
	 * @param bBID
	 * @param entity
	 * @param value
	 * @return
	 */
	private String lookupEntityValue(String BBID, String message, EntityExtraction item) {
		try {
			Space space = getSpace(userContext, BBID);
			String dimension = item.getEntity().substring(ITEM_ENTITY_PREFIX.length());
			String value = item.getValue();
			// we need to lookup the actual value
			Axis axis = space.getUniverse().axis(dimension);
			if (!axis.getParent().getTop().equals(space)) {
				throw new ScopeException("invalid dimension request - you are using the wrong model");
			}
			DashboardSelection empty = new DashboardSelection();
			Facet facet = ComputingService.INSTANCE.glitterFacet(space.getUniverse(),
					space.getDomain(), empty, axis, value, 0, 50, null);
			if (facet.getItems().isEmpty()) {
				throw new ScopeException("I don't know any "+axis.getDimension().getName()+" that matchs "+value);
			}
			// just one
			FacetMember checked = getMatchingFacet(message, item, facet);
			if (checked!=null) {
				return checked.toString();
			} else {
				return null;
			}
		} catch (ScopeException | ComputingException | InterruptedException | TimeoutException e) {
			return null;
		}
	}
	
	private FacetMember getMatchingFacet(String message, EntityExtraction item, Facet facet) {
		if (facet.getItems().size()>1) {
			// ok let's try deeper
			String extendedExtraction = message.substring(item.getStart());// get the complete message beginning at extraction
			FacetMember match = null;
			for (FacetMember member : facet.getItems()) {
				if (extendedExtraction.startsWith(member.toString())) {
					if (match==null || match.toString().length()<member.toString().length()) {
						match = member;
					}
				}
			}
			return match;
		} else {
			// just one
			return facet.getItems().get(0);
		}
	}

	public TrainingSet generateTrainingSet(String BBID) {
		return generateTrainingSet(BBID, true);
	}

	/**
	 * @param userContext
	 * @param bBID
	 * @return
	 */
	public TrainingSet generateTrainingSet(String BBID, boolean training) {
		//
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
		common_examples.add(createExample(DIALOG_HELP, "Hello!"));
		common_examples.add(createExample(DIALOG_HELP, "What can you do for me?"));
		common_examples.add(createExample(DIALOG_HELP, "Help?"));
		common_examples.add(createExample(DIALOG_HELP, "What's your name?"));
		// bookmark_overview: display all kpis for the current period
		common_examples.add(createExample(BOOKMARK_OVERVIEW, "What are the overall results?"));
		common_examples.add(createExample(BOOKMARK_OVERVIEW, "What are the numbers?"));
		common_examples.add(createExample(BOOKMARK_OVERVIEW, "What are the total numbers?"));
		common_examples.add(createExample(BOOKMARK_OVERVIEW, "What is the situation?"));
		common_examples.add(createExample(BOOKMARK_OVERVIEW, "What are the key metrics?"));
		common_examples.add(createExample(BOOKMARK_OVERVIEW, "What are the kpis?"));
		common_examples.add(createExample(BOOKMARK_OVERVIEW, "What are the key metrics look like?"));
		common_examples.add(createExample(BOOKMARK_OVERVIEW, "What are the kpis look like?"));
		common_examples.add(createExample(BOOKMARK_OVERVIEW, "How is my business going?"));
		common_examples.add(createExample(BOOKMARK_OVERVIEW, "How is my business doing?"));
		common_examples.add(createExample(BOOKMARK_OVERVIEW, "What are the "+contextName+"?"));
		common_examples.add(createExample(BOOKMARK_OVERVIEW, "What are the "+contextName+" kpis?"));
		//
		// bookmark_overall_metric: return a single metric for the current period, compare to previous period if set
		//
		for (Measure m : space.M()) {
			if (m.getMetric()!=null && !m.getMetric().isDynamic()) {
				String name = normalize(m.getName());
				String ID = m.prettyPrint(new PrettyPrintOptions(defaultStyle, null));
				CommonExample example = createExample(BOOKMARK_OVERALL_METRIC, "What are the %metric ?");
				if (addEntity(example,METRIC_ENTITY,name,ID)) common_examples.add(example);
				CommonExample example1 = createExample(BOOKMARK_OVERALL_METRIC, "What are the %metric overview ?");
				if (addEntity(example1,METRIC_ENTITY,name,ID)) common_examples.add(example1);
				CommonExample example2 = createExample(BOOKMARK_OVERALL_METRIC, "What are the total %metric ?");
				if (addEntity(example2,METRIC_ENTITY,name,ID)) common_examples.add(example2);
			}
		}
		// trying the same but by using a different intent for each
		/*
		for (Measure m : space.M()) {
			if (m.getMetric()!=null && !m.getMetric().isDynamic()) {
				String name = m.getName();
				String ID = m.prettyPrint(new PrettyPrintOptions(defaultStyle, null));
				common_examples.add(createExample("bookmark_overall_metric:"+ID, "What are the "+name+" ?"));
				common_examples.add(createExample("bookmark_overall_metric:"+ID, "What are the overall "+name+" ?"));
				common_examples.add(createExample("bookmark_overall_metric:"+ID, "What are the total "+name+" ?"));
			}
		}
		*/
		//
		// bookmark_top_dimension: return the top 5 dimension, for some metric, for the current period
		//
		for (Axis axis : space.A(true)) {
			IDomain image = axis.getDefinitionSafe().getImageDomain();
			if (!image.isInstanceOf(IDomain.OBJECT)) {
				if (axis.getDimension().getType().equals(Type.CATEGORICAL)) {
					String dim_name = normalize(getAxisSHortName(axis));
					String dim_ID = axis.prettyPrint(new PrettyPrintOptions(defaultStyle, null));
					{
						CommonExample example = createExample(BOOKMARK_TOP_DIMENSION, "What are the top %dimension ?");
						if (addEntity(example,DIMENSION_ENTITY,dim_name,dim_ID)) common_examples.add(example);
					}
					for (Measure m : space.M()) {
						if (m.getMetric()!=null && !m.getMetric().isDynamic()) {
							String name = m.getName();
							String ID = m.prettyPrint(new PrettyPrintOptions(defaultStyle, null));
							CommonExample example = createExample(BOOKMARK_TOP_DIMENSION, "What are the top %dimension by %metric ?");
							if (addEntity(example,DIMENSION_ENTITY,dim_name,dim_ID) && addEntity(example,METRIC_ENTITY,name,ID)) common_examples.add(example);
						}
					}
				}
			}
		}
		//
		// bookmark_drill_down: display all or specifc kpis for a given facet item
		//
		for (Axis axis : space.A(true)) {
			IDomain image = axis.getDefinitionSafe().getImageDomain();
			if (!image.isInstanceOf(IDomain.OBJECT) && image.isInstanceOf(IDomain.STRING)) {
				if (axis.getDimension().getType().equals(Type.CATEGORICAL)) {
					String dim_name = getAxisSHortName(axis);
					String dim_ID = axis.prettyPrint(new PrettyPrintOptions(defaultStyle, null));
					String intent_name = BOOKMARK_DRILL_DOWN;
					// look for the facets
					try {
						DashboardSelection empty = new DashboardSelection();
						int size = training?1000:1000;
						Facet facet = ComputingService.INSTANCE.glitterFacet(space.getUniverse(),
								space.getDomain(), empty, axis, "", 0, size, null);
						int count = 0;
						for (FacetMember item : facet.getItems()) {
							FacetMemberString member = (FacetMemberString)item;
							{
								CommonExample example = createExample((count<10&&training)?intent_name:null, "What are the results for %item:"+dim_ID+"?");
								if (addEntity(example,ITEM_ENTITY_PREFIX+dim_ID,member.getValue(),"\""+member.getValue()+"\"")) common_examples.add(example);
							}
							if (count<10) {// the same by appending the dimension name, only for intent
								CommonExample example = createExample(intent_name, "What are the results for %item:"+dim_ID+" "+dim_name+"?");
								if (addEntity(example,ITEM_ENTITY_PREFIX+dim_ID,member.getValue(),"\""+member.getValue()+"\"")) common_examples.add(example);
							}
							// we could use the attributes too
							count++;
						}
					} catch (TimeoutException | ComputingException | InterruptedException e) {
						// ignore
					}
				}
			}
		}
		return set;
	}
	
	/**
	 * @param name
	 * @return
	 */
	private String normalize(String name) {
		return name.toLowerCase();
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
		return normalize(contextName);
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
	 * @param state 
	 * @return
	 * @throws ScopeException 
	 */
	public CardInfo query(String BBID, String message, String state) throws ScopeException {
		//
		// read the json message
		RasaQuery query = readMessage(message);
		//
		try {
			Space space = getSpace(userContext, BBID);
			//
			if (query.getIntent()==null) {
				throw new ScopeException("404: no intent detected");
			}
			// help
			if (query.getIntent().getName().equals(DIALOG_HELP)) {
				String info = "Hello, I am the Bouquet Bot, your Personnal Analytic Assistant. In general you can ask me many questions about "+getContextName(space)+" from the "+space.getUniverse().getProject().getName()+" database.";
				info += "\nFor example you can ask me:";
				CardInfo card = new CardInfo(info);
				card.addFollowUp("What are the overall results?");
				return card;
			}
			// bookmark drill-down
			else if (query.getIntent().getName().equals(BOOKMARK_DRILL_DOWN)) {
				EntityExtraction item = getEntityByPrefix(query, ITEM_ENTITY_PREFIX);
				if (item!=null) {
					String dimension = item.getEntity().substring(ITEM_ENTITY_PREFIX.length());
					String value = item.getValue();
					// we need to lookup the actual value
					Axis axis = space.getUniverse().axis(dimension);
					if (!axis.getParent().getTop().equals(space)) {
						throw new ScopeException("invalid dimension request - you are using the wrong model");
					}
					if (!value.startsWith("\"")) {
						DashboardSelection empty = new DashboardSelection();
						try {
							Facet facet = ComputingService.INSTANCE.glitterFacet(space.getUniverse(),
									space.getDomain(), empty, axis, value, 0, 50, null);
							if (facet.getItems().isEmpty()) {
								throw new ScopeException("I don't know any "+axis.getDimension().getName()+" that matchs "+value);
							}
							FacetMember checked = getMatchingFacet(message, item, facet);
							if (checked==null) {
								return addParserOutput(new CardInfo("The "+axis.getDimension().getName()+" that matchs "+value+" is ambiguous: would you want to say: "), query);
							}
							// query the bookmark without dimensions
							AnalyticsQueryImpl aquery = new AnalyticsQueryImpl();
							aquery.setGroupBy(new ArrayList<>());// empty - not sure it's going to work
							// add filter
							String filter = axis.prettyPrint(new PrettyPrintOptions(ReferenceStyle.IDENTIFIER, space.getImageDomain()));
							filter += "=\""+checked+"\"";
							aquery.setFilters(new ArrayList<>());
							aquery.getFilters().add(filter);
							return addParserOutput(runQuery(space, state, aquery, "results for "+checked+" "+axis.getDimension().getName()+": "), query);
						} catch (ComputingException | InterruptedException | TimeoutException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} else {
						AnalyticsQueryImpl aquery = new AnalyticsQueryImpl();
						aquery.setGroupBy(new ArrayList<>());// empty - not sure it's going to work
						// add filter
						String filter = axis.prettyPrint(new PrettyPrintOptions(ReferenceStyle.IDENTIFIER, space.getImageDomain()));
						filter += "="+value;
						aquery.setFilters(new ArrayList<>());
						aquery.getFilters().add(filter);
						return addParserOutput(runQuery(space, state, aquery, "results for "+value+" "+axis.getDimension().getName()+": "), query);
					}
				}
			}
			// bookmark_top_dimension
			if (query.getIntent().getName().equals(BOOKMARK_TOP_DIMENSION)) {
				String answer = "";
				EntityExtraction entity = getEntity(query, DIMENSION_ENTITY);
				if (entity==null) {
					return addAvailableDimensions(new CardInfo("I don't understand what dimension you are looking for. You can try the following:"),space);
				}
				String dimension = lookupDimension(space, entity);
				if (dimension!=null) {
					Axis axis = space.getUniverse().axis(dimension);
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
					EntityExtraction metric = getEntity(query, METRIC_ENTITY);
					if (metric!=null && (metric.getValue().startsWith("@") || metric.getValue().startsWith("'"))) {
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
					return addParserOutput(runQuery(space, state, aquery, answer+": "), query);
				} else {
					return addAvailableDimensions(CardInfo.incomplete("I don't understand what dimension you are looking for. '"+entity.getValue()+"' is not a valid dimension. You can try the following:"), space);
				}
			}
			// bookmark_overall_metric
			if (query.getIntent().getName().equals(BOOKMARK_OVERALL_METRIC)) {
				EntityExtraction metric = getEntity(query, METRIC_ENTITY);
				if (metric==null) {
					query.getIntent().setName(BOOKMARK_OVERVIEW);// redirect
				}
				if (metric!=null && (metric.getValue().startsWith("@") || metric.getValue().startsWith("'"))) {
					// ok, we found one
					Measure measure = space.getUniverse().measure(metric.getValue());
					if (!measure.getParent().getTop().equals(space)) {
						throw new ScopeException("invalid metric request - you are using the wrong model");
					}
					// query the bookmark without dimensions
					AnalyticsQueryImpl aquery = new AnalyticsQueryImpl();
					//aquery.setGroupBy(new ArrayList<>());// empty - not sure it's going to work
					aquery.setMetrics(new ArrayList<>());
					aquery.getMetrics().add(measure.prettyPrint(new PrettyPrintOptions(ReferenceStyle.IDENTIFIER, space.getImageDomain())));
					return addParserOutput(runQuery(space, state, aquery, "Here are the overall results for "+measure.getName()+": "), query);
				} else {
					if (metric!=null) {
						return CardInfo.incomplete("I don't understand what metric you are looking for. '"+metric.getValue()+"' is not a valid metric. You can try the following metrics: NYI");
					} else {
						return CardInfo.incomplete("I don't understand what metric you are looking for. You can try the following metrics: NYI");
					}
				}
			}
			// bookmark_overview
			if (query.getIntent().getName().equals(BOOKMARK_OVERVIEW)) {
				// query the bookmark without dimensions
				AnalyticsQueryImpl aquery = new AnalyticsQueryImpl();
				aquery.setGroupBy(new ArrayList<>());// empty - not sure it's going to work
				return addParserOutput(runQuery(space, state, aquery, "Here are the overall results for "+getContextName(space)+": "), query);
			}
			// else
			throw new ScopeException("404: intent not yet implemented: "+message);
		} catch (DatabaseServiceException e) {
			if (query.getIntent().getName().equals(DIALOG_HELP)) {
				return new CardInfo("Hello, I am the Bouquet Bot, your Personnal Analytic Assistant. In general you can ask me many questions about your data. But unfortunately it looks like I have trouble accessing the database right now... maybe you should try again later ?");
			} else {
				return new CardInfo("Hum, it looks like I have trouble accessing the database right now... maybe you should try again later ?");
			}
		}
	}
	
	protected State createState(Space space, AnalyticsQueryImpl query) throws ScopeException, IOException {
		BookmarkConfig config = createBookmarkConfig(space, query);
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		String json = mapper.writeValueAsString(config);
		JsonNode tree = mapper.readTree(json);
		StatePK pk = new StatePK(userContext.getCustomerId());
		State state = new State(pk);
		state.setConfig(tree);
		return state;
	}
	
	/**
	 * @param message 
	 * @param space
	 * @return
	 */
	private CardInfo addAvailableDimensions(CardInfo card, Space space) {
		for (Axis axis : space.A(true)) {
			IDomain image = axis.getDefinitionSafe().getImageDomain();
			if (!image.isInstanceOf(IDomain.OBJECT)) {
				if (axis.getDimension().getType().equals(Type.CATEGORICAL)) {
					String dim_name = getAxisSHortName(axis);
					card.addFollowUp("What are the top "+dim_name+" ?");
				}
			}
		}
		return card;
	}

	private String lookupDimension(Space space, EntityExtraction entity) {
		String value = entity.getValue();
		if (value.startsWith("@")) return value;// this is an ID
		if (value.startsWith("'")) return value;// this is an name
		String not_sure = null;
		value = value.toLowerCase();
		for (Axis axis : space.A(true)) {
			IDomain image = axis.getDefinitionSafe().getImageDomain();
			if (!image.isInstanceOf(IDomain.OBJECT)) {
				if (axis.getDimension().getType().equals(Type.CATEGORICAL)) {
					String dim_name = getAxisSHortName(axis);
					if (dim_name.equalsIgnoreCase(value)) {
						String dim_ID = axis.prettyPrint(new PrettyPrintOptions(defaultStyle, null));
						return dim_ID;
					}
					dim_name = dim_name.toLowerCase();
					if (dim_name.startsWith(value) || value.startsWith(dim_name)) {
						if (not_sure==null) {
							not_sure = axis.prettyPrint(new PrettyPrintOptions(defaultStyle, null));
						}
					}
				}
			}
		}
		//
		return not_sure;
	}
	
	private CardInfo addParserOutput(CardInfo info, RasaQuery query) {
		info.setParserOutput(query);
		return info;
	}
	
	private CardInfo runQuery(Space space, String state, AnalyticsQueryImpl query, String successMsg) throws ScopeException {
		try {
			AnalyticsReply reply = runAnalysis(userContext, space.getBBID(Style.ROBOT), state, query, DataLayout.RECORDS, false, true, null);
			AnalyticsResult result = (AnalyticsResult)reply.getResult();
			Object[] data = (Object[])result.getData();
			ArrayList<String> followUp = new ArrayList<>();
			if (result.getInfo().getTotalSize()>1) {
				String rowAsString = "";
				for (Object row : data) {
					rowAsString += "<li>";
					HashMap<String, Object> map = (HashMap<String, Object>)row;
					for (Column col : result.getHeader().getColumns()) {
						rowAsString += map.get(col.getName())+" | ";
						if (col.getRole()==Role.GROUPBY) {
							followUp.add("what are results for "+map.get(col.getName()));
						}
					}
				}
				CardInfo info = new CardInfo(successMsg.trim()+" "+rowAsString);
				//
				info.setFollowUp(followUp);
				//
				ViewQuery view = new ViewQuery(query);
				ViewReply viz = viewAnalysis(userContext, space.getBBID(Style.ROBOT), view, ViewQuery.DataMode.EMBEDDED.toString(), true, true);
				info.setDataviz(viz.getResult());
				//
				State newState = StateServiceBaseImpl.getInstance().store(userContext, createState(space, query));
				info.setState(newState.getOid());
				//
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
				//
				ViewQuery view = new ViewQuery(query);
				ViewReply viz = viewAnalysis(userContext, space.getBBID(Style.ROBOT), view, ViewQuery.DataMode.EMBEDDED.toString(), true, true);
				info.setDataviz(viz.getResult());
				//
				State newState = StateServiceBaseImpl.getInstance().store(userContext, createState(space, query));
				info.setState(newState.getOid());
				//
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
	
	/**
	 * @param query
	 * @param string
	 * @return
	 */
	private EntityExtraction getEntityByPrefix(RasaQuery query, String prefix) {
		if (query.getEntities()!=null) {
			for (EntityExtraction item : query.getEntities()) {
				if (item.getEntity().startsWith(prefix)) {
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

	/**
	 * @param bBID
	 * @param message
	 * @return
	 * @throws IOException 
	 * @throws ScopeException 
	 */
	public CardInfo chat(String BBID, String message) throws IOException, ScopeException {
		URL parser = new URL("http://192.168.99.100:5000/parse?q="+URLEncoder.encode(message, "UTF-8"));
		URLConnection parse = parser.openConnection();
        String result = IOUtils.toString(parse.getInputStream(), StandardCharsets.UTF_8);
        String state = null;// to be defined
        return query(BBID, result, state);
	}
	
	private RasaQuery parseMessage(String message) throws IOException, ScopeException {
		URL parser = new URL("http://192.168.99.100:5000/parse?q="+URLEncoder.encode(message, "UTF-8"));
		URLConnection parse = parser.openConnection();
        String result = IOUtils.toString(parse.getInputStream(), StandardCharsets.UTF_8);
        return readMessage(result);
	}
	
	public Response generateUI(String BBID, String message, String state) throws IOException, ScopeException {
		URL parser = new URL("http://192.168.99.100:5000/parse?q="+URLEncoder.encode(message, "UTF-8"));
		URLConnection parse = parser.openConnection();
        String result = IOUtils.toString(parse.getInputStream(), StandardCharsets.UTF_8);
        CardInfo card = query(BBID, result, state);
        // display the simple UI
        StringBuilder html = createHTMLHeader("Chat Bot");
        html.append("<body>");
        if (message!=null && message.length()>0) {
        	html.append("<p><span class='label label-primary'>You:</span>&nbsp;"+message+"</p>");
        }
        String status = card.getStatus()==Status.VALID?"success":(card.getStatus()==Status.INCOMPLETE?"warning":"error");
        html.append("<p><span class='label label-"+status+"'>Bouquet:</span>&nbsp;"+card.getMessage().replaceAll("\n", "<br>")+"</p>");
        if (card.getDataviz()!=null) {
        	html.append("<script src=\"//d3js.org/d3.v3.min.js\" charset=\"utf-8\"></script>\n" +
    				"<script src=\"https://cdnjs.cloudflare.com/ajax/libs/vega/2.5.0/vega.min.js\"></script>\n" +
    				"  <script src=\"https://cdnjs.cloudflare.com/ajax/libs/vega-lite/1.0.7/vega-lite.min.js\"></script>\n" +
    				"<script src=\"https://cdnjs.cloudflare.com/ajax/libs/vega-embed/2.2.0/vega-embed.min.js\" charset=\"utf-8\"></script>");
        	html.append("<div id=\"vis\"></div>\r\n\r\n<script>\r\nvar embedSpec = {\r\n  mode: \"vega-lite\", renderer:\"svg\",  spec:");
			html.append(writeVegalightSpecs(card.getDataviz()));
			//channels = reply.getResult().encoding;
			html.append("}\r\nvg.embed(\"#vis\", embedSpec, function(error, result) {\r\n  // Callback receiving the View instance and parsed Vega spec\r\n  // result.view is the View, which resides under the '#vis' element\r\n});\r\n</script>\r\n");
        }
        if (!card.getFollowUp().isEmpty()) {
        	for (String followUp : card.getFollowUp()) {
        		html.append("<li><a href='?msg="+followUp+"&access_token="+userContext.getToken().getOid()+"'>"+followUp+"</a>");
        	}
        }
        html.append("<form><label><span class='label label-primary'>You:</span></label><input type='text' name='msg' size=100>");
        html.append("<input type=\"hidden\" name=\"access_token\" value=\""+userContext.getToken().getOid()+"\">");
        if (card.getState()!=null && card.getState().length()>0) html.append("<input type=\"hidden\" name=\"state\" value=\""+card.getState()+"\">");
        html.append("</form>");
        html.append("<button type='submit' value='Ask'><i class=\"fa fa-refresh\" aria-hidden=\"true\"></i>&nbsp;Ask</button>");
        html.append("</body>");
        return Response.ok(html.toString(),"text/html").build();
	}

	private String writeVegalightSpecs(VegaliteSpecs specs) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			mapper.setSerializationInclusion(Include.NON_NULL);
			return mapper.writeValueAsString(specs);
		} catch (JsonProcessingException e) {
			throw new APIException("failed to write vegalite specs to JSON", e, true);
		}
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

}
