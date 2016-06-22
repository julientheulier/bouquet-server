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
package com.squid.kraken.v4.ESIndexFacade;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.recovery.ShardRecoveryResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TypeFilterBuilder;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.recovery.RecoveryState.Stage;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.ESIndexFacade.ESMapping.ESIndexMapping;
import com.squid.kraken.v4.ESIndexFacade.ESMapping.ESTypeMapping;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.engine.index.DimensionStoreES;

public class ESIndexFacade implements IESIndexFacade {

	static final Logger logger = LoggerFactory.getLogger(ESIndexFacade.class);

	private Client client;
	private Node node;

	private static int MIN_NGRAM = 1;
	private static int MAX_NGRAM = 6;

	public ESIndexFacade() {
	}

	public void start(Node n) {
		this.node = n;
		client = node.client();
	}

	public void stop() {
		this.client.close();
		node.close();
	}

	// one index per domain

	@Override
	public boolean addDimensionMapping(String domainName, String dimensionName, String idFieldName,
			HashMap<String, ESMapping> mapping) throws ESIndexFacadeException {

		return this.createMapping(domainName, dimensionName, idFieldName, mapping);
	}

	@Override
	public boolean dimensionMappingInES(String domainName, String dimensionName) throws ESIndexFacadeException {
		if (domainInES(domainName)) {
			try {
				TypesExistsResponse exists = client.admin().indices().prepareTypesExists(domainName)
						.setTypes(dimensionName).execute().actionGet();
				return exists.isExists();
			} catch (Exception e) {
				throw new ESIndexFacadeException("failed checking type for " + domainName + "/" + dimensionName, e);
			}
		} else {
			return false;
		}
	}

	@Override
	public void updateDimension(String projectName, String dimensionName, HashMap<String, ESMapping> mapping)
			throws ESIndexFacadeException {
		// TODO Auto-generated method stub

	}

	// one index per domain
	@Override
	public void addDomain(String domainName) throws ESIndexFacadeException {
		try {
			/*
			 * CreateIndexRequest request = new CreateIndexRequest(projectName);
			 * CreateIndexResponse response = client.admin().indices()
			 * .create(request).get();
			 */

			XContentBuilder analyzerBuilder = XContentFactory.jsonBuilder();

			String settings = analyzerBuilder.startObject().startObject("analysis").startObject("analyzer")
					.startObject("my_ngram_analyzer").field("tokenizer", "my_ngram_tokenizer")
					.field("filter", "lowercase").endObject().endObject().startObject("tokenizer")
					.startObject("my_ngram_tokenizer").field("type", "nGram").field("min_gram", MIN_NGRAM)
					.field("max_gram", MAX_NGRAM).endObject().endObject()
					// .endObject()
					.endObject().string();

			CreateIndexResponse response = client.admin().indices().prepareCreate(domainName)
					.setSettings(ImmutableSettings.settingsBuilder().loadFromSource(settings)).execute().actionGet();

			// wait until the project is available - for 10s; if still not
			// available, just continue?
			int retry = 0;
			while (!domainInES(domainName) && ++retry < 60) {
				Thread.sleep(1000);
			}
			logger.info("settings\n" + settings);
			logger.info("index creation " + domainName + " response " + response.isAcknowledged());
		} catch (IndexAlreadyExistsException e) {
			logger.info("index " + domainName + " already exists");
		} catch (Exception e) {
			e.printStackTrace();
			throw new ESIndexFacadeException("failed creating a new Index for project " + domainName, e);
		}
	}

	@Override
	public void removeDomain(String domainName) throws ESIndexFacadeException {
		try {
			DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest(domainName))
					.actionGet();
			if (!delete.isAcknowledged()) {
				logger.info("Index " + domainName + " wasn't deleted");
				throw new ESIndexFacadeException("Index " + domainName + " wasn't deleted");
			}
		} catch (IndexMissingException e) {
			logger.info("Index " + domainName + " does not exist");
		} catch (Exception e) {
			throw new ESIndexFacadeException("failed removing a new Index for domain " + domainName, e);
		}
	}

	@Override
	public boolean domainInES(String domainName) {
		try {
			IndicesExistsResponse exists = client.admin().indices().exists(new IndicesExistsRequest(domainName))
					.actionGet();
			return exists.isExists();
		} catch (Exception e) {
			logger.info("failed checking index for project " + domainName, e);
			return false;
		}
	}

	@Override
	public boolean isRecoveryDone(String indexName) throws ESIndexFacadeException {
		try {
			RecoveryRequest req = new RecoveryRequest(indexName);
			RecoveryResponse resp = client.admin().indices().recoveries(req).actionGet();
			Map<String, List<ShardRecoveryResponse>> shardStates = resp.shardResponses();
			boolean res = true;
			for (String s : shardStates.keySet()) {
				logger.info(s);
				List<ShardRecoveryResponse> l = shardStates.get(s);
				for (ShardRecoveryResponse shardResp : l) {
					;
					Stage stage = shardResp.recoveryState().getStage();
					logger.info("shard id " + shardResp.getShardId() + "  " + stage);
					res = res && stage.equals(Stage.DONE);
				}

			}
			return res;
		} catch (Exception e) {
			throw new ESIndexFacadeException("failed checking Index recovery for project " + indexName, e);
		}
	}

	@Override
	public boolean destroyCorrelationMapping(String domainName, String hierarchyName) {
		return this.destroyMapping(domainName, hierarchyName);
	}

	@Override
	public boolean destroyDimensionMapping(String domainName, String dimensionName) {
		return this.destroyMapping(domainName, dimensionName);
	}

	private boolean destroyMapping(String projectName, String name) {
		DeleteMappingResponse resp = client.admin().indices().prepareDeleteMapping(projectName).setType(name).execute()
				.actionGet();
		return resp.isAcknowledged();
	}

	private boolean createMapping(String domainName, String mappingName, String idFieldName,
			HashMap<String, ESMapping> mapping) throws ESIndexFacadeException {
		try {

			logger.info("create mapping for " + mapping.toString());
			XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();

			mappingBuilder.startObject();
			mappingBuilder.startObject(mappingName);
			mappingBuilder.startObject("properties");
			//add sorting field 
			if (idFieldName !=null){
			ESMapping idMapping = mapping.get(idFieldName);
			if ( idMapping!= null && idMapping.getType().equals("string")){
				mappingBuilder.startObject(ESIndexFacadeUtilities.sortKey);
				mappingBuilder.field("index", "not_analyzed");
				mappingBuilder.field("type", idMapping.getType());
				mappingBuilder.field("doc_values", true);
				mappingBuilder.endObject();
			}
			}
			for (String k : mapping.keySet()) {
				ESMapping map = mapping.get(k);
				if (map.index == ESIndexMapping.BOTH) {

					mappingBuilder.startObject(k);
					mappingBuilder.field("type", map.getType());

					if (map.getType().equals("string")) {
						mappingBuilder.field("analyzer", "my_ngram_analyzer");

						mappingBuilder.startObject("fields");
						mappingBuilder.startObject(ESIndexFacadeUtilities.not_analyzed);
						mappingBuilder.field("index", "not_analyzed");
						mappingBuilder.field("type", map.getType());
						if ((idFieldName != null) && (k.equals(idFieldName))) {
							mappingBuilder.field("doc_values", true);
						}
						mappingBuilder.endObject();
						mappingBuilder.endObject();
					}
					mappingBuilder.endObject();

				} else if (map.index == ESIndexMapping.NOT_ANALYZED) {
					mappingBuilder.startObject(k);

					if (map.getType().equals("string")) {
						mappingBuilder.field("index", "not_analyzed");
					}
					mappingBuilder.field("type", map.getType());
					mappingBuilder.endObject();
				} else { // analyzed or no indication
					mappingBuilder.startObject(k);
					mappingBuilder.field("type", map.getType());
					if (map.getType().equals("string")) {
						mappingBuilder.field("analyzer", "my_ngram_analyzer");
						mappingBuilder.field("index", "analyzed");
					} else {
						if (map.getType().equals("date")) {
							mappingBuilder.field("format", "dateOptionalTime");
						}
					}
					mappingBuilder.endObject();
				}
			}
			mappingBuilder.endObject();
			mappingBuilder.endObject();
			mappingBuilder.endObject();

			logger.info("Mapping\n" + mappingBuilder.string());

			PutMappingResponse resp = client.admin().indices().preparePutMapping(domainName).setType(mappingName)
					.setSource(mappingBuilder).execute().actionGet();

			if (!resp.isAcknowledged()) {
				return false;
			}

			return true;
		} catch (MergeMappingException e) {
			logger.error("ES mapping failed: cannot merge: ", e);
			return false;
		} catch (Exception e) {
			logger.error("ES mapping failed: ", e);
			return false;
		}
	}



	public enum MappingState {
		EXISTSEQUAL, EXISTSDIFFERENT, DOESNOTEXIST, ERROR
	};

	@Override
	public MappingState computeCorrelationMappingState(String domainName, String hierarchyName,
			HashMap<String, ESMapping> mapping) {
		return computeMappingState(domainName, hierarchyName, null, mapping);
	}

	@Override
	public MappingState computeDimensionMappingState(String domainName, String dimensionName, String idFieldName,
			HashMap<String, ESMapping> mapping) {
		return computeMappingState(domainName, dimensionName, idFieldName, mapping);
	}

	@SuppressWarnings("unchecked")
	private MappingState computeMappingState(String projectName, String type, String idFieldName,
			HashMap<String, ESMapping> mapping) {

		Map<String, Object> existingMapping = getMapping(projectName, type);
		if (existingMapping == null) {
			return MappingState.DOESNOTEXIST;
		}

		LinkedHashMap<String, Object> properties = (LinkedHashMap<String, Object>) existingMapping.get("properties");
		logger.debug("current mappings : " + properties.toString());

		
		if(idFieldName !=null
			&& mapping.get(idFieldName) !=null
			&& mapping.get(idFieldName).getType().equals("string")
			&& mapping.keySet().size() +1 != properties.keySet().size() ) {
			logger.debug("a different set of properties");
			return MappingState.EXISTSDIFFERENT;
		}
		else
			{
			if (mapping.keySet().size() != properties.keySet().size()) {
				logger.debug("a different set of properties");
				return MappingState.EXISTSDIFFERENT;
			} 
		}
		
		
		
		if (idFieldName !=null){
			ESMapping idMapping= mapping.get(idFieldName);
			if (idMapping!=null && idMapping.getType().equals("string") ){
				Map<String, Object> property = (Map<String, Object>) properties.get(ESIndexFacadeUtilities.sortKey);
				if (property == null){
					logger.debug(" no mapping found ES side for sortKey");
					return MappingState.EXISTSDIFFERENT;
				}else{
					if ( !property.get("type").equals("string") 
							|| !"not_analyzed".equals(property.get("index"))
							|| property.get("doc_values")== null
							|| !property.get("doc_values").equals(true) )	{
							logger.debug("  wrong mapping for sortKey");
	
							return MappingState.EXISTSDIFFERENT;
		
						}
					}
				}		
			}
		

	
			for (String fieldName : mapping.keySet()) {

				ESMapping map = mapping.get(fieldName);
				if (map == null) {
					logger.debug("no ESmapping found for " + fieldName);
					return MappingState.EXISTSDIFFERENT;
				}
				if (map.index == ESIndexMapping.BOTH) {

					Map<String, Object> property = (Map<String, Object>) properties.get(fieldName);
					if (property == null) {
						logger.debug(" no mapping found ES side for " + fieldName);
						return MappingState.EXISTSDIFFERENT;
					}

					if (!map.getType().equals(property.get("type"))) {
						logger.debug(
								"type differs for " + fieldName + " " + map.getType() + ":" + property.get("type"));
						return MappingState.EXISTSDIFFERENT;
					}

					if (map.getType().equals(ESMapping.ESTypeMapping.STRING) && (property.get("analyzer") == null
							|| !"my_ngram_analyzer".equals(property.get("analyzer")))) {
						logger.debug("badly configured analyzer");
						return MappingState.EXISTSDIFFERENT;
					}

					if (map.getType().equals(ESMapping.ESTypeMapping.STRING)){
					Map<String, Object> fields = (Map<String, Object>) property.get("fields");

					if (fields == null || fields.keySet().size() != 1
							|| !fields.keySet().contains(ESIndexFacadeUtilities.not_analyzed)) {
						logger.debug("missing field");
						return MappingState.EXISTSDIFFERENT;
					}
					Map<String, Object> naField = (Map<String, Object>) fields.get(ESIndexFacadeUtilities.not_analyzed);

					if (!"not_analyzed".equals(naField.get("index")) || !map.getType().equals(naField.get("type"))) {
						logger.debug("wrong index for not_analyzed field");
						return MappingState.EXISTSDIFFERENT;
					}
					if ((idFieldName != null) && (fieldName.equals(idFieldName))
							&& (naField.get("doc_values") == null || !naField.get("doc_values").equals(true))) {
						logger.debug("wrong id field");
						return MappingState.EXISTSDIFFERENT;
					}
					}

				} else if (map.index == ESIndexMapping.NOT_ANALYZED) {

					Map<String, Object> property = (Map<String, Object>) properties.get(fieldName);
					if (property == null) {
						logger.debug("not analyzed - wrong name");
						return MappingState.EXISTSDIFFERENT;
					}

					if (!map.getType().equals(property.get("type"))) {
						logger.debug("not analyzed  type");
						return MappingState.EXISTSDIFFERENT;
					}

					if (map.getType().equals(ESTypeMapping.STRING) && !"not_analyzed".equals(property.get("index"))) {
						logger.debug("not analyzed  -  string + wrong index");
						return MappingState.EXISTSDIFFERENT;

					}
					if ((idFieldName != null) && (fieldName.equals(idFieldName))
							&& (property.get("doc_values") == null || !property.get("doc_values").equals(true))) {
						logger.debug("not analyzed  -  wrong id field");
						return MappingState.EXISTSDIFFERENT;
					}

				} else { // analyzed or no indication
					Map<String, Object> property = (Map<String, Object>) properties.get(fieldName);
					if (property == null) {
						return MappingState.EXISTSDIFFERENT;
					}

					if (!map.getType().equals(property.get("type"))) {
						return MappingState.EXISTSDIFFERENT;
					}

					if (map.getType().equals(ESTypeMapping.STRING)
							&& (!"analyzed".equals(property.get("index")) || property.get("analyzer") == null
									|| !"my_ngram_analyzer".equals(property.get("analyzer")))) {
						return MappingState.EXISTSDIFFERENT;
					}

					if (map.getType().equals(ESTypeMapping.DATE)
							&& !"dateOptionalTime".equals(property.get("format"))) {
						return MappingState.EXISTSDIFFERENT;
					}
				}
			}
		
		return MappingState.EXISTSEQUAL;
	}

	@Override
	public Map<String, Object> getMapping(String projectName, String type) {

		GetMappingsRequest mapReq = new GetMappingsRequest();
		mapReq.types(type);
		mapReq.indices(projectName);
		GetMappingsResponse resp = client.admin().indices().getMappings(mapReq).actionGet();

		ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = resp.getMappings();

		ImmutableOpenMap<String, MappingMetaData> map = mappings.get(projectName);
		if (map == null) {
			return null;
		} else {
			MappingMetaData md = map.get(type);
			if (md != null) {
				try {
					return md.sourceAsMap();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}
			} else {
				return null;
			}

		}
	};

	// one document per dimension member

	@Override
	public String addDimensionMember(String domainName, String dimensionName, String idName,
			HashMap<String, Object> attributes, HashMap<String, ESMapping> mappings) throws ESIndexFacadeException {

		try {
			IndexRequest indexReq = new IndexRequest().index(domainName).type(dimensionName)
					.id(attributes.get(idName).toString()).source(attributes);
			IndexResponse resp = client.index(indexReq).actionGet();
			return resp.getId();
		} catch (Exception e) {
			throw new ESIndexFacadeException("failed adding member for " + dimensionName, e);
		}
	}

	@Override
	public void updateDimensionMember(String domainName, String dimensionName, String idName,
			HashMap<String, Object> attributes, HashMap<String, ESMapping> mappings) throws ESIndexFacadeException {
		// TODO Auto-generated method stub

	}

	@Override
	public String addBatchDimensionMembers(String domainName, String dimensionName, String idName,
			ArrayList<HashMap<String, Object>> members, HashMap<String, ESMapping> mappings, boolean wait)
					throws ESIndexFacadeException {
		try {

			CountDownLatch finish = new CountDownLatch(1);
			BulkProcessor.Listener listener;
			if (wait) {
				listener = new BulkIndexingNotifyListener(finish);
			} else {
				listener = new SimpleIndexingListener();
			}

			BulkProcessor bulkProcessor = BulkProcessor.builder(client, listener).build();
			// }).setBulkActions(bulkSize).setConcurrentRequests(maxConcurrentBulk).build();

			for (HashMap<String, Object> attributes : members) {
				IndexRequest req = new IndexRequest().index(domainName).type(dimensionName)
						.id(attributes.get(idName).toString()).source(attributes);
				// System.out.println(req.toString());
				bulkProcessor.add(req);
			}
			bulkProcessor.close();
			if (wait) {
				BulkIndexingNotifyListener l = (BulkIndexingNotifyListener) listener;
				try {
					finish.await();
				} catch (InterruptedException e) {
					throw new ESIndexFacadeException(
							"Dimension Indexation ended with error " + domainName + "/" + dimensionName);
				}
				if (l.withError) {
					throw new ESIndexFacadeException(
							"Dimension Indexation ended with error " + domainName + "/" + dimensionName);
				} else {
					return l.lastId;
				}
			} else {
				return "";
			}

		} catch (Exception e) {
			throw new ESIndexFacadeException("failed adding members for " + domainName + "/" + dimensionName, e);
		}

	}

	// get whole index

	@Override
	public ArrayList<Map<String, Object>> getWholeIndex(String dimensionName) throws ESIndexFacadeException {
		// logger.info("Getting whole index");
		try {
			MatchAllQueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
			SearchRequestBuilder srb = client.prepareSearch(dimensionName).setQuery(queryBuilder)
					.addSort(SortBuilders.fieldSort("_uid"));
			srb.setSize(1024);

			SearchResponse resp = srb.execute().actionGet();

			return ESIndexFacadeUtilities.getSourceFromHits(resp.getHits().getHits());
		} catch (Exception e) {
			throw new ESIndexFacadeException("failed getting whole index for dimension " + dimensionName, e);
		}
	}

	// search within a dimension
	@Override
	public Map<String, Object> getDimensionValue(String domainName, String dimensionName, String valueID)
			throws ESIndexFacadeException {
		try {
			logger.debug("get dimension value  index " + dimensionName + " type " + dimensionName + " id " + valueID);
			GetRequest getReq = new GetRequest(domainName, dimensionName, valueID);
			GetResponse resp = client.get(getReq).actionGet();

			if (resp.isExists()) {
				Map<String, Object> fields = resp.getSourceAsMap();
				return fields;
			} else {
				logger.debug("entry " + valueID + ", type " + dimensionName + " does not exist in index " + domainName);
				return null;
			}
		} catch (Exception e) {
			throw new ESIndexFacadeException("failed getting values for " + domainName + "/" + dimensionName, e);
		}
	}

	@Override
	public ArrayList<Map<String, Object>> getNDimensionMembers(String domainName, String dimensionName,
			String sortingFieldName, int from, int nbRes, HashMap<String, ESMapping> mappings)
					throws ESIndexFacadeException {
		try {
			logger.debug(" Searching " + nbRes + "first results in " + domainName + "/" + dimensionName);

			QueryBuilder query = ESIndexFacadeUtilities.filterOnType(dimensionName);

			SearchRequestBuilder srb = client.prepareSearch(domainName).setTypes(dimensionName).setQuery(query);
			srb.setFrom(from);
			srb.setSize(nbRes);
			srb.addSort(SortBuilders.fieldSort(ESIndexFacadeUtilities.getSortingFieldName(sortingFieldName, mappings)));
		//	 logger.info(srb.toString());
			SearchResponse resp = srb.execute().actionGet();

			ArrayList<Map<String, Object>> res = new ArrayList<Map<String, Object>>();
			for (SearchHit hit : resp.getHits().getHits()) {
				Map<String, Object> source = hit.getSource();
				if (!source.isEmpty()) {
					res.add(hit.getSource());
				} else {
					res.add(Collections.singletonMap(DimensionStoreES.idName, (Object) hit.getId()));
				}
			}

			return res;
		} catch (ElasticsearchException e) {
			throw new ESIndexFacadeException(e);
		}
	}

	@Override
	public DimensionsSearchResult searchDimensionMembersByTokensAndLocalFilter(String domainName, String dimensionName,
			String[] tokens, int from, int nbResults, HashMap<String, ESMapping> mappings, String idFieldname)
					throws ESIndexFacadeException {
		boolean ok = false;
		int currentFrom = from;
		DimensionsSearchResult results = new DimensionsSearchResult();
		List<Map<String, Object>> res = new ArrayList<Map<String, Object>>();

		while (!ok) {

			// get results
			SearchResponse resp = this.partialSearchDimensionMembersByTokensAndLocalFilter(domainName, dimensionName,
					tokens, currentFrom, nbResults, mappings);
			List<Map<String, Object>> intermediateRes = ESIndexFacadeUtilities
					.getSourceFromHits(resp.getHits().getHits());

			int nbPartialRes = 0;
			// local filter
			for (Map<String, Object> row : intermediateRes) {
				boolean rowOk = true;
				// special case : numeric ID and only one token
				if (mappings.containsKey(idFieldname + "_raw" ) && (mappings.get(idFieldname + "_raw").type == ESTypeMapping.DOUBLE) && (tokens.length == 1)) {
					String val = row.get(idFieldname + "_raw").toString();
					rowOk = val.toLowerCase().equals(tokens[0].toLowerCase());
				} else {
					for (String token : tokens) {
						boolean okOneToken = false;
						for (String key : row.keySet()) {
							if (key.equals(ESIndexFacadeUtilities.sortKey))
								continue;
							if (mappings.get(key).type == ESTypeMapping.STRING) {
								String val = (String) row.get(key);
								if (val.toLowerCase().contains(token.toLowerCase())) {
									okOneToken = true;
									break;
								}
							}
						}
						if (!okOneToken) {
							rowOk = false;
							break;
						}
					}
				}
				if (rowOk) {
					res.add(row);
					nbPartialRes += 1;
				}
			}

			// enough results?

			if ((nbPartialRes == 0) || (intermediateRes.size() < nbResults)) {
				results.hits = res;
				results.hasMore = false;
				ok = true;
			} else {
				if (res.size() == nbResults) {
					ok = true;
					results.hits = res;
					results.hasMore = true;
					results.stoppedAt = currentFrom + nbResults;
				} else {
					currentFrom = currentFrom + nbResults;
				}
			}
		}
		return results;

	}

	private SearchResponse partialSearchDimensionMembersByTokensAndLocalFilter(String domainName, String dimensionName,
			String[] tokens, int from, int nbResults, HashMap<String, ESMapping> mappings)
					throws ESIndexFacadeException {

		BoolQueryBuilder andQuery = QueryBuilders.boolQuery();
		if (tokens.length == 1) {
			String filter = tokens[0];
			if (filter.length() == 1) {
				andQuery = ESIndexFacadeUtilities.matchOnSubstringAnyField(filter, mappings, true);
			} else {
				andQuery = ESIndexFacadeUtilities.matchOnSubstringAnyField(filter, mappings, false);
			}
		} else {
			for (String token : tokens) {
				BoolQueryBuilder oneTokenQuery = ESIndexFacadeUtilities.matchOnSubstringAnyField(token, mappings,
						false);
				andQuery.must(oneTokenQuery);
			}
		}

		// apply the search to the right index on the dimensionType
		SearchRequestBuilder srb = client.prepareSearch(domainName).setTypes(dimensionName).setQuery(andQuery);
		srb.setSize(nbResults);
		srb.setFrom(from);

		// srb.addSort(SortBuilders.fieldSort("_uid"));
		if (logger.isDebugEnabled()) {
			logger.debug((srb.toString()));
		}

		SearchResponse resp = srb.execute().actionGet();
		if (logger.isDebugEnabled()) {
			logger.debug((resp.toString()));
		}
		return resp;

	}

	// add correlations

	@Override
	public boolean addHierarchyCorrelationMapping(String domainName, String hierarchyName,
			HashMap<String, ESMapping> mappings) throws ESIndexFacadeException {
		// return this.testAndCreateMapping(projectName, hierarchyName, null,
		// mappings);
		return this.createMapping(domainName, hierarchyName, null, mappings);
	}

	@Override
	public String addHierarchyCorrelationsBatch(String domainName, String hierarchyName, ArrayList<String> types,
			Collection<List<DimensionMember>> ids, HashMap<String, ESMapping> mappings, boolean wait)
					throws ESIndexFacadeException {

		CountDownLatch finish = new CountDownLatch(1);

		BulkProcessor.Listener listener;
		if (wait) {
			listener = new BulkIndexingNotifyListener(finish);
		} else {
			listener = new SimpleIndexingListener();
		}

		BulkProcessor bulkProcessor = BulkProcessor.builder(client, listener).build();
		// }).setBulkActions(bulkSize).setConcurrentRequests(maxConcurrentBulk).build();

		for (List<DimensionMember> corr : ids) {
			if (types.size() != corr.size()) {
				continue;
			}
			HashMap<String, Object> source = ESIndexFacadeUtilities.buildSourceIDs(types, corr, mappings);

			IndexRequest req = new IndexRequest().index(domainName).type(hierarchyName).source(source);
			bulkProcessor.add(req);
		}
		bulkProcessor.close();
		if (wait) {
			BulkIndexingNotifyListener l = (BulkIndexingNotifyListener) listener;
			try {
				finish.await();
			} catch (InterruptedException e) {
				throw new ESIndexFacadeException("Correlation Indexation ended with error ");
			}
			if (l.withError) {
				throw new ESIndexFacadeException("Correlation Indexation ended with error ");
			} else {
				return l.lastId;
			}
		} else {
			return "";
		}
	};

	@Override
	public String addHierarchyCorrelation(String domainName, String hierarchyName, ArrayList<String> types,
			List<DimensionMember> corr, HashMap<String, ESMapping> mappings) throws ESIndexFacadeException {

		if (types.size() != corr.size()) {
			throw new ESIndexFacadeException(
					"parameters " + types.toString() + " and " + corr.toString() + " do not match");
		}
		HashMap<String, Object> source = ESIndexFacadeUtilities.buildSourceIDs(types, corr, mappings);
		IndexRequest req = new IndexRequest().index(domainName).type(hierarchyName).source(source);
		// logger.info(req.toString());
		IndexResponse resp = client.index(req).actionGet();
		return resp.getId();
	};

	// search through correlations

	@Override
	public ArrayList<ArrayList<String>> getCorrelations(String domainName, String hierarchyName,
			ArrayList<String> hierarchyType, int from, int nbResults) {

		TypeFilterBuilder typeFilter = FilterBuilders.typeFilter(hierarchyName);

		ConstantScoreQueryBuilder csqb = QueryBuilders.constantScoreQuery(typeFilter);

		SearchRequestBuilder srb = client.prepareSearch(domainName).setTypes(hierarchyName).setQuery(csqb);
		srb.setSize(nbResults);
		srb.setFrom(from);
		srb.addSort(SortBuilders.fieldSort("_uid"));
		SearchResponse resp = srb.execute().actionGet();

		ArrayList<ArrayList<String>> res = new ArrayList<ArrayList<String>>();
		for (SearchHit hit : resp.getHits().getHits()) {
			ArrayList<String> record = new ArrayList<String>();

			for (String k : hit.getSource().keySet()) {
				record.add((String) hit.getSource().get(k));
			}
			res.add(record);
		}

		return res;

	}

	@Override
	public HierarchiesSearchResult filterHierarchyByMemberValues(String domainName, String hierarchyName,
			String resultType, HashMap<String, ArrayList<String>> filterVal, int from, int nbResults,
			HashMap<String, ESMapping> mappings) throws ESIndexFacadeException {

		return this.getNresults(domainName, hierarchyName, resultType, filterVal, null, from, nbResults, mappings);
	}

	private SearchResponse partialFilterHierarchyByFirstChar(String domainName, String hierarchyName, String resultType,
			HashMap<String, ArrayList<String>> filterVal, String prefix, int from, int nbResults,
			HashMap<String, ESMapping> mappings) {

		TypeFilterBuilder typeFilter = FilterBuilders.typeFilter(hierarchyName);

		BoolQueryBuilder andBoolQuery = QueryBuilders.boolQuery();
		for (String type : filterVal.keySet()) {
			if (filterVal.containsKey(type)) {
				BoolQueryBuilder orBoolQuery = QueryBuilders.boolQuery();
				orBoolQuery.minimumNumberShouldMatch(1);
				for (String val : filterVal.get(type)) {
					QueryBuilder q = ESIndexFacadeUtilities.filterOnField(type, val, mappings);
					orBoolQuery.should(q);
				}
				andBoolQuery.must(orBoolQuery);
			}
		}

		QueryBuilder substringFilter = ESIndexFacadeUtilities.filterOnFirstCharOneField(prefix, resultType, mappings);

		andBoolQuery.must(substringFilter);

		SearchRequestBuilder srb = client.prepareSearch(domainName).setTypes(hierarchyName)
				.setQuery(QueryBuilders.filteredQuery(andBoolQuery, typeFilter));
		srb.setSize(nbResults);
		srb.setFrom(from);
		logger.info(srb.toString());

		SearchResponse resp = srb.execute().actionGet();
		return resp;

	}

	private SearchResponse partialFilterHierarchyByMemberValues(String domainName, String hierarchyName,
			String resultType, HashMap<String, ArrayList<String>> filterVal, int from, int nbResults,
			HashMap<String, ESMapping> mappings) {
		//

		TypeFilterBuilder typeFilter = FilterBuilders.typeFilter(hierarchyName);

		BoolQueryBuilder andBoolQuery = QueryBuilders.boolQuery();
		for (String type : filterVal.keySet()) {
			if (filterVal.containsKey(type)) {
				BoolQueryBuilder orBoolQuery = QueryBuilders.boolQuery();
				orBoolQuery.minimumNumberShouldMatch(1);
				for (String val : filterVal.get(type)) {
					QueryBuilder q = ESIndexFacadeUtilities.filterOnField(type, val, mappings);
					orBoolQuery.should(q);
				}
				andBoolQuery.must(orBoolQuery);
			}
		}

		SearchRequestBuilder srb = client.prepareSearch(domainName).setTypes(hierarchyName)
				.setQuery(QueryBuilders.filteredQuery(andBoolQuery, typeFilter));
		srb.setSize(nbResults);
		srb.setFrom(from);

		String sortingFieldName = ESIndexFacadeUtilities.getSortingFieldName(resultType, mappings);

		srb.addSort(SortBuilders.fieldSort(sortingFieldName));
		SearchResponse resp = srb.execute().actionGet();
		return resp;
	}

	@Override
	public HierarchiesSearchResult filterHierarchyByMemberValuesAndSubstring(String domainName, String hierarchyName,
			String resultType, HashMap<String, ArrayList<String>> filterVal, String substring, int from, int nbResults,
			HashMap<String, ESMapping> mappings) throws ESIndexFacadeException {

		return this.getNresults(domainName, hierarchyName, resultType, filterVal, substring, from, nbResults, mappings);
	}

	private SearchResponse partialFilterHierarchyByMemberValuesAndSubstring(String domainName, String hierarchyName,
			String resultType, HashMap<String, ArrayList<String>> filterVal, String substring, int from, int nbResults,
			HashMap<String, ESMapping> mappings) {
		//

		TypeFilterBuilder typeFilter = FilterBuilders.typeFilter(hierarchyName);

		BoolQueryBuilder andBoolQuery = QueryBuilders.boolQuery();
		for (String type : filterVal.keySet()) {
			if (filterVal.containsKey(type)) {
				BoolQueryBuilder orBoolQuery = QueryBuilders.boolQuery();
				orBoolQuery.minimumNumberShouldMatch(1);
				for (String val : filterVal.get(type)) {
					QueryBuilder q = ESIndexFacadeUtilities.filterOnField(type, val, mappings);
					orBoolQuery.should(q);
				}
				andBoolQuery.must(orBoolQuery);
			}
		}

		/*
		 * QueryBuilder substringFilter = ESIndexFacadeUtilities
		 * .queryStringOnSubstringOneField(substring, resultType, mappings);
		 */
		QueryBuilder substringFilter = ESIndexFacadeUtilities.matchOnSubstringOneField(substring, resultType, mappings);

		andBoolQuery.must(substringFilter);

		SearchRequestBuilder srb = client.prepareSearch(domainName).setTypes(hierarchyName)
				.setQuery(QueryBuilders.filteredQuery(andBoolQuery, typeFilter));
		srb.setSize(nbResults);
		srb.setFrom(from);
		// logger.info(srb.toString());

		SearchResponse resp = srb.execute().actionGet();
		return resp;
	}

	private HierarchiesSearchResult getNresults(String domainName, String hierarchyName, String resultType,
			HashMap<String, ArrayList<String>> filterVal, String substring, int from, int nbResults,
			HashMap<String, ESMapping> mappings) throws ESIndexFacadeException {
		try {
			int totalResults = 0;
			long totalHits = -1;
			LinkedHashSet<String> results = new LinkedHashSet<>();
			HierarchiesSearchResult res = new HierarchiesSearchResult();

			boolean ok = false;
			int currentFrom = from;

			while (!ok) {
				SearchResponse resp;
				if (substring == null) {
					resp = this.partialFilterHierarchyByMemberValues(domainName, hierarchyName, resultType, filterVal,
							currentFrom, nbResults, mappings);

				} else {
					if (substring.length() == 1) {
						resp = this.partialFilterHierarchyByFirstChar(domainName, hierarchyName, resultType, filterVal,
								substring, currentFrom, nbResults, mappings);
					} else {
						resp = this.partialFilterHierarchyByMemberValuesAndSubstring(domainName, hierarchyName,
								resultType, filterVal, substring, currentFrom, nbResults, mappings);
					}
				}

				// logger.info(resp.toString());

				if (totalHits == -1) {
					totalHits = resp.getHits().getTotalHits();
				}

				totalResults += resp.getHits().getHits().length;

				int incr = 0;
				for (SearchHit hit : resp.getHits().getHits()) {
					Object value = hit.getSource().get(resultType);
					if (value != null) {
						if ((substring != null) && !(value.toString().toLowerCase().contains(substring))) {
							continue;
						}
						if (results.add(value.toString())) {
							if (logger.isDebugEnabled()) {
								logger.debug(("score " + value.toString() + " " + hit.getScore()));
							}
							incr++;
						}
					}
				}

				// logger.info("results size " + results.size() +
				// " total results " +totalResults + " total hits " + totalHits
				// );
				if (results.size() >= nbResults || incr == 0) {
					if (totalResults >= totalHits) {
						res.hasMore = false;
					}
					res.hits = results;
					res.stoppedAt = currentFrom + nbResults;
					ok = true;
				} else {
					if (totalResults >= totalHits) {
						res.hasMore = false;
						res.hits = results;
						ok = true;
					} else {
						currentFrom += nbResults;
					}
				}
			}
			if (logger.isDebugEnabled()) {
				logger.debug((" get N results " + res.hits.toString()));
			}
			return res;
		} catch (ElasticsearchException e) {
			throw new ESIndexFacadeException(e);
		}
	}

	// count
	@Override
	public long getCountType(String domainName, String dimensionName) {

		TypeFilterBuilder tf = FilterBuilders.typeFilter(dimensionName);
		CountResponse response = client.prepareCount(domainName).setQuery(QueryBuilders.constantScoreQuery(tf))
				.execute().actionGet();

		return response.getCount();
	}

	// range
	@Override
	public List<Map<String, Object>> searchWithinRange(String domainName, String dimensionName, String sortingFieldName,
			String lowerLimitFieldName, String upperLimitFieldName, Object lowerLimit, Object upperLimit, int from,
			int nbResults, HashMap<String, ESMapping> mappings) {

		BoolQueryBuilder orBoolQuery = QueryBuilders.boolQuery();
		orBoolQuery.minimumNumberShouldMatch(1);
		orBoolQuery.should(ESIndexFacadeUtilities.withinRange(upperLimitFieldName, lowerLimit, upperLimit));
		orBoolQuery.should(ESIndexFacadeUtilities.withinRange(lowerLimitFieldName, lowerLimit, upperLimit));

		SearchRequestBuilder srb = client.prepareSearch(domainName).setTypes(dimensionName).setQuery(orBoolQuery);
		srb.setSize(nbResults);
		srb.setFrom(from);
		srb.addSort(SortBuilders.fieldSort(ESIndexFacadeUtilities.getSortingFieldName(sortingFieldName, mappings)));

		SearchResponse resp = srb.execute().actionGet();

		return ESIndexFacadeUtilities.getSourceFromHits(resp.getHits().getHits());
	}

	@Override
	public List<Map<String, Object>> searchOnThreshold(String domainName, String dimensionName, String sortingFieldName,
			String fieldname, Object threshold, ESIndexFacadeUtilities.InequalityRelation binRel, int from,
			int nbResults, HashMap<String, ESMapping> mappings) {

		QueryBuilder query = ESIndexFacadeUtilities.compareToThreshold(fieldname, threshold, binRel);

		SearchRequestBuilder srb = client.prepareSearch(domainName).setTypes(dimensionName).setQuery(query);
		srb.setSize(nbResults);
		srb.setFrom(from);

		srb.addSort(SortBuilders.fieldSort(ESIndexFacadeUtilities.getSortingFieldName(sortingFieldName, mappings)));

		SearchResponse resp = srb.execute().actionGet();

		return ESIndexFacadeUtilities.getSourceFromHits(resp.getHits().getHits());
	}

}
