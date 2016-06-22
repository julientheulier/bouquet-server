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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.PrefixFilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.index.query.TypeFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.ESIndexFacade.ESMapping.ESIndexMapping;
import com.squid.kraken.v4.ESIndexFacade.ESMapping.ESTypeMapping;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;

public class ESIndexFacadeUtilities {

	
	static final Logger logger = LoggerFactory.getLogger(ESIndexFacadeUtilities.class);

	public static final String not_analyzed = "not_analyzed";
	public static final String not_analyzedSuffix = "."+ not_analyzed;

	public static final String sortKey = "sortKey";
	
	
	
//	private static final String[] specialChar = {"+", "-", "=", "&&", "||", ">", "<", "!", 
	//	"(",  ")", "{", "}", "[", "]", "^", "\"",  "~",  "*", "?", ":", "\\", "/" };
	// char \ has to be processed first
	private static final String[] specialChar = {"+", "-", "=", "&&", "||", ">", "<", "!", 
		"(",  ")", "{", "}", "[", "]", "^", "\"",  "~",  "*", "?", ":", "/" };
	
	public enum InequalityRelation{
		LESS, LESSEQUAL, GREATER, GREATEREQUAL
	}

	public static HashMap<String, Object> buildSourceIDs(List<String> types,
			List<DimensionMember> ids, HashMap<String, ESMapping> mappings) {
		HashMap<String, Object> source = new HashMap<String, Object>();

		for (int i = 0; i < types.size(); i++) {
		    DimensionMember id = ids.get(i);
			if (id!=null){ 
				source.put(types.get(i), id.getID());
			}
		}
		return source; 
	}

	
	public static String escapeQueryStringQuery(String toEscape){
		String res =toEscape ; 
		res= res.replace("\\", "\\\\");
		for (String s : specialChar){
			res = res.replace(s, "\\"+s);
		}
		return res;
	}
	
	public static IdsQueryBuilder getIdsQuery(String  dimensionName, Collection<String> ids){
		IdsQueryBuilder idsQuery = QueryBuilders.idsQuery(dimensionName);
		for (String id : ids) {
			idsQuery.addIds(id);
		}
		return idsQuery;
	}
	
	public static BoolQueryBuilder queryStringOnSubstringAnyField( String substring,HashMap<String, ESMapping> mappings)
	{
		BoolQueryBuilder orQuery  = QueryBuilders.boolQuery();
		orQuery.minimumNumberShouldMatch(1);
		for(String fieldname : mappings.keySet()){
			QueryBuilder query = queryStringOnSubstringOneField(substring, fieldname, mappings);
			if (query != null)
			orQuery.should(query);
		}
		return orQuery ;
	}
	
	public static QueryBuilder queryStringOnSubstringOneField( String substring, String fieldname, HashMap<String, ESMapping> mappings)
	{
		ESMapping map = mappings.get(fieldname) ;
		if (map.type == ESTypeMapping.STRING){
				String filterVal = escapeQueryStringQuery(substring);
			 	QueryStringQueryBuilder a = QueryBuilders.queryString("*"+filterVal+"*")  ;
			 	a.analyzeWildcard(true);
			 	a.field(fieldname);// do not use not_analyzedSuffix because it is case-sensitive
			 	a.allowLeadingWildcard(true);
			return a;			
		}else
			return null;
	}
	
	public static QueryBuilder filterOnFirstCharOneField( String prefix ,String fieldname,  HashMap<String, ESMapping> mappings){
		
		ESMapping map = mappings.get(fieldname) ;
		if (map.type == ESTypeMapping.STRING){		
			PrefixFilterBuilder prefixFilter = FilterBuilders.prefixFilter(fieldname +not_analyzedSuffix, prefix) ;
			QueryBuilder q = QueryBuilders.constantScoreQuery(prefixFilter);			
			return q;			
		}else
			return null;		
	}
	
	
	public static BoolQueryBuilder matchOnSubstringAnyField( String substring,HashMap<String, ESMapping> mappings, boolean firstChar)
	{
		BoolQueryBuilder orQuery  = QueryBuilders.boolQuery();
		orQuery.minimumNumberShouldMatch(1);
		for(String fieldname : mappings.keySet()){
			QueryBuilder query ;
			if (firstChar){
				if (mappings.get(fieldname).type == ESTypeMapping.STRING){	
					BoolQueryBuilder firstCharQuery = QueryBuilders.boolQuery();
					firstCharQuery.minimumNumberShouldMatch(1);
					firstCharQuery.should(filterOnFirstCharOneField(substring.toLowerCase(), fieldname, mappings)); 
					firstCharQuery.should(filterOnFirstCharOneField(substring.toUpperCase(), fieldname, mappings)); 
					query = firstCharQuery; 
				}else{
					query = null;
				}
			}
			else{
				query= matchOnSubstringOneField(substring, fieldname, mappings);
			}
			if (query != null)
			orQuery.should(query);
		}
		return orQuery ;
	}

	public static QueryBuilder matchOnSubstringOneField(String substring, String fieldname, HashMap<String, ESMapping> mappings){
		ESMapping map = mappings.get(fieldname) ;
		if (map.type == ESTypeMapping.STRING){
			 	QueryBuilder a  = QueryBuilders.matchQuery(fieldname , substring); 
			return a;			
		}else
			return null;
		
	}
	
	
	public static BoolQueryBuilder combineMatchQuerySubstringAnyField(String substring,HashMap<String, ESMapping> mappings){
		
		BoolQueryBuilder orQuery  = QueryBuilders.boolQuery();
		orQuery.minimumNumberShouldMatch(1);
		for(String fieldname : mappings.keySet()){
			QueryBuilder queryMatch = matchOnSubstringOneField(substring, fieldname, mappings);
			QueryBuilder queryString = queryStringOnSubstringOneField(substring, fieldname, mappings);

			if ((queryMatch != null) && (queryString!=null)){
				BoolQueryBuilder andQuery = QueryBuilders.boolQuery();
				andQuery.must(queryMatch);
				andQuery.must(queryString);
				orQuery.should(andQuery);
			}
		}
		return orQuery ;
		
	}
	
	public static String prettyPrintHit(SearchHit[] hits) {

		String res = "";
		for (SearchHit hit : hits) {
			Map<String, Object> src = hit.getSource();
			for (String k : src.keySet()) {
				res += src.get(k).toString() + "\t";
			}
			res += "\n";
		}
		return res;
	}


	public static QueryBuilder filterOnField(String filteredType, String val, HashMap<String, ESMapping> mappings ){
		
		QueryBuilder query;
		if ((mappings!=null) && (mappings.containsKey(filteredType))){
			ESMapping map = mappings.get(filteredType) ;
			
			if (map.index== ESIndexMapping.BOTH || map.index== ESIndexMapping.NOT_ANALYZED){
				 TermFilterBuilder tf = FilterBuilders.termFilter(getSortingFieldName(filteredType, mappings), val);
				 query=  QueryBuilders.filteredQuery( QueryBuilders.matchAllQuery(), tf);				
			}else{
				query = QueryBuilders.matchQuery(filteredType, val);
			}	
		}else{
			query = QueryBuilders.matchQuery(filteredType, val);
		}
		
		return query;
	}

	public static QueryBuilder filterOnType(String dimensionName){
		TypeFilterBuilder typeFilter = FilterBuilders.typeFilter(dimensionName);
		QueryBuilder csqb = QueryBuilders.filteredQuery( QueryBuilders.matchAllQuery(), typeFilter);
		return csqb;
	}

	
	public static ArrayList<Map<String, Object>> getSourceFromHits(SearchHit[] hits){
		ArrayList<Map<String, Object>> res = new ArrayList<Map<String, Object>>();
		for (SearchHit hit : hits) {
			res.add(hit.getSource());
			if(logger.isDebugEnabled()){logger.debug((hit.getSourceAsString() + " " + hit.getScore()));}
		}
		return res;	
	}
	
	public static String getSortingFieldName(String resultType, HashMap<String, ESMapping> mappings){
		if ((mappings!=null) && (mappings.containsKey(resultType))){
			ESMapping map = mappings.get(resultType) ;
			if (map.index.equals(ESIndexMapping.BOTH) && map.type.equals(ESTypeMapping.STRING)){
				return ESIndexFacadeUtilities.sortKey;
			}else{
				return resultType;
			}	
		}else{
			return resultType;
		}
	}
	
	
	public static QueryBuilder compareToThreshold(String fieldName, Object threshold, InequalityRelation binRel ){
		RangeFilterBuilder  rfb = FilterBuilders.rangeFilter(fieldName);
		switch(binRel){
		case LESS :
			rfb.lt(threshold);
			break;
		case LESSEQUAL :
			rfb.lte(threshold);
			break;
		case GREATER:
			rfb.gt(threshold);
			break;		
		case GREATEREQUAL :
			rfb.gte(threshold);
			break;
		}		
		return  QueryBuilders.constantScoreQuery(rfb);
	}
	
	public static QueryBuilder withinRange(String fieldName, Object lowerLimit, Object upperLimit){	
		RangeFilterBuilder  rfb = FilterBuilders.rangeFilter(fieldName);
		rfb.gte(lowerLimit);
		rfb.lte(upperLimit);
		return QueryBuilders.constantScoreQuery(rfb);		
	}
}
