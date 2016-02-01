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

import com.squid.kraken.v4.ESIndexFacade.ESIndexFacade.MappingState;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;

public interface IESIndexFacade {

	// create/remove index
/*	public void addProject(String projectName) throws ESIndexFacadeException,
			InterruptedException; 

	public void removeProject(String projectName) throws ESIndexFacadeException;
*/
	public boolean dimensionMappingInES(String domainName, String dimensionName) throws ESIndexFacadeException;

	public boolean isRecoveryDone(String domainName) throws ESIndexFacadeException;

	// mappings
	public boolean addDimensionMapping(String domainName, String dimensionName,String idFieldName,
			HashMap<String, ESMapping> mapping) throws ESIndexFacadeException;

	public void updateDimension(String domainName, String dimensionName,
			HashMap<String, ESMapping> mapping) throws ESIndexFacadeException;

	public boolean domainInES(String domainName) throws ESIndexFacadeException;

	/**
	 * 
	 * @param projectName
	 * @param type
	 * @return
	 */
	public Map<String,Object> getMapping(String projectName, String type);

	// populate dimensions

	public String addDimensionMember(String domainName, String dimensionName,
			String id, HashMap<String, Object> attributes,
			HashMap<String, ESMapping> mappings) throws ESIndexFacadeException;

	public void updateDimensionMember( String domainName, String dimensionName,
			String id, HashMap<String, Object> attributes,
			HashMap<String, ESMapping> mappings) throws ESIndexFacadeException;


	public String addBatchDimensionMembers(String domainName, String dimensionName, String id,
			ArrayList<HashMap<String, Object>> members,
			HashMap<String, ESMapping> mappings, boolean wait) throws ESIndexFacadeException;

	// search dimensions

	public List<Map<String, Object>> getWholeIndex(String domainName) throws ESIndexFacadeException;
/*
	public List<Map<String, Object>> searchNFirstDimensionMembersByAttributes(
			String projectName, String dimensionName,
			HashMap<String, Object> attributes, int nbResults)
			throws ESIndexFacadeException;
*/
	// public ArrayList<Map<String, Object>> getAllDimensionMembers(String
	// projectName, String dimensionName) throws DimensionIndexException;
	public List<Map<String, Object>> getNDimensionMembers(
			String domainName,
			String dimensionName,String sortingFieldName, int from, int nbRes,HashMap<String, ESMapping> mappings)
			throws ESIndexFacadeException;

	public Map<String, Object> getDimensionValue(
			String domainName,
			String dimensionName, String valueID) throws ESIndexFacadeException;

	public DimensionsSearchResult searchDimensionMembersByTokensAndLocalFilter(
			String domainName,
			String dimensionName, String[] tokens,
			int from, int nbResults, HashMap<String, ESMapping> mappings) throws ESIndexFacadeException ;

	// populate correlations

	public boolean addHierarchyCorrelationMapping(String domainName, String hierarchyName,
			HashMap<String, ESMapping> mappings)
			throws ESIndexFacadeException;


	public String addHierarchyCorrelationsBatch(String domainName, String hierarchyName,
			ArrayList<String> types, Collection<List<DimensionMember>> ids,
			HashMap<String, ESMapping> mappings, boolean wait) throws ESIndexFacadeException;

	public String addHierarchyCorrelation(String domainName, String hierarchyName, ArrayList<String> types,
			List<DimensionMember> corr, HashMap<String, ESMapping> mappings)
			throws ESIndexFacadeException;

	// search correlations

	public HierarchiesSearchResult filterHierarchyByMemberValues(String domainName, String hierarchyName,
			String resultType, HashMap<String, ArrayList<String>> filterVal, int from,
			int nbResults, HashMap<String, ESMapping> mappings) throws ESIndexFacadeException;

	public HierarchiesSearchResult filterHierarchyByMemberValuesAndSubstring(String domainName, String hierarchyName,
			String resultType,
			HashMap<String, ArrayList<String>> filterVal, String substring,
			int from, int nbResults, HashMap<String, ESMapping> mappings) throws ESIndexFacadeException;

	public ArrayList<ArrayList<String>> getCorrelations(String domainName, String hierarchyName,
			 ArrayList<String> hierarchyType, int from,
			int nbResults);
	
	

	// search within range
	
	public List<Map<String, Object>> searchWithinRange(String domainName, String dimensionName,
			String sortingFieldName,  String lowerLimitFieldName,
			String upperLimitFieldName, Object lowerLimit, Object upperLimit,
			int from, int nbResults, HashMap<String, ESMapping> mappings);

	public List<Map<String, Object>> searchOnThreshold(String domainName, String dimensionName, 
			String sortingFieldName,  String fieldname, Object threshold,
			ESIndexFacadeUtilities.InequalityRelation binRel, int from,
			int nbResults, HashMap<String, ESMapping> mappings);

	// count

	public long getCountType(String domainName, String dimensionName);


	void addDomain(String domainName) throws ESIndexFacadeException;

	

	void removeDomain(String domainName) throws ESIndexFacadeException;

	boolean destroyDimensionMapping(String domainName, String dimensionName);

	boolean destroyCorrelationMapping(String domainName, String hierarchyName);

	MappingState computeCorrelationMappingState(String domainName,
			String hierarchyName, HashMap<String, ESMapping> mapping);

	MappingState computeDimensionMappingState(String domainName,
			String dimensionName, String idFieldName,
			HashMap<String, ESMapping> mapping);

}
