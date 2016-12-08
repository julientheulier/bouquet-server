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
package com.squid.kraken.v4.core.analysis.engine.index;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.ESIndexFacade.ESIndexFacade;
import com.squid.kraken.v4.ESIndexFacade.ESIndexFacade.MappingState;
import com.squid.kraken.v4.ESIndexFacade.ESIndexFacadeException;
import com.squid.kraken.v4.ESIndexFacade.ESIndexFacadeUtilities;
import com.squid.kraken.v4.ESIndexFacade.ESMapping;
import com.squid.kraken.v4.ESIndexFacade.ESMapping.ESIndexMapping;
import com.squid.kraken.v4.ESIndexFacade.ESMapping.ESTypeMapping;
import com.squid.kraken.v4.ESIndexFacade.HierarchiesSearchResult;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex.Status;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.model.IntervalleObject;
import com.squid.kraken.v4.model.Attribute;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Dimension.Type;

public class DimensionStoreES extends DimensionStoreAbstract {

	private static final Logger logger = LoggerFactory
			.getLogger(DimensionStoreES.class);

	public static final String idName = "ID";

	public static final String ESIndexPrefix = "ESI/";
	public static final String ESHierarchyPrefix = "ESH/";

	private ESIndexFacade master;

	// the ES index name
	private String indexName = null;

	private String query = null;

	// the field name for the dimension
	private String dimensionFieldName = null;
	// the type name for the main dimension
	private String dimensionTypeName = null;
	// the type name for the hierarchy correlation
	private String hierarchyTypeName = null;

	private boolean mappingInitialized;
	protected boolean correlationMappingInitialized ;

	/*
	 * private String dimensionGenKey = null; private String correlationGenKey =
	 * null;
	 */
	// the hierarchy root index - used to resolve hierarchy filters
	private DimensionStoreES rootStore = null;

	private AtomicInteger size = new AtomicInteger(0);

	private boolean cached = false;

	private HashMap<String, ESMapping> mapping = null;
	private HashMap<String, ESMapping> mappingCorrelations = null;

	private String idName_mapping = idName;// this is the actual name we will
											// use to lookup the ID

	public DimensionStoreES(ESIndexFacade master, String indexES,
			DimensionIndex index) throws ESIndexFacadeException {
		super(index);
		this.master = master;
		this.indexName = indexES;
		this.rootStore = getStore(index.getRoot());
		//
		this.mappingInitialized = false;
		this.correlationMappingInitialized = false; 
	}

	/**
	 * setup ES type
	 * 
	 * @param index
	 * @param indexES
	 * @throws ESIndexFacadeException
	 */
	@Override
	public void setup(DimensionIndex index, String query)
			throws ESIndexFacadeException {
		this.query = query;
		initReferences(index);
		if (index.getStatus() == Status.DONE) {
			// if index has status DONE, check if we can get the type in ES
			this.cached = master.dimensionMappingInES(indexName,
					dimensionTypeName);
			logger.debug("setup - " +  index.getDimensionName()+" DONE");
			if (!this.cached) {
				// clear the status
				logger.debug("setup " +  index.getDimensionName()+" not in cache");
				index.setStale();
			}
		} else {
			if (index.getStatus() == Status.ERROR) {
				logger.debug("setup " +  index.getDimensionName()+" ERROR - set to STALE");
				index.setStale();
			}
		}
		this.mapping = createMapping(index);
		restoreDimensionMapping();
		this.mappingInitialized = true;
	}

	public String getDimensionFieldName() {
		return dimensionFieldName;
	}

	private void refreshDimensionTypeName() {
		this.dimensionTypeName = ESIndexPrefix + dimensionFieldName + "-"
				+ DigestUtils.sha256Hex(this.query);
	}

	private void refreshHierarchyTypeName() {
		hierarchyTypeName = ESHierarchyPrefix + dimensionFieldName + "-"
				+ DigestUtils.sha256Hex(this.query);
	}

	private void initReferences(DimensionIndex index) {
		this.dimensionFieldName = getFieldName(this.getDimensionIndex()
				.getDimension());
		refreshDimensionTypeName();
	}

	protected void restoreDimensionMapping() {

		logger.info("restoring dimension mapping "
				+ this.getDimensionIndex().getDimensionName());
		MappingState state = this.master.computeDimensionMappingState(
				indexName, dimensionTypeName, this.idName_mapping, mapping);

		if (state == MappingState.ERROR) {
			logger.info("Could not create a mapping for type "
					+ dimensionTypeName);
			this.getDimensionIndex().setPermanentError(
					"could not create mappings");
			return;
		}

		if (state == MappingState.EXISTSEQUAL) {
			logger.info("Mapping for type  " + dimensionTypeName
					+ "  already exists");
			Status status = this.getDimensionIndex().getStatus();
			if (status == DimensionIndex.Status.DONE) {
				logger.info("index " + this.getDimensionIndex().toString()
						+ ": restoring DimensionStore from ES cache");
			} else {
				logger.info("index " + this.getDimensionIndex().toString()
						+ ": not in ES cache");
				this.cached = false;
			}
		}
		if (state == MappingState.EXISTSDIFFERENT) {
			logger.info("A different mapping exists for type  "
					+ dimensionTypeName,
					" attempt to destroy it a create a new one");
			logger.info(mapping.toString());
			this.cached = false;
			this.getDimensionIndex().setStale();
			boolean delRes = this.master.destroyDimensionMapping(indexName,
					dimensionTypeName);
			if (!delRes) {
				logger.info("failed creating mapping for type "
						+ dimensionTypeName);
				this.getDimensionIndex().setPermanentError(
						"could not create mappings");
			} else {
				// reset references
				// RedisCacheManager.getInstance().refresh(ESIndexPrefix +
				// dimensionFieldName);// refresh the index
				initReferences(this.getDimensionIndex());
				try {
					if (!this.master.addDimensionMapping(indexName,
							dimensionTypeName, this.idName_mapping, mapping)) {
						logger.info("failed creating mapping for type "
								+ dimensionTypeName);
						this.getDimensionIndex().setPermanentError(
								"could not create mappings");
					}
				} catch (ESIndexFacadeException e) {
					logger.info("failed creating mapping for type "
							+ dimensionTypeName);
					this.getDimensionIndex().setPermanentError(
							"could not create mappings");

				}
			}
		}
		if (state == MappingState.DOESNOTEXIST) {
			logger.info("index " + this.getDimensionIndex().toString()
					+ ": new");
			this.getDimensionIndex().setStale();
			this.cached = false;
			try {
				if (!this.master.addDimensionMapping(indexName,
						dimensionTypeName, this.idName_mapping, mapping)) {
					logger.info("failed creating mapping for type "
							+ dimensionTypeName);
					this.getDimensionIndex().setPermanentError(
							"could not create mappings");
				}
			} catch (ESIndexFacadeException e) {
				logger.info("failed creating mapping for type "
						+ dimensionTypeName);
				this.getDimensionIndex().setPermanentError(
						"could not create mappings");
			}
		}
	}

	/**
	 * return the fieldName for the given Dimension
	 * 
	 * @param dimension
	 * @return
	 */
	private String getFieldName(Dimension dimension) {
		return dimension.getId().toUUID();
	}

	private DimensionStoreES getStore(DimensionIndex index) {
		if (index == getDimensionIndex()) {
			return this;
		} else {
			IDimensionStore store = index.getStore();
			if (store instanceof DimensionStoreES) {
				return (DimensionStoreES) store;
			} else {
				return null;
			}
		}
	}
	

	/**
	 * create the mapping for the dimension type - note that the mapping is not
	 * written to ES
	 * 
	 * @param index
	 * @return
	 */
	private HashMap<String, ESMapping> createMapping(DimensionIndex index) {
		HashMap<String, ESMapping> mapping = new HashMap<>();
		IDomain idDomainType = this.getAxisDomain(index);
		ESTypeMapping idType = computeIDTypeMapping(index, idDomainType);
//		this.idName_mapping = idName + "_" + idType.toString();
		this.idName_mapping = idName ;
		
		if (idType.equals(ESTypeMapping.STRING)){	
			mapping.put(idName_mapping, new ESMapping(idName_mapping,
				ESIndexMapping.BOTH, idType));// ESTypeMapping.STRING));
		}else{
			//we store a version with the original type and an indexable version
			mapping.put(idName_mapping+ESIndexFacadeUtilities.rawSuffix, new ESMapping(idName_mapping+ESIndexFacadeUtilities.rawSuffix,
					ESIndexMapping.NOT_ANALYZED, idType ));
			mapping.put(idName_mapping, new ESMapping(idName_mapping,
					ESIndexMapping.BOTH, ESTypeMapping.STRING ));
			
		}
		
		if (getAttributeCount() > 0) {
			for (Attribute attr : getAttributes()) {
				try {
					ExpressionAST attribute = index
							.getAxis()
							.getParent()
							.getUniverse()
							.getParser()
							.parse(index.getAxis().getParent().getDomain(),
									attr);
					ESTypeMapping attrType = computeTypeMapping(getAttributeDomain(attribute));
					if (attrType.equals((ESTypeMapping.STRING))) {
						mapping.put(attr.getId().getAttributeId(),
								new ESMapping(attr.getId().getAttributeId(),
										ESIndexMapping.BOTH, attrType));
					} else {
						mapping.put(attr.getId().getAttributeId(),
								new ESMapping(attr.getId().getAttributeId(),
										ESIndexMapping.NOT_ANALYZED, attrType));
					}
				} catch (ScopeException e) {
					mapping.put(attr.getId().getAttributeId(), new ESMapping(
							attr.getId().getAttributeId(), ESIndexMapping.BOTH,
							ESTypeMapping.STRING));
				}
			}
		}
		if (index.getDimension().getType() == Type.CONTINUOUS) {

			mapping.put(idName + "_l", new ESMapping(idName + "_l",
					ESIndexMapping.NO, computeTypeMapping(idDomainType)));
			mapping.put(idName + "_u", new ESMapping(idName + "_u",
					ESIndexMapping.NO, computeTypeMapping(idDomainType)));
		}

		return mapping;
	}

	private IDomain getAxisDomain(DimensionIndex index) {
		return index.getAxis().getDefinitionSafe().getImageDomain();
	}

	private IDomain getAttributeDomain(ExpressionAST attribute) {
		return attribute.getImageDomain();
	}

	private ESTypeMapping computeIDTypeMapping(DimensionIndex index,
			IDomain type) {
		if (type.isInstanceOf(IDomain.DATE)
				&& index.getDimension().getType() == Type.CONTINUOUS) {
			return ESTypeMapping.STRING;
		} else {
			return computeTypeMapping(type);
		}
	}

	private ESTypeMapping computeTypeMapping(IDomain type) {
		if (type.isInstanceOf(IDomain.STRING)) {
			return ESTypeMapping.STRING;
		} else if (type.isInstanceOf(IDomain.NUMERIC)) {
			return ESTypeMapping.DOUBLE;
		} else if (type.isInstanceOf(IDomain.DATE)) {
			return ESTypeMapping.DATE;
		} else if (type.isInstanceOf(IDomain.CONDITIONAL)) {// works for BOOLEAN
															// too
			return ESTypeMapping.BOOLEAN;
		} else {
			return ESTypeMapping.STRING;
		}
	}

	@Override
	public boolean isCached() {
		return cached;
	}

	@Override
	public int getSize() {
		return size.get();
	}

	@Override
	public String index(List<DimensionMember> members, boolean wait)
			throws IndexationException {
		try {
			ArrayList<HashMap<String, Object>> data = new ArrayList<>();
			for (DimensionMember member : members) {
				HashMap<String, Object> attributes = new HashMap<>();
				Object ID = member.getID();
				if (ID instanceof IntervalleObject) {
					IntervalleObject x = (IntervalleObject) ID;
					attributes.put(idName + "_l", x.getLowerBound());
					attributes.put(idName + "_u", x.getUpperBound());
					attributes.put(idName_mapping, ID);
				} else {
					attributes.put(idName_mapping, ID.toString());
					attributes.put(ESIndexFacadeUtilities.sortKey, ID.toString().toLowerCase());
					
					if (mapping.containsKey(idName_mapping+ESIndexFacadeUtilities.rawSuffix)){
						attributes.put(idName_mapping+ESIndexFacadeUtilities.rawSuffix, ID);
					}
				}
				for (int k = 0; k < getAttributeCount(); k++) {
					attributes.put(getAttributes().get(k).getId()
							.getAttributeId(), member.getAttributes()[k]);
				}
				data.add(attributes);
			}
			return master.addBatchDimensionMembers(indexName,
					dimensionTypeName, idName_mapping, data, mapping, wait);
		} catch (ESIndexFacadeException e) {
			// TODO Auto-generated catch block
			throw new IndexationException(e);
		}
	}

	@Override
	public void index(DimensionMember member) {
		// TODO Auto-generated method stub

	}

	@Override
	public DimensionMember index(Object[] raw) {
		HashMap<String, Object> attributes = new HashMap<>();
		if (raw[0] instanceof IntervalleObject) {
			IntervalleObject x = (IntervalleObject) raw[0];
			attributes.put(idName + "_l", x.getLowerBound());
			attributes.put(idName + "_u", x.getUpperBound());
			attributes.put(idName_mapping, raw[0]);
		} else {
			attributes.put(idName_mapping, raw[0].toString());
			if (mapping.containsKey(idName_mapping+ESIndexFacadeUtilities.rawSuffix)){
				attributes.put(idName_mapping+ESIndexFacadeUtilities.rawSuffix, raw[0]);
			}
		}
		for (int k = 1; k < raw.length; k++) {
			attributes.put(getAttributes().get(k - 1).getId().getAttributeId(),
					raw[k]);
		}
		try {
			master.addDimensionMember(indexName, dimensionTypeName,
					idName_mapping, attributes, mapping);
			size.incrementAndGet();
			return new DimensionMember(raw);
		} catch (ESIndexFacadeException e) {
			return null;
		}
	}

	@Override
	public List<DimensionMember> getMembers(int offset, int size) {
		try {
			if (this.mappingInitialized) {
				ArrayList<Map<String, Object>> elements = master
						.getNDimensionMembers(indexName, dimensionTypeName,
								this.idName_mapping, offset, size, this.mapping);
				return readMembers(elements);
			} else {
				return new ArrayList<DimensionMember>();
			}
		} catch (ESIndexFacadeException e) {
			logger.error("failed to get members page from ES: "
					+ e.getMessage());
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<DimensionMember> getMembers(String filter, int offset, int size) {

		try {
			if (this.mappingInitialized) {
				String[] tokens = filter.split("\\s");
				List<Map<String, Object>> elements = master
						.searchDimensionMembersByTokensAndLocalFilter(
								indexName, dimensionTypeName, tokens, offset,
								size, mapping, idName_mapping).hits;
				return readMembers(elements);
			} else {
				return new ArrayList<DimensionMember>();
			}
		} catch (Exception e) {
			logger.error("failed to get members page from ES: "
					+ e.getMessage());
			throw new RuntimeException(e);
		}
	}

	protected List<DimensionMember> readMembers(
			List<Map<String, Object>> elements) {
		ArrayList<DimensionMember> members = new ArrayList<>();
		boolean isContinuous = getDimensionIndex().getDimension().getType() == Type.CONTINUOUS ;
				IDomain image = getDimensionIndex().getAxis().getDefinitionSafe().getImageDomain();
		boolean isDate = image.isInstanceOf(IDomain.TEMPORAL);

			for (Map<String, Object> element : elements) {
				DimensionMember m =readMember(element, isContinuous, isDate); 
				if (m!=null){
					members.add(m) ;
				}
			}
		
		return members;
	}
	
	
	
	private DimensionMember readMember(Map<String, Object> element, boolean isContinuous, boolean isDate){
		if (isContinuous) {
			return  readContinuousMemberNoAttribute(element, isDate);
		}else{
			return readMemberWithAttributes(element);
		}			
	}
	
	
	
	private DimensionMember readContinuousMemberNoAttribute(Map<String, Object> element, boolean isDate){
		Comparable<?> lower_bound = (Comparable<?>) element
				.get(idName + "_l");
		Comparable<?> upper_bound = (Comparable<?>) element
				.get(idName + "_u");
		if (lower_bound != null && upper_bound != null) {
			if (isDate) {
				try {
					Date lower_date = ServiceUtils.getInstance().toDate((String)lower_bound);
					Date upper_date = ServiceUtils.getInstance().toDate((String)upper_bound);
					IntervalleObject interval = new IntervalleObject(
							lower_date, upper_date);
					return new DimensionMember(-1, interval, 0);
				} catch (ParseException e) {
					IntervalleObject interval = new IntervalleObject(
							lower_bound, upper_bound);
					return new DimensionMember(-1, interval, 0);
				}
			} else {
				IntervalleObject interval = new IntervalleObject(
						lower_bound, upper_bound);
				return new DimensionMember(-1, interval, 0);
			}
		}
		return null;
	}
	
	
	 private DimensionMember readMemberNoAttribute(Map<String, Object> element){
		 Object ID;
			if(mapping.containsKey(idName_mapping+ESIndexFacadeUtilities.rawSuffix)){
				ID = element.get(idName_mapping+ESIndexFacadeUtilities.rawSuffix);
			}else{
				ID = element.get(idName_mapping);					
			}
			if (ID != null) {
				return new DimensionMember(-1, ID, 0);
			} else {
				ID = element.get(idName);
				if (ID != null) {
					return new DimensionMember(-1, ID, 0);
				}
				return null;
			}		 
	 }
	 
	 private DimensionMember readMemberWithAttributes(Map<String, Object> element){
		 DimensionMember m = readMemberNoAttribute(element);
		 if (m!=null && getAttributeCount() > 0){
			 int i = 0;
				for (Attribute attr : getAttributes()) {
					Object value = element.get(attr.getOid());
					if (value != null) {
						m.setAttribute(i, value);
					}
					i++;
				}
		 }
		 
		 return m ;
	 }

	
	
	

	@Override
	public List<DimensionMember> getMembersFilterByParents(
			Map<DimensionIndex, List<DimensionMember>> selections, int offset,
			int size) {
		// call the root method
		try {
			return rootStore.getMembersFilterByParents(this, selections,
					offset, size);
		} catch (ESIndexFacadeException e) {
			logger.error("failed to get members page from ES: "
					+ e.getMessage());
			throw new RuntimeException(e);
		}
	}

	protected List<DimensionMember> getMembersFilterByParents(
			DimensionStoreES target,
			Map<DimensionIndex, List<DimensionMember>> selections, int offset,
			int size) throws ESIndexFacadeException {
		HashMap<String, ArrayList<String>> filters = createFilterByParents(selections);

		// get the ID of dimensionMember
		HierarchiesSearchResult  results = master.filterHierarchyByMemberValues(indexName,
				hierarchyTypeName, target.getDimensionFieldName(), filters,
				offset, size, mappingCorrelations);
		if (getAttributeCount() != 0){			
			//we need to retrieve the full dimensionmembers - with attributes - from the dimensionIndex 
			 ArrayList<Map<String, Object>> withAttr = master.getDimensionByIDs(indexName,target.getDimensionFieldName(),new ArrayList<String>(results.hitsID));
			 return createDimensionMembers(results, withAttr,target.getDimensionFieldName() );
		}else{			
			return createDimensionMembers(results,target.getDimensionFieldName());
		}
	}

	@Override
	public List<DimensionMember> getMembersFilterByParents(
			Map<DimensionIndex, List<DimensionMember>> selections,
			String filter, int offset, int size) {
		try {
			return rootStore.getMembersFilterByParents(this, selections,
					filter, offset, size);
		} catch (ESIndexFacadeException e) {
			logger.error("failed to get members page from ES: "
					+ e.getMessage());
			throw new RuntimeException(e);
		}
	}

	protected List<DimensionMember> getMembersFilterByParents(
			DimensionStoreES target,
			Map<DimensionIndex, List<DimensionMember>> selections,
			String filter, int offset, int size) throws ESIndexFacadeException {
		
		
		if ((this.rootStore == this ) && (this.correlationMappingInitialized )){ 
			HashMap<String, ArrayList<String>> filters = createFilterByParents(selections);
			if (getAttributeCount() == 0) {
				Set<String> results = master
						.filterHierarchyByMemberValuesAndSubstring(indexName,
								hierarchyTypeName, target.getDimensionFieldName(),
								filters, filter, offset, size, mappingCorrelations).hits;
				return createDimensionMembers(results);
			} else {
				// Set<String> results =
				// master.filterHierarchyByMemberValues(indexES, "H/"+rootGen,
				// dimensionUUID, filters, offset, size,
				// rootStore.mappingCorrelations).hits;
				// List<Map<String, Object>> filtered =
				// master.searchDimensionMembersByIdsAndSubstring(indexES,
				// dimensionGen, results, filter, offset, size, mapping);
				// return createDimensionMembers(filtered);
				Set<> results = master
						.filterHierarchyByMemberValuesAndSubstring(indexName,
								hierarchyTypeName, target.getDimensionFieldName(),
								filters, filter, offset, size, mappingCorrelations).hits;
				return createDimensionMembers(results);
			}
		}else{
			return  new ArrayList<DimensionMember>() ;
		}
	}

	private List<DimensionMember> createDimensionMembers(HierarchiesSearchResult res, String dimensionName ) {
		ArrayList<DimensionMember> members = new ArrayList<>();
		if (res.hasAttr){ 
			boolean isContinuous = getDimensionIndex().getDimension().getType() == Type.CONTINUOUS ;
			IDomain image = getDimensionIndex().getAxis().getDefinitionSafe().getImageDomain();
			boolean isDate = image.isInstanceOf(IDomain.TEMPORAL);
			for (String ID : res.hitsID){ 
				members.add(this.readMember(res.hitsAttr.get(ID), isContinuous, isDate));
			}
		}else{
		for (String ID : res.hitsID) {
			if (ID != null) {
				members.add(new DimensionMember(-1, ID, getAttributeCount()));
			}
		}
		}
		return members;
	}

	private List<DimensionMember> createDimensionMembers(HierarchiesSearchResult res, ArrayList<Map<String, Object>> withAttr, String dimensionName ) {
		ArrayList<DimensionMember> members = new ArrayList<DimensionMember>(res.hitsID.size());
		if (withAttr ==null){
			return createDimensionMembers(res, dimensionName);
		}
		boolean isContinuous = getDimensionIndex().getDimension().getType() == Type.CONTINUOUS ;
		IDomain image = getDimensionIndex().getAxis().getDefinitionSafe().getImageDomain();
		boolean isDate = image.isInstanceOf(IDomain.TEMPORAL);
		
		for (Map<String, Object> hit : withAttr ){
			DimensionMember m = this.readMember(hit, isContinuous, isDate);	
			ArrayList<String> ids = new ArrayList<String>( res.hitsID);
			members.add(ids.indexOf(m.getID()), m);
						
		}
		return members;
		
	}
	
	
	private HashMap<String, ArrayList<String>> createFilterByParents(
			Map<DimensionIndex, List<DimensionMember>> selections) {
		//
		HashMap<String, ArrayList<String>> filters = new HashMap<>();
		for (DimensionIndex index : selections.keySet()) {
			if (index.getDimension().getType() == Type.CATEGORICAL) {// only
																		// handles
																		// categorical
				ArrayList<String> values = new ArrayList<>();
				filters.put(index.getDimension().getId().toUUID(), values);
				for (DimensionMember member : selections.get(index)) {
					values.add(member.getID().toString());
				}
			}
		}
		return filters;
	}

	@Override
	public List<DimensionMember> getMembers() {
		try {
			if (this.mappingInitialized){
				ArrayList<Map<String, Object>> elements = master
					.getNDimensionMembers(indexName, dimensionTypeName,
							this.idName_mapping, 0, 10000, this.mapping);
				return readMembers(elements);
			}else{
				return new ArrayList<DimensionMember>();
			}
			
		} catch (ESIndexFacadeException e) {
			logger.error("failed to get members page from ES: "
					+ e.getMessage());
			throw new RuntimeException(e);
		}
	}

	@Override
	public DimensionMember getMember(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DimensionMember getMemberByID(Object iD) {
		if (iD==null) {
			// handling NULL value
			return new DimensionMember(-1, iD, getAttributeCount());
		}
		DimensionMember member = getMemberByKey(iD.toString());
		if (member != null) {
			return member;
		} else {
			return new DimensionMember(-1, iD, getAttributeCount());
		}
	}

	@Override
	public DimensionMember getMemberByKey(String key) {
		try {
			if (this.mappingInitialized){
				Map<String, Object> map = master.getDimensionValue(indexName,
						dimensionTypeName, key);
				if (map != null) {
					Object value = map.get(idName_mapping);
					DimensionMember member = new DimensionMember(0, value,
							getAttributeCount());
					int i = 0;
					for (Attribute attr : getAttributes()) {
						Object v = map.get(attr.getId().getAttributeId());
						if (v != null) {
							member.setAttribute(i, v);
						}
						i++;
					}
					return member;
				} else {
					return null;
				}
			} else {
				return null;
			}
		} catch (ESIndexFacadeException e) {
			logger.error(e.toString());
			return null;
		}
	}

	@Override
	public String indexCorrelations(List<DimensionIndex> types,
			List<DimensionMember> values) throws IndexationException {
		try {
			ArrayList<String> estypes = new ArrayList<>();
			for (int i = 0; i < types.size(); i++) {
				estypes.add(types.get(i).getDimension().getId().toUUID());
			}
			return master.addHierarchyCorrelation(indexName, hierarchyTypeName,
					estypes, values, mappingCorrelations);
		} catch (ESIndexFacadeException e) {
			throw new IndexationException(e);
		}
	}

	@Override
	public String indexCorrelations(List<DimensionIndex> types,
			Collection<List<DimensionMember>> batch, boolean wait)
			throws IndexationException {
		//
		ArrayList<String> estypes = new ArrayList<>();
		for (int i = 0; i < types.size(); i++) {
			estypes.add(types.get(i).getDimension().getId().toUUID());
		}
		try {

			return master.addHierarchyCorrelationsBatch(indexName,
					hierarchyTypeName, estypes, batch, mappingCorrelations,
					wait);
		} catch (ESIndexFacadeException e) {
			// TODO Auto-generated catch block

			throw new IndexationException(e);
		}
	}

	@Override
	public boolean initCorrelationMapping(List<DimensionIndex> hierarchy) {
		//
		// init the hierarchy type name
		ArrayList<String> dependencies = new ArrayList<>();
		for (DimensionIndex child : hierarchy) {
			dependencies.add(getFieldName(child.getDimension()));
		}
		logger.debug("init correlation mapping ");
		this.refreshHierarchyTypeName();

		logger.info("init correlation mapping " + this.hierarchyTypeName);
		/*
		 * this.correlationGenKey = getGenKey(ESHierarchyPrefix +
		 * dimensionFieldName, dependencies );
		 */
		//
		// init the mapping
		mappingCorrelations = new HashMap<>();
		for (DimensionIndex index : hierarchy) {
			String typename = getFieldName(index.getDimension());
			ESTypeMapping type = getTypeMapping(index);
			if (index.getDimension().getType() == Type.CONTINUOUS) {
				mappingCorrelations.put(typename, new ESMapping(
						typename + "_l", ESIndexMapping.NOT_ANALYZED, type));
				mappingCorrelations.put(typename, new ESMapping(
						typename + "_u", ESIndexMapping.NOT_ANALYZED, type));
			} else {
				mappingCorrelations.put(typename, new ESMapping(typename,
						ESIndexMapping.BOTH, type));
			}
		}
		return this.restoreHierarchyMapping();

	}

	protected boolean restoreHierarchyMapping() {
		// check mapping
		logger.info("restore hierarchyMapping");

		MappingState state = this.master.computeCorrelationMappingState(
				indexName, hierarchyTypeName, mappingCorrelations);

		if (state == MappingState.ERROR) {
			logger.info("Could not create a mapping for type "
					+ hierarchyTypeName);
			this.getDimensionIndex().setPermanentError(
					"could not create mappings");
			return false;
		}

		if (state == MappingState.EXISTSEQUAL) {
			logger.info("Mapping for type  " + hierarchyTypeName
					+ "  already exists");
			Status status = this.getDimensionIndex().getStatus();
			if (status == DimensionIndex.Status.DONE) {
				logger.info("index " + this.getDimensionIndex().toString()
						+ ": restoring DimensionStore from ES cache");
			} else {
				logger.info("index " + this.getDimensionIndex().toString()
						+ ": not in ES cache");
				this.cached = false;
			}
		}
		if (state == MappingState.EXISTSDIFFERENT) {
			logger.info("A different mapping exists for type  "
					+ hierarchyTypeName,
					", we'll attempt to destroy it and create a new one");
			this.cached = false;
			this.getDimensionIndex().setStale();
			boolean delRes = this.master.destroyCorrelationMapping(
					this.indexName, hierarchyTypeName);
			if (!delRes) {
				logger.info("failed creating mapping for hierarchy "
						+ hierarchyTypeName);
				this.getDimensionIndex().setPermanentError(
						"could not create hierarchy mappings");
				return false;
			} else {
				// reset references
				// RedisCacheManager.getInstance().refresh( ESHierarchyPrefix +
				// dimensionFieldName);// refresh the hierarchy
				initReferences(this.getDimensionIndex());

				try {
					if (!this.master.addHierarchyCorrelationMapping(indexName,
							hierarchyTypeName, mappingCorrelations)) {
						logger.info("failed creating mapping for hierarchy "
								+ hierarchyTypeName);
						this.getDimensionIndex().setPermanentError(
								"could not create hierarchy mappings");
						return false;
					}
				} catch (ESIndexFacadeException e) {
					logger.info("failed creating mapping for type "
							+ hierarchyTypeName);
					this.getDimensionIndex().setPermanentError(
							"could not create hierarchy mappings");
					return false;
				}
			}
		}
		if (state == MappingState.DOESNOTEXIST) {
			logger.info("index " + this.getDimensionIndex().toString()
					+ ": new");
			this.getDimensionIndex().setStale();
			this.cached = false;
			try {
				if (!this.master.addHierarchyCorrelationMapping(indexName,
						hierarchyTypeName, mappingCorrelations)) {
					logger.info("failed creating mapping for hierarchy "
							+ hierarchyTypeName);
					this.getDimensionIndex().setPermanentError(
							"could not create hierachy mappings");
					return false;
				}
			} catch (ESIndexFacadeException e) {
				logger.info("failed creating mapping for hierarchy  "
						+ hierarchyTypeName);
				this.getDimensionIndex().setPermanentError(
						"could not create hierarchy mappings");
				return false;
			}
		}
		
		this.correlationMappingInitialized = true;
		return true;
	}

	protected ESTypeMapping getTypeMapping(DimensionIndex index) {
		try {
			return getTypeMapping(index.getAxis().getDefinition()
					.getImageDomain());
		} catch (ScopeException e) {
			return ESTypeMapping.STRING;
		}
	}

	protected ESTypeMapping getTypeMapping(IDomain domain) {
		if (domain.isInstanceOf(IDomain.STRING)) {
			return ESTypeMapping.STRING;
		} else if (domain.isInstanceOf(IDomain.DATE)) {
			return ESTypeMapping.DATE;
		} else if (domain.isInstanceOf(IDomain.NUMERIC)) {
			return ESTypeMapping.DOUBLE;
		} else {
			return ESTypeMapping.STRING;
		}
	}

	@Override
	public boolean isDimensionIndexationDone(String lastIndexedDimension) {
		try {
			if (this.mappingInitialized) {
				if (master.getDimensionValue(indexName, dimensionTypeName,
						lastIndexedDimension) != null) {
					return true;
				} else {
					return false;
				}
			} else {
				return false;
			}

		} catch (ESIndexFacadeException e) {
			return false;
		}
	}

	@Override
	public boolean isCorrelationIndexationDone(String lastIndexedCorrelation) {
		try {
			if(rootStore.correlationMappingInitialized){
				if (master.getDimensionValue(indexName, hierarchyTypeName,
						lastIndexedCorrelation) != null) {
					return true;
				} else {
					return false;
				}
			}else{
				return false;
			}
			
		} catch (ESIndexFacadeException e) {
			return false;
		}
	}

	
}
