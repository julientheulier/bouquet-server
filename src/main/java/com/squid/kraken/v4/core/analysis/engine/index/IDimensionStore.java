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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.squid.kraken.v4.ESIndexFacade.ESIndexFacadeException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.model.Attribute;

public interface IDimensionStore {

	int getSize();

	String index(List<DimensionMember> members, boolean wait) throws IndexationException;

	void index(DimensionMember member);

	List<DimensionMember> getMembers();

	DimensionMember getMember(int index);

	DimensionMember getMemberByID(Object iD);

	List<Attribute> getAttributes();

	DimensionIndex getDimensionIndex();

	// new API

	/**
	 * return true if the store has been restored from a cache - so no need to
	 * recompute for instance
	 * 
	 * @return
	 */
	boolean isCached();

	/**
	 * index this raw member
	 * 
	 * @param raw
	 * @return
	 */
	DimensionMember index(Object[] raw);

	/**
	 * return a page of members
	 * 
	 * @param offset
	 * @param size
	 * @return
	 */
	List<DimensionMember> getMembers(int offset, int size);

	List<DimensionMember> getMembers(String filter, int offset, int size);

	/**
	 * return a page filtered by parents selection
	 * 
	 * @param selections
	 * @param offset
	 * @param size
	 * @return
	 */
	List<DimensionMember> getMembersFilterByParents(Map<DimensionIndex, List<DimensionMember>> selections, int offset,
			int size);

	List<DimensionMember> getMembersFilterByParents(Map<DimensionIndex, List<DimensionMember>> selections,
			String filter, int offset, int size);

	/**
	 * return a member by its key
	 * 
	 * @param key
	 * @return
	 */
	DimensionMember getMemberByKey(String key);

	/**
	 * register the correlations
	 * 
	 * @param types
	 *            the type of each value
	 * @param values
	 *            the values to index
	 */
	String indexCorrelations(List<DimensionIndex> types, List<DimensionMember> values) throws IndexationException;

	/**
	 * register a batch of correlations
	 * 
	 * @param types
	 * @param values
	 */
	String indexCorrelations(List<DimensionIndex> types, Collection<List<DimensionMember>> values, boolean wait)
			throws IndexationException;

	/**
	 * initialize the hierarchy mapping
	 * 
	 * @param hierarchy
	 */
	boolean initCorrelationMapping(List<DimensionIndex> hierarchy);

	boolean isDimensionIndexationDone(String lastIndexedDimension);

	boolean isCorrelationIndexationDone(String lastIndexedCorrelation);

	void setup(DimensionIndex index, String query) throws ESIndexFacadeException;

}
