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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.squid.kraken.v4.ESIndexFacade.ESIndexFacadeException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.model.Attribute;

public class DimensionStoreMock implements IDimensionStore {

	public DimensionStoreMock() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public int getSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String index(List<DimensionMember> members, boolean wait) {
		// TODO Auto-generated method stub
		return "";
	}

	@Override
	public void index(DimensionMember member) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<DimensionMember> getMembers() {
		return Collections.emptyList();
	}

	@Override
	public DimensionMember getMember(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DimensionMember getMemberByID(Object iD) {
		return new DimensionMember(-1, iD, 0);
	}

	@Override
	public List<Attribute> getAttributes() {
		return Collections.emptyList();
	}

	@Override
	public DimensionIndex getDimensionIndex() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isCached() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public DimensionMember index(Object[] raw) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<DimensionMember> getMembers(int offset, int size) {
		return Collections.emptyList();
	}

	@Override
	public List<DimensionMember> getMembers(String filter, int offset, int size) {
		return Collections.emptyList();
	}

	@Override
	public List<DimensionMember> getMembersFilterByParents(
			Map<DimensionIndex, List<DimensionMember>> selections, int offset,
			int size) {
		return Collections.emptyList();
	}

	@Override
	public List<DimensionMember> getMembersFilterByParents(
			Map<DimensionIndex, List<DimensionMember>> selections,
			String filter, int offset, int size) {
		return Collections.emptyList();
	}

	@Override
	public DimensionMember getMemberByKey(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String indexCorrelations(List<DimensionIndex> types,
			List<DimensionMember> values) {
		// TODO Auto-generated method stub
		return "";

	}

	@Override
	public String  indexCorrelations(List<DimensionIndex> types,
			Collection<List<DimensionMember>> values, boolean wait) {
		// TODO Auto-generated method stub
		return "";
	}

	@Override
	public boolean initCorrelationMapping(List<DimensionIndex> hierarchy) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isDimensionIndexationDone(String lastIndexedDimension) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCorrelationIndexationDone(String lastIndexedCorrelation) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setup(DimensionIndex index, String query)
			throws ESIndexFacadeException {
		// TODO Auto-generated method stub
		
	}

}
