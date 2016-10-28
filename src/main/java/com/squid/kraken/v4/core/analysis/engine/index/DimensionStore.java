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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.ESIndexFacade.ESIndexFacadeException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.model.Dimension.Type;

/**
 * a very simple piece of storage for DimensionMember that can be share by several DimensionIndexes if needed
 * @author sergefantino
 *
 */
public class DimensionStore extends DimensionStoreAbstract {
    
    private static final Logger logger = LoggerFactory.getLogger(DimensionStore.class);
    
    @SuppressWarnings("unused")
	private String generation;
    private List<DimensionMember> members = new ArrayList<DimensionMember>();
    private int size = 0;
    // index the members by their IDs
    private ConcurrentSkipListMap<Object, DimensionMember> IDs = 
    		// use a special comparator to avoid classCastExceptions
    		new ConcurrentSkipListMap<Object, DimensionMember>(new PolymorphComparator());

    public DimensionStore(DimensionIndex index) {
        super(index);
        this.generation = getGeneration(getDimensionIndex().getDimension());
    }
    
    @Override
    public boolean isCached() {
        return false;
    }
    
    public int getSize() {
        return size;
    }
    
    @Override
    public String index(List<DimensionMember> members, boolean wait) {
        synchronized (this) {
            for (DimensionMember member : members) {
                DimensionMember check = IDs.get(member.getID());
                if (check==null) {
                    member.setIndex(size);
                    members.add(member);
                    size++;
                    IDs.put(member.getID(), member);
                } else {
                    // update values
                    members.set(check.getIndex(), member);
                    IDs.put(member.getID(), member);
                }
            }
            return "";
        }
    }
    
    @Override
    public DimensionMember index(Object[] raw) {
        Object ID = raw[0];
        DimensionMember member = IDs.get(ID);
        if (member==null) {
            // create a member with that ID
            // note that the member may not be fully initialized (attributes..)
            synchronized (this) {// make sure we create only one entry
                // ticket:2993 always sync because initialization is not blocking
                member = IDs.get(ID);// atomic check
                if (member==null) {
                    member = new DimensionMember(members.size(),ID,getAttributeCount());
                    for (int k=1;k<raw.length;k++) {
                        member.setAttribute(k-1, raw[k]);
                    }
                    members.add(member);
                    size++;
                    IDs.put(member.getID(), member);
                }
            }
        }
        return member;
    }

    /**
     * Always return a DimensionMember
     * check if a member with that ID already exists and return it or else create a new one
     * @param ID
     * @return the DimensionMember, or a new one
     */
    public DimensionMember getMemberByID(Object ID) {
		if (ID==null) {
			// handling NULL value
			return new DimensionMember(-1, ID, getAttributeCount());
		}
        DimensionMember member = IDs.get(ID);
        if (member==null) {
            // create a member with that ID
            // note that the member may not be fully initialized (attributes..)
            synchronized (this) {// make sure we create only one entry
                // ticket:2993 always sync because initialization is not blocking
                member = IDs.get(ID);// atomic check
                if (member==null) {
                    member = new DimensionMember(members.size(),ID,getAttributeCount());
                    members.add(member);
                    size++;
                    IDs.put(member.getID(), member);
                }
            }
        }
        return member;
    }

    public DimensionMember getMember(int index) {
        if (index!=DimensionMember.NULL) {
            if (index<size) {
                return members.get(index);
            } else {
                logger.warn("invalid index ("+index+") for member in dimension '"+getDimensionIndex().getDimension().getName()+"'");
                return null;
            }
        } else {
            return null;
        }
    }
    
    public List<DimensionMember> getMembers() {
        return Collections.unmodifiableList(members);
    }
    
    @Override
    public List<DimensionMember> getMembers(int offset, int size) {
        if (!members.isEmpty()) {
            int fromIndex = offset>=members.size()?members.size():offset;
            int toIndex = offset+size-1;
            toIndex = toIndex>=members.size()?members.size():toIndex;
            return members.subList(fromIndex, toIndex);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public List<DimensionMember> getMembers(String filter, int offset, int size) {
        if (!members.isEmpty()) {
            int index = offset>=members.size()?members.size()-1:offset;
            List<DimensionMember> result = new ArrayList<>();
            int max = members.size();
            String filterLowerCase = filter.toLowerCase();
            while (result.size()<size && index<max) {
                DimensionMember item = members.get(index);
                if (item.match(filterLowerCase)) {
                    result.add(item);
                }
                index++;
            }
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    public void index(DimensionMember member) {
        if (getDimensionIndex().getDimension().getType()==Type.CATEGORICAL) {
            // ticket:2944 disengage Search service until implementation has been fixed
            //SearchManager.INSTANCE.index(dimension, member);
        }
    }
    
    public Collection<DimensionMember> simpleLookup(Object something) {
/*      System.out.println(" in simple lookup searching for  "+ something + " " + something.getClass());
        for(Object o : this.IDs.keySet())
            System.out.println(" in simple lookup "+ o + " " + o.getClass()); */
        DimensionMember m = this.IDs.get(something);
        if (m!=null){
//          System.out.println("found it" );
            return Collections.singletonList(m);
        }else
            return Collections.emptyList();
    }

    @Override
    public DimensionMember getMemberByKey(String key) {
       return IDs.get(key);
    }
    
    @Override
    public String indexCorrelations(List<DimensionIndex> types, List<DimensionMember> values) {
        ArrayList<DimensionMember> accu = new ArrayList<>();
        for (int i=0;i<values.size();i++) {
            DimensionMember value = values.get(i);
            if (!accu.isEmpty()) {
                addCorrelations(value,accu);
            }
            accu.add(value);
        }
        return "";
    }

    @Override
    public String indexCorrelations(List<DimensionIndex> types, Collection<List<DimensionMember>> batch, boolean wait) {
        for (List<DimensionMember> value : batch) {
            indexCorrelations(types,value);
        }
        return "";
    }
    
    @Override
    public boolean initCorrelationMapping(List<DimensionIndex> hierarchy) {
        return true;// always OK
    }
    
    @Override
    public List<DimensionMember> getMembersFilterByParents(
            Map<DimensionIndex, List<DimensionMember>> selections, int offset,
            int size) {
        ArrayList<DimensionMember> result = new ArrayList<>();
        Set<DimensionMember> flatsel = new HashSet<>();
        int types = 0;
        for (List<DimensionMember> forType : selections.values()) {
            flatsel.addAll(forType);
            types++;
        }
        for (DimensionMember member : members) {
            @SuppressWarnings("unchecked")
			Collection<DimensionMember> check = CollectionUtils.intersection(getCorrelations(member),flatsel);
            if (!check.isEmpty() && check.size()>=types) {
                // ok, in some special cases, the test is not enough - we should check that we have a hit for each type
                result.add(member);
            }
        }
        return result;
    }
    
    @Override
    public List<DimensionMember> getMembersFilterByParents(
            Map<DimensionIndex, List<DimensionMember>> selections, 
            String filter, int offset, int size) {
        ArrayList<DimensionMember> result = new ArrayList<>();
        Set<DimensionMember> flatsel = new HashSet<>();
        int types = 0;
        for (List<DimensionMember> forType : selections.values()) {
            flatsel.addAll(forType);
            types++;
        }
        String filterLowerCase = filter.toLowerCase();
        for (DimensionMember member : members) {
            @SuppressWarnings("unchecked")
			Collection<DimensionMember> check = CollectionUtils.intersection(getCorrelations(member),flatsel);
            if (!check.isEmpty() && check.size()>=types && member.match(filterLowerCase)) {
                // ok, in some special cases, the test is not enough - we should check that we have a hit for each type
                result.add(member);
                if (result.size()>=size) {
                    break;
                }
            }
        }
        return result;
    }
    
    private ConcurrentHashMap<Object, Set<DimensionMember>> correlationMap = new ConcurrentHashMap<>();
    
    private Set<DimensionMember> getCorrelations(DimensionMember member) {
        Set<DimensionMember> correlations = correlationMap.get(member.getID());
        if (correlations!=null) {
            return correlations;
        } else {
            return Collections.emptySet();
        }
    }

    private void addCorrelations(DimensionMember member,
            Collection<DimensionMember> accu) {
        Set<DimensionMember> correlations = correlationMap.get(member.getID());
        if (correlations==null) {
            correlations = Collections.newSetFromMap(new ConcurrentHashMap<DimensionMember,Boolean>());
            Set<DimensionMember> previous = correlationMap.putIfAbsent(member.getID(), correlations);
            if (previous!=null) {
                correlations = previous;
            }
        }
        correlations.addAll(accu);
    }

	@Override
	public boolean isDimensionIndexationDone(String lastIndexedDimension) {
		return true;
	}

	@Override
	public boolean isCorrelationIndexationDone(String lastIndexedCorrelation) {
		return true;
	}

	@Override
	public void setup(DimensionIndex index, String query)
			throws ESIndexFacadeException {
		// TODO Auto-generated method stub
		
	}
    
}
