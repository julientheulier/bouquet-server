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
package com.squid.kraken.v4.core.analysis.engine.hierarchy;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;

import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.model.Intervalle;
import com.squid.kraken.v4.core.analysis.model.IntervalleObject;
import com.squid.kraken.v4.model.Attribute;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.FacetMember;
import com.squid.kraken.v4.model.FacetMemberInterval;
import com.squid.kraken.v4.model.FacetMemberString;

/**
 * wrap logic to build facets. Optimization: cache the DateFormat locally. Note
 * that this cause the class to NOT BE THREAD SAFE.
 * 
 * @author sergefantino
 * 
 */
public class FacetBuilder {
	
	public static final String ISO8601_FULL_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

	// note: no need to sync since the FacetBuilder is created by a given thread and not shared
	// so keep it private, we don't want to share it
	private static final DateFormat ISO8601_full = createUTCDateFormat(ISO8601_FULL_FORMAT);

	/**
	 * Build a paged Facet.
	 * 
	 * @param dashboardFacets
	 *            new
	 * @param domain
	 * @param sel 
	 * @param facet
	 * @param maxResults
	 * @param startIndex
	 * @return
	 */
	public Facet buildFacet(Domain domain, DimensionIndex index,
			Collection<DimensionMember> values, DashboardSelection sel) {
		// Facet
		Facet resultFacet = new Facet();
		// TODO better way of localizing
		resultFacet.setDimension(index.getDimension());
		resultFacet.setProxy(index instanceof DimensionIndexProxy);
		resultFacet.setCompositeName(index.isCompositeName());
		// override the dimension name
		resultFacet.setName(index.getDimensionName());
		String id = index.getAxis().prettyPrint();
		resultFacet.setId(encode(id));// kick-fix
		// resultFacet.setId(dimension.getId().getDomainId() + ":" +
		// dimension.getId().getDimensionId());
		// facet members
		List<FacetMember> items = resultFacet.getItems();
		// ticket:3043 - if continuous, only return a single value...
		if (index.getDimension().getType() == Type.CONTINUOUS) {
			if (values != null) {
				IntervalleObject value = null;
				for (DimensionMember next : values) {
					if (value == null) {
						if (next.getID() instanceof IntervalleObject) {
							value = (IntervalleObject) next.getID();
						} else {
							items.add(toFacet(index, next));
						}
					} else {
						if (next.getID() instanceof IntervalleObject) {
							IntervalleObject moreDate = (IntervalleObject) next
									.getID();
							value = value.merge(moreDate);
						} else {
							items.add(toFacet(index, next));
						}
					}
				}
				if (value != null) {
					items.add(toFacet(value));
				}
			}
			resultFacet.setTotalSize(items.size());
		} else {
			if (values != null) {
				// iterate within paging boundaries
				for (DimensionMember member : values) {
					FacetMember fm = toFacet(index, member);
					items.add(fm);
				}
			}
		}
		// add the current selection if any
		Collection<DimensionMember> members = sel.getMembers(index.getAxis());
		for (DimensionMember member : members) {
			resultFacet.getSelectedItems().add(toFacet(index,member));
		}
		return resultFacet;
	}
	
	private String encode(String id) {
	    return id;
	    /*
	    try {
	        return URLEncoder.encode(id, "UTF-8");
	    } catch (UnsupportedEncodingException e) {
	        return URLEncoder.encode(id);
	    }
	    */
	}

    /**
     * Create a FacetMember from a DimensionMember.
     * @param index 
     * 
     * @param member
     * @return
     */
    public FacetMember toFacet(DimensionIndex index, DimensionMember member) {
        if (member.getID() instanceof Intervalle) {
        	Intervalle interval = (Intervalle) member.getID();
            return toFacet(interval);
        } else {
            FacetMemberString fm = new FacetMemberString(member.getKey(), index.getDisplayName(member).toString().trim());
			if (index!=null && !index.getAttributes().isEmpty() && member.getAttributes()!=null) {
				int i=0;
				HashMap<String, String> attrs = new HashMap<String, String>();
				for (Attribute attr : index.getAttributes()) {
					if (member.getAttributes()[i]!=null) {
						attrs.put(attr.getName(),member.getAttributes()[i].toString());
					}
					i++;
				}
				fm.setAttributes(attrs);
			}
			return fm;
        }
    }
    
	private FacetMember toFacet(Intervalle interval) {
		if (interval.getLowerBound() instanceof Date && interval.getUpperBound() instanceof Date) {
			String lowerTime = ISO8601_full.format((Date) interval.getLowerBound());
			String upperTime = ISO8601_full.format((Date) interval.getUpperBound());
			return new FacetMemberInterval(lowerTime, upperTime);
		} else {
			// todo: format number ?
			return new FacetMemberInterval(interval.getLowerBound()!=null?interval.getLowerBound().toString():"",
					interval.getUpperBound()!=null?interval.getUpperBound().toString():"");
		}
	}

	public static DateFormat createUTCDateFormat(String format) {
		DateFormat df = new SimpleDateFormat(format);
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		return df;
	}

}
