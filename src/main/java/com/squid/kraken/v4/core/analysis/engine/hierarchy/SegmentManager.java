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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.StringTokenizer;

import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.InvalidCredentialsAPIException;
import com.squid.kraken.v4.api.core.domain.DomainServiceBaseImpl;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.model.ExpressionInput;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.scope.SegmentExpressionScope;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.model.DimensionPK;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.FacetMember;
import com.squid.kraken.v4.model.FacetMemberString;
import com.squid.kraken.v4.persistence.AppContext;

/**
 * handling segment facet - that is a facet build using all the conditional dimensions from the domain
 * 
 * @author sergefantino
 *
 */
public class SegmentManager {
    
    private static final String FACET_ID = "__segments";
    private static final String FACET_NAME = "Segments";
    
    private static final String OPEN_FILTER_ID = "__openfilter";
    
    public static boolean isSegmentFacet(Facet facet) {
        return facet.getId()!=null && facet.getId().equals(FACET_ID);
        // it is not possible to check the dimension type = NULL
        //return facet.getDimension().getType()==Type.SEGMENTS;
    }

    public static boolean isSegmentFacet(String facetId) {
        return facetId.startsWith(FACET_ID);
    }
    
    public static Facet createSegmentFacet(Universe universe, DomainHierarchy hierarchy, Domain domain, DashboardSelection sel) {
        return createSegmentFacet(universe, hierarchy, domain, FACET_ID, null, 100, 0, sel);
    }

    public static Facet newSegmentFacet(Domain domain) {
        Facet goalFacet = new Facet();
        goalFacet.setName(FACET_NAME);
        goalFacet.setId(SegmentManager.FACET_ID);
        DimensionPK pseudo_id = new DimensionPK(domain.getId(), SegmentManager.FACET_ID);
        Dimension pseudo_dimension = new Dimension();
        pseudo_dimension.setId(pseudo_id);
        pseudo_dimension.setName(FACET_NAME);
        pseudo_dimension.setType(Type.SEGMENTS);
        goalFacet.setDimension(pseudo_dimension);
        goalFacet.setItems(new ArrayList<FacetMember>());
        goalFacet.setSelectedItems(new ArrayList<FacetMember>());
        return goalFacet;
    }

    public static Facet createSegmentFacet(Universe universe, DomainHierarchy hierarchy,
            Domain domain, 
            String facetId,
            String filter, Integer maxResults, Integer startIndex, DashboardSelection sel) {
    	//
    	// create the segment facet
        Facet goalFacet = newSegmentFacet(domain);
        //
        HashSet<String> checkDuplicate = new HashSet<String>();
        // - disabling restriction on open-filters for now
        boolean allow_open_filters = true;//universe.hasRole(domain, Role.WRITE);
        // first check the filter. If it's an expression, use it also to filter existing facets (KRKN-47)
        if (filter!=null && !filter.isEmpty()
                && filter.startsWith("=")) // formula starts with =, like in Excel?
        {
        	// filter is an open filter (KRKN-47)
        	if (!allow_open_filters) {
        		throw new InvalidCredentialsAPIException("you are not allowed to use open-filters", false);
        	}
        	String formula = filter.substring(1);
        	ExpressionAST condition = null;
            try {
                SegmentExpressionScope scope = new SegmentExpressionScope(universe, domain);
                condition = universe.getParser().parse(hierarchy.getRoot().getDomain().getId(),scope,formula);
                if (condition.getImageDomain()!=IDomain.UNKNOWN) {
                	FacetMemberString openFilter = newOpenFilter(condition, formula);
                    goalFacet.getItems().add(openFilter);
                    checkDuplicate.add(openFilter.getId());
                }
            } catch (ScopeException e) {
                throw new APIException(e.getMessage(), e, false);
            }
            // filter on name, expression
	        for (DimensionIndex goal : hierarchy.getSegments(universe.getContext())) {
	            FacetMemberString member = new FacetMemberString(goal.getAxis().prettyPrint(), goal.getDimensionName());
	            if (filter==null || filter=="") {
	            	if (member.getValue().contains(filter)) {
	            		goalFacet.getItems().add(member);
	            	} else if (goal.getDimension().getExpression().getValue().contains(filter)) {
	            		goalFacet.getItems().add(member);
	            	} else if (condition!=null && condition.equals(goal.getAxis().getDefinitionSafe())) {
	            		goalFacet.getItems().add(member);
	            	}
	            }
	            // handle the original selection
	            if (!sel.getMembers(goal.getAxis()).isEmpty()) {
	            	goalFacet.getSelectedItems().add(member);
	            }
	        }
        } else {
        	// filter on name
	        for (DimensionIndex goal : hierarchy.getSegments(universe.getContext())) {
	            FacetMemberString member = new FacetMemberString(goal.getAxis().prettyPrint(), goal.getDimensionName());
	            if (filter==null || filter=="" || match(member.getValue().toLowerCase(),filter.toLowerCase())) {
	            	goalFacet.getItems().add(member);
	            }
	            // handle the original selection
	            if (!sel.getMembers(goal.getAxis()).isEmpty()) {
	            	goalFacet.getSelectedItems().add(member);
	            }
	        }
        }
        // add selected open filters if any
        for (ExpressionInput condition : sel.getConditions(domain)) {
    		FacetMemberString openFilter = newOpenFilter(condition.getExpression(), condition.getInput());
    		goalFacet.getSelectedItems().add(openFilter);
    		if (!checkDuplicate.contains(openFilter.getId())) {
    			goalFacet.getItems().add(openFilter);
    		}
        }
        // check if there are some conditions in the current selection
        return goalFacet;
    }
    
    public static FacetMemberString newOpenFilter(ExpressionAST condition, String formula) {
    	String ID = OPEN_FILTER_ID+"_"+condition.hashCode();
		FacetMemberString openFilter = new FacetMemberString(ID, "="+formula);
		return openFilter;
    }
    
    public static boolean match(String value, String filter) {
        StringTokenizer tokenizer = new StringTokenizer(filter);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if (value.contains(token)) return true;
        }
        // else
        return false;
    }
    
    public static void addSegmentSelection(AppContext ctx, Universe universe, Facet facet, DashboardSelection ds) throws ComputingException, InterruptedException, ScopeException {
        DomainPK pk = new DomainPK(facet.getDimensionId().getCustomerId(), facet.getDimensionId().getProjectId(), facet.getDimensionId().getDomainId());
        Domain domain = DomainServiceBaseImpl.getInstance().read(ctx, pk);
        DomainHierarchy hierarchy = universe.getDomainHierarchy(domain);
        for (FacetMember selectedItem : facet.getSelectedItems()) {
            if (selectedItem instanceof FacetMemberString) {
                FacetMemberString fmember = (FacetMemberString) selectedItem;
                if (isOpenFilter(fmember)) {
                    String formula = fmember.getValue();
                    if (formula.startsWith("=")) {// if start with equal, remove it
                    	formula = formula.substring(1);
                    }
                    SegmentExpressionScope scope = new SegmentExpressionScope(universe, domain);
                    ExpressionAST condition = universe.getParser().parse(pk,scope,formula);
                    ds.add(new ExpressionInput(formula, condition));
                } else {
                    String id = fmember.getId();
                    if (id!=null) {
                        Axis axis = universe.axis(id);
                        DimensionIndex index = hierarchy.getDimensionIndex(axis);
                        if (index!=null) {
                            AccessRightsUtils.getInstance().checkRole(ctx, index.getDimension(), Role.READ);
                            DimensionMember member = index.getMemberByID(Boolean.TRUE);
                            if (member!=null) {
                                ds.add(axis, member);
                            }
                        }
                    } else {
                        // just ignore
                    }
                }
            }
        }
    }

    /**
     * support for open filters (KRKN-47).
     * Check if the facetMember is actually defining an open filter
     * @param id
     * @return
     */
    private static boolean isOpenFilter(FacetMemberString member) {
        return member.getId()!=null && member.getValue()!=null && member.getId().startsWith(OPEN_FILTER_ID);
    }

}
