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
package com.squid.kraken.v4.api.core;

import java.text.ParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionOptionUtils;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainFacetCompute;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchy;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.SegmentManager;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.model.IntervalleObject;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.scope.DimensionDefaultValueScope;
import com.squid.kraken.v4.core.expression.scope.ExpressionEvaluator;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.DimensionOption;
import com.squid.kraken.v4.model.DimensionPK;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Facet;
import com.squid.kraken.v4.model.FacetMember;
import com.squid.kraken.v4.model.FacetMemberInterval;
import com.squid.kraken.v4.model.FacetMemberString;
import com.squid.kraken.v4.model.FacetSelection;
import com.squid.kraken.v4.persistence.AppContext;

/**
 * provide methods to convert API FacetSelection into Engine DashboardSelection
 *  
 * @author sergefantino
 *
 */
public class EngineUtils {
    
    static final Logger logger = LoggerFactory.getLogger(EngineUtils.class);

    private static EngineUtils instance;

    public static EngineUtils getInstance() {
        if (instance == null) {
        	synchronized (EngineUtils.class) {
                if (instance == null) {
                	instance = new EngineUtils();
                }
			}
        }
        return instance;
    }
    
    /**
     * return the facetID: support legacy code
     * @param facet
     * @return
     */
    public Object getFacetID(Facet facet) {
        if (facet.getId()==null) {
            return facet.getDimensionId();
        } else {
            return facet.getId();
        }
    }

    /**
     * compare the two facet using the first facet identifiction method
     * @param selFacet
     * @param facet
     * @return
     */
    public boolean compareFacets(Facet selFacet, Facet facet) {
        if (selFacet.getId()==null) {
            return selFacet.getDimensionId().equals(facet.getDimensionId());
        } else {
            return selFacet.getId().equals(facet.getId());
        }
    }
    
    /**
     * return the axis for the facet
     * @param ctx
     * @param universe
     * @param facet
     * @return
     * @throws ScopeException if cannot interpret the facet
     */
    public Axis getFacetAxis(AppContext ctx, Universe universe, Facet facet) throws ScopeException {
        String facetID = facet.getId();
        if (facetID!=null) {
            try {
                return universe.axis(facetID);
            } catch (ScopeException e) {
                logger.error("Invalid facet ID = " + facetID, e);
                throw new ScopeException("Invalid facet ID = " + facetID, e);
            }
        } else {
            // legacy
            DimensionPK facetPK = facet.getDimensionId();
            if (facetPK==null) {
                logger.error("Invalid facet ID");
                throw new ScopeException("Invalid facet ID");
            }
            try {
            	facetPK.setCustomerId(ctx.getCustomerId());
            	return universe.axis(facetPK);
            } catch (Exception e) {
                logger.error("Invalid facet ID = " + facetPK.toString(), e);
                throw new ScopeException("Invalid facet ID = " + facetPK.toString(), e);
            }
        }
    }
    
    public Axis getFacetAxis(AppContext ctx, Universe universe, String expression) throws ScopeException {
        if (expression!=null) {
            try {
                return universe.axis(expression);
            } catch (ScopeException e) {
                logger.error("Invalid facet ID = " + expression, e);
                throw new ScopeException("Invalid facet ID = " + expression + " caused by " + e.getLocalizedMessage(), e);
            }
        } else {
            throw new ScopeException("Invalid facet, ID is null");
        }
    }
    
    /**
     * Convert the facet value into a date. 
     * If the value start with '=', it is expected to be a Expression, in which case we'll try to resolve it to a Date constant.
     * @param ctx
     * @param index
     * @param value
     * @return
     * @throws ParseException
     * @throws ScopeException
     */
    public Date convertToDate(AppContext ctx, DimensionIndex index, String value) throws ParseException, ScopeException {
    	if (value.startsWith("=")) {
    		String expr = value.substring(1);
    		DimensionDefaultValueScope scope = new DimensionDefaultValueScope(ctx, index);
			ExpressionAST defaultExpression = scope.parseExpression(expr);
			ExpressionEvaluator evaluator = new ExpressionEvaluator(ctx);
			Object defaultValue = evaluator.evalSingle(defaultExpression);
			if (defaultValue==null) {
				throw new ScopeException("unable to parse the facet expression as a constant: "+value);
			}
			if (!(defaultValue instanceof Date)) {
				throw new ScopeException("unable to parse the facet expression as a date: "+value);
			}
			// ok, it's a date
			return (Date)defaultValue;
    	} else {
    		return ServiceUtils.getInstance().toDate(value);
    	}
    }

    /**
     * Apply a facet selection to a dashboard.
     * 
     * @param universe
     * @param dashboard
     * @param selection
     * @throws ComputingException 
     * @throws ScopeException 
     * @throws InterruptedException 
     */
    public DashboardSelection applyFacetSelection(AppContext ctx, Universe universe, List<Domain> domains, FacetSelection selection) throws ComputingException, ScopeException, InterruptedException {
        //
    	DashboardSelection ds = new DashboardSelection();
    	//
    	List<Facet> facetsSel = selection!=null?selection.getFacets():Collections.<Facet> emptyList();
        for (Facet facetSel : facetsSel) {
            if (SegmentManager.isSegmentFacet(facetSel)) {
                SegmentManager.addSegmentSelection(ctx, universe, facetSel, ds);
            } else {
                Axis axis = getFacetAxis(ctx, universe, facetSel);
                if (axis!=null) {
                    Domain domain = axis.getParent().getRoot();
                    DimensionIndex index = universe.
                            getDomainHierarchy(domain).
                            getDimensionIndex(axis);
                    AccessRightsUtils.getInstance().checkRole(ctx, index.getDimension(), Role.READ);
                    for (FacetMember selectedItem : facetSel.getSelectedItems()) {
                        if (selectedItem instanceof FacetMemberInterval) {
                            FacetMemberInterval fmi = (FacetMemberInterval) selectedItem;
                            try {
                                Date lowerDate = convertToDate(ctx, index, fmi.getLowerBound());
                                Date upperDate = convertToDate(ctx, index, fmi.getUpperBound());
                                // add as a Date Interval
                                ds.add(axis, IntervalleObject.createInterval(lowerDate, upperDate));
                            } catch (java.text.ParseException e) {
                                throw new ComputingException(e);
                            }
                        } else if (selectedItem instanceof FacetMemberString) {
                        	FacetMemberString fmember = (FacetMemberString) selectedItem;
                        	if (fmember.getId()!=null && !fmember.getId().equals("") 
                        	        //&& !fmember.getId().equals("-1") // support legacy drill-down // end of legacy support !
                        	        && facetSel.getId()!=null) {// to support legacy
                        		// if we provide the ID, it's safe to use it...
                        		DimensionMember member = index.getMemberByKey(fmember.getId());
                        		if (member!=null) {
                        			ds.add(axis, member);
                        		} else if (fmember.getValue()!=null) {
                                    // ticket:2992
                                    // if the value is defined, try to get a member
                                    member = index.getMemberByID(fmember.getValue());
                                    if (member!=null) {
                                        fmember.setId(member.getKey());// update the facet Id
                                        ds.add(axis, member);
                                    } else {
                                        throw new ComputingException("invalid selection, unkonwn index value");
                                    }
                        		} else {
                        			throw new ComputingException("invalid selection, unkonwn index reference");
                        		}
                        	} else if (fmember.getValue()!=null) {
                        		// ticket:2992
                        		// if the value is defined, try to get a member
                        		DimensionMember member = index.getMemberByID(fmember.getValue());
                        		if (member!=null) {
                        		    fmember.setId(member.getKey());// update the facet Id
                        			ds.add(axis, member);
                        		} else {
                        			throw new ComputingException("invalid selection, unkonwn index value");
                        		}
                        	} else {
                    			throw new ComputingException("invalid selection, undefine index");
                        	}
                        }
                    }
                } else {
                    logger.info("ignoring invalid facet selection");
                }
            }
        }
        
        //
        // krkn-61: handling dimension options
        for (Domain domain : domains) {
        	DomainHierarchy hierarchy = universe.getDomainHierarchy(domain);
        	for (DimensionIndex index : hierarchy.getDimensionIndexes()) {
        		DimensionOption option = DimensionOptionUtils.computeContextOption(index.getDimension(), ctx);
        		if (option!=null) {
					// handling dimension option
					//
					Collection<DimensionMember> sel = ds.getMembers(index.getAxis());
					// is SingleSelection
					if (option.isSingleSelection() && sel.size()>1) {
						throw new ScopeException("Dimension '"+index.getDimensionName()+"' does not allow multi-selection");
					}
					// is unmodifiable
					if (option.isUnmodifiableSelection()) {
						// if no default value, this is a bug in the meta-model
						if (option.getDefaultSelection()==null) {
    						throw new ScopeException("Dimension '"+index.getDimensionName()+"' is unmodifiable but does not set a default value - report the isue to the application support");
						}
						// check if the default value is selected
						if (!sel.isEmpty()) {
							ds.clear(index.getAxis());
    						sel = ds.getMembers(index.getAxis());
						}
					}
					// default selection
					if (sel.isEmpty() && option.getDefaultSelection()!=null) {
						List<DimensionMember> defaultMembers = DimensionOptionUtils.computeDefaultSelection(index, option, ctx);
						if (defaultMembers.isEmpty()) {
							// ?
						} else {
							ds.add(index.getAxis(), defaultMembers);
    						sel = ds.getMembers(index.getAxis());
						}
						//
					}
					// is mandatory
					if (sel.isEmpty() && option.isMandatorySelection()) {
						DomainFacetCompute compute = new DomainFacetCompute(universe);
						List<DimensionMember> members = compute.populateDimensionFacets(index, ds, null, 0, 1);
						if (!members.isEmpty()) {
							ds.add(index.getAxis(), members.get(0));
    						sel = ds.getMembers(index.getAxis());
						}
						//throw new ScopeException("Dimension '"+index.getDimensionName()+"' cannot be unselected");
					}
        		}
        	}
        }
        //
        return ds;
    }
    
}
