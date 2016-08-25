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
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionMember;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionOptionUtils;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainFacetCompute;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchy;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.SegmentManager;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex.Status;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.model.DashboardSelection;
import com.squid.kraken.v4.core.analysis.model.DomainSelection;
import com.squid.kraken.v4.core.analysis.model.Intervalle;
import com.squid.kraken.v4.core.analysis.model.IntervalleObject;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.scope.DimensionDefaultValueScope;
import com.squid.kraken.v4.core.expression.scope.ExpressionEvaluator;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Dimension.Type;
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

    enum Bound {
    	LOWER, UPPER
    }
    
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
     * @param lower 
     * @param value
     * @param compareFromInterval 
     * @return
     * @throws ParseException
     * @throws ScopeException
     * @throws ComputingException 
     */
	public Date convertToDate(Universe universe, DimensionIndex index, Bound bound, String value, IntervalleObject compareFromInterval)
			throws ParseException, ScopeException, ComputingException {
		if (value.startsWith("__")) {
			//
			// support hard-coded shortcuts
			if (value.toUpperCase().startsWith("__COMPARE_TO_")) {
				// for compareTo
				if (compareFromInterval==null) {
					// invalid compare_to selection...
					return null;
				}
				if (value.equalsIgnoreCase("__COMPARE_TO_PREVIOUS_PERIOD")) {
					LocalDate localLower = new LocalDate(((Date)compareFromInterval.getLowerBound()).getTime());
					if (bound==Bound.UPPER) {
						LocalDate date = localLower.minusDays(1);
						return date.toDate();
					} else {
						LocalDate localUpper = new LocalDate(((Date)compareFromInterval.getUpperBound()).getTime());
						Days days = Days.daysBetween(localLower, localUpper);
						LocalDate date = localLower.minusDays(1+days.getDays());
						return date.toDate();
					}
				}
				if (value.equalsIgnoreCase("__COMPARE_TO_PREVIOUS_MONTH")) {
					LocalDate localLower = new LocalDate(((Date)compareFromInterval.getLowerBound()).getTime());
					LocalDate compareLower = localLower.minusMonths(1);
					if (bound==Bound.LOWER) {
						return compareLower.toDate();
					} else {
						LocalDate localUpper = new LocalDate(((Date)compareFromInterval.getUpperBound()).getTime());
						Days days = Days.daysBetween(localLower, localUpper);
						LocalDate compareUpper = compareLower.plusDays(days.getDays());
						return compareUpper.toDate();
					}
				}
				if (value.equalsIgnoreCase("__COMPARE_TO_PREVIOUS_YEAR")) {
					LocalDate localLower = new LocalDate(((Date)compareFromInterval.getLowerBound()).getTime());
					LocalDate compareLower = localLower.minusYears(1);
					if (bound==Bound.LOWER) {
						return compareLower.toDate();
					} else {
						LocalDate localUpper = new LocalDate(((Date)compareFromInterval.getUpperBound()).getTime());
						Days days = Days.daysBetween(localLower, localUpper);
						LocalDate compareUpper = compareLower.plusDays(days.getDays());
						return compareUpper.toDate();
					}
				}
			} else {
				// for regular
				// get MIN, MAX first
				Intervalle range = null;
				if (index.getDimension().getType() == Type.CONTINUOUS) {
					if (index.getStatus()==Status.DONE) {
						List<DimensionMember> members = index.getMembers();
						if (!members.isEmpty()) {
							DimensionMember member = members.get(0);
							Object object = member.getID();
							if (object instanceof Intervalle) {
								range = (Intervalle)object;
							}
						}
					} else {
						try {
							DomainHierarchy hierarchy = universe.getDomainHierarchy(index.getAxis().getParent().getDomain());
							hierarchy.isDone(index, null);
						} catch (ComputingException | InterruptedException | ExecutionException | TimeoutException e) {
							throw new ComputingException("failed to retrieve period interval");
						}
					}
				}
				if (range==null) {
					range = IntervalleObject.createInterval(new Date(), new Date());
				}
				if (value.equalsIgnoreCase("__ALL")) {
					if (index.getDimension().getType() != Type.CONTINUOUS) {
						return null;
					}
					if (bound==Bound.UPPER) {
						return (Date)range.getUpperBound();
					} else {
						return (Date)range.getLowerBound();
					}
				}
				if (value.equalsIgnoreCase("__LAST_DAY")) {
					if (bound==Bound.UPPER) {
						return (Date)range.getUpperBound();
					} else {
						return (Date)range.getUpperBound();
					}
				}
				if (value.equalsIgnoreCase("__LAST_7_DAYS")) {
					if (bound==Bound.UPPER) {
						return (Date)range.getUpperBound();
					} else {
						LocalDate localUpper = new LocalDate(((Date)range.getUpperBound()).getTime());
						LocalDate date = localUpper.minusDays(6);// 6+1
						return date.toDate();
					}
				}
				if (value.equalsIgnoreCase("__CURRENT_MONTH")) {
					if (bound==Bound.UPPER) {
						return (Date)range.getUpperBound();
					} else {
						LocalDate localUpper = new LocalDate(((Date)range.getUpperBound()).getTime());
						LocalDate date = localUpper.withDayOfMonth(1);
						return date.toDate();
					}
				}
				if (value.equalsIgnoreCase("__CURRENT_YEAR")) {
					if (bound==Bound.UPPER) {
						return (Date)range.getUpperBound();
					} else {
						LocalDate localUpper = new LocalDate(((Date)range.getUpperBound()).getTime());
						LocalDate date = localUpper.withMonthOfYear(1).withDayOfMonth(1);
						return date.toDate();
					}
				}
				if (value.equalsIgnoreCase("__PREVIOUS_MONTH")) {// the previous complete month
					if (bound==Bound.UPPER) {
						LocalDate localUpper = new LocalDate(((Date)range.getUpperBound()).getTime());
						LocalDate date = localUpper.withDayOfMonth(1).minusDays(1);
						return date.toDate();
					} else {
						LocalDate localUpper = new LocalDate(((Date)range.getUpperBound()).getTime());
						LocalDate date = localUpper.withDayOfMonth(1).minusMonths(1);
						return date.toDate();
					}
				}
				if (value.equalsIgnoreCase("__PREVIOUS_YEAR")) {// the previous complete month
					if (bound==Bound.UPPER) {
						LocalDate localUpper = new LocalDate(((Date)range.getUpperBound()).getTime());
						LocalDate date = localUpper.withMonthOfYear(1).withDayOfMonth(1).minusDays(1);
						return date.toDate();
					} else {
						LocalDate localUpper = new LocalDate(((Date)range.getUpperBound()).getTime());
						LocalDate date = localUpper.withMonthOfYear(1).withDayOfMonth(1).minusYears(1);
						return date.toDate();
					}
				}
			}
			throw new ScopeException("undefined facet expression alias: " + value);
		} else if (value.startsWith("=")) {
			// if the value starts by equal token, this is a formula that can be
			// evaluated
			try {
				String expr = value.substring(1);
				// check if the index content is available or wait for it
				DomainHierarchy hierarchy = universe.getDomainHierarchy(index.getAxis().getParent().getDomain(), true);
				hierarchy.isDone(index, null);
				// evaluate the expression
				Object defaultValue = evaluateExpression(universe, index, expr, compareFromInterval);
				// check we can use it
				if (defaultValue == null) {
					//throw new ScopeException("unable to parse the facet expression as a constant: " + expr);
					// T1769: it's ok to return null
					return null;
				}
				if (!(defaultValue instanceof Date)) {
					throw new ScopeException("unable to parse the facet expression as a date: " + expr);
				}
				// ok, it's a date
				return (Date) defaultValue;
			} catch (ComputingException | InterruptedException | ExecutionException
					| TimeoutException e) {
				throw new ComputingException("failed to retrieve period interval");
			}
		} else {
			Date date = ServiceUtils.getInstance().toDate(value);
			if (bound==Bound.UPPER && !index.getAxis().getDefinitionSafe().getImageDomain().isInstanceOf(IDomain.TIME)) {
				// clear the timestamp
				return new LocalDate(date.getTime()).toDate();
			} else {
				return date;
			}
		}
	}
	
	private Object evaluateExpression(Universe universe, DimensionIndex index, String expr, IntervalleObject compareFromInterval) throws ScopeException {
		try {
			DimensionDefaultValueScope scope = new DimensionDefaultValueScope(universe.getContext(), index);
			if (compareFromInterval!=null) {
				scope.addParam("UPPER",IDomain.DATE);
				scope.addParam("LOWER", IDomain.DATE);
			}
			ExpressionAST defaultExpression = scope.parseExpression(expr);
			ExpressionEvaluator evaluator = new ExpressionEvaluator(universe.getContext());
			// provide sensible default for MIN & MAX -- we don't want the parser to fail is not set
			Calendar calendar = Calendar.getInstance();
			evaluator.setParameterValue("MAX", calendar.getTime());
			// there is no sensible default for MIN
			evaluator.setParameterValue("MIN", null);
			// handle compareFrom interval values if available
			if (compareFromInterval!=null) {
				evaluator.setParameterValue("LOWER", compareFromInterval.getLowerBound());
				evaluator.setParameterValue("UPPER", compareFromInterval.getUpperBound());
			}
			return evaluator.evalSingle(defaultExpression);
		} catch (ScopeException e) {
			throw new ScopeException("unable to parse the facet expression: " + expr + "\n" + e.getLocalizedMessage(), e);
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
    	if (selection!=null) {
	        addFacetSelection(ctx, universe, selection.getFacets(), ds, null);
	        // T994 support the compareFacets
	        if (selection.hasCompareFacets()) {
	        	DashboardSelection compare = new DashboardSelection();
		        addFacetSelection(ctx, universe, selection.getCompareTo(), compare, ds);
		        // we let the ds.compare chack that the selection is valid
		        for (DomainSelection s : compare.get()) {
		        	for (Axis filter : s.getFilters()) {
		        		for (DimensionMember member : s.getMembers(filter)) {
		        			ds.addCompareTo(filter, member);
		        		}
		        	}
		        }
	        }
    	}
        //
        // krkn-61: handling dimension options
        for (Domain domain : domains) {
        	DomainHierarchy hierarchy = universe.getDomainHierarchy(domain, true);
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

    /**
     * initialise the DashboardSelection
     * @param ctx
     * @param universe
     * @param facetsSel
     * @param ds : the output selection
     * @param compareFrom : if the output selection is a compareTo, you can provide the current selection in order to resolve $LOWER and $UPPER parameters
     * @throws ScopeException
     * @throws ComputingException
     * @throws InterruptedException
     */
	private void addFacetSelection(AppContext ctx, Universe universe, List<Facet> facetsSel, DashboardSelection ds, DashboardSelection compareFrom) throws ScopeException, ComputingException, InterruptedException {
		for (Facet facetSel : facetsSel) {
            if (SegmentManager.isSegmentFacet(facetSel)) {
                SegmentManager.addSegmentSelection(ctx, universe, facetSel, ds);
            } else {
                Axis axis = getFacetAxis(ctx, universe, facetSel);
                if (axis!=null) {
                    Domain domain = axis.getParent().getRoot();
                    DimensionIndex index = universe.
                            getDomainHierarchy(domain, true).
                            getDimensionIndex(axis);
                    AccessRightsUtils.getInstance().checkRole(ctx, index.getDimension(), Role.READ);
                    for (FacetMember selectedItem : facetSel.getSelectedItems()) {
                        if (selectedItem instanceof FacetMemberInterval) {
                        	IntervalleObject compareFromInterval = null;
                        	if (compareFrom!=null) {
                        		Collection<DimensionMember> members = compareFrom.getMembers(axis);
                        		if (members.size()==1) {
                        			DimensionMember member = members.iterator().next();
                        			if (member.getID() instanceof IntervalleObject) {
                        				compareFromInterval = (IntervalleObject)member.getID();
                        			}
                        		}
                        	}
                            FacetMemberInterval fmi = (FacetMemberInterval) selectedItem;
                            try {
                                Date lowerDate = convertToDate(universe, index, Bound.LOWER, fmi.getLowerBound(), compareFromInterval);
                                Date upperDate = convertToDate(universe, index, Bound.UPPER, fmi.getUpperBound(), compareFromInterval);
                                // add as a Date Interval
                                // T1769: if lower&upper are null, this is no-op
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
	}
    
}
