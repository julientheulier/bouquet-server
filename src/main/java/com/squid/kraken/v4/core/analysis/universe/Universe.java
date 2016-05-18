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
package com.squid.kraken.v4.core.analysis.universe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.squid.core.database.model.Table;
import com.squid.core.domain.IDomain;
import com.squid.core.expression.Compose;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.parser.ParseException;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.core.analysis.engine.cartography.Cartography;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchy;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainHierarchyManager;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.scope.AxisExpression;
import com.squid.kraken.v4.core.analysis.scope.MeasureExpression;
import com.squid.kraken.v4.core.analysis.scope.SpaceExpression;
import com.squid.kraken.v4.core.analysis.scope.UniverseScope;
import com.squid.kraken.v4.core.expression.reference.DomainReference;
import com.squid.kraken.v4.core.expression.reference.RelationReference;
import com.squid.kraken.v4.core.expression.scope.Parser;
import com.squid.kraken.v4.core.model.domain.DomainDomain;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.DimensionPK;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.ProjectDAO;

/**
 * The Universe is a representation of the meta-model that can span an entire transaction lifetime.
 * Cached information will never be updated, so the object must be disposed once the transaction is over.
 * Universe is made of Space, Axis and Measure.
 * 
 * @author sergefantino
 *
 */
public class Universe extends Physics {
	
	private Parser parser;

	public Universe(AppContext ctx, Project project) {
		super(ctx, project);
		this.parser = new Parser(this);
	}
	
	public Universe(Project project) {
		this(ServiceUtils.getInstance().getRootUserContext(project.getCustomerId()),project);
	}
	
	/**
	 * get access to the shared parser (with cache)
	 * @return
	 */
	public Parser getParser() {
		return parser;
	}

	/**
	 * return the underlying table by parsing the Domain definition
	 * @return the Table or an exception if not well defined
	 */
	public Table getTable(Domain domain) throws ScopeException {
		try {
			if(domain.getOptions() != null && domain.getOptions().getAlink()){
				Domain toUseDomain = ProjectManager.INSTANCE.getDomain(getContext(), new DomainPK(getContext().getCustomerId(), domain.getOptions().getLinkSource()));
				if (toUseDomain.getSubject() == null) {
					throw new ScopeException("Cannot lookup table definition for domain '" + toUseDomain.getName() + "'");
				}
				ExpressionAST subject = getParser().parse(toUseDomain);
				IDomain image = subject.getImageDomain();// it should return a TableProxy
				Object adapt = image.getAdapter(Table.class);
				if (adapt != null && adapt instanceof Table) {
					return (Table) adapt;
				} else {
					throw new ScopeException("Cannot lookup table definition for domain '" + toUseDomain.getName() + "'");
				}
			}else {
				if (domain.getSubject() == null) {
					throw new ScopeException("Cannot lookup table definition for domain '" + domain.getName() + "'");
				}
				ExpressionAST subject = getParser().parse(domain);
				IDomain image = subject.getImageDomain();// it should return a TableProxy
				Object adapt = image.getAdapter(Table.class);
				if (adapt != null && adapt instanceof Table) {
					return (Table) adapt;
				} else {
					throw new ScopeException("Cannot lookup table definition for domain '" + domain.getName() + "'");
				}
			}
		} catch (ParseException e) {
			throw new ScopeException("Parsing error in table definition for domain '"+domain.getName()+"': "+e.getMessage());
		}
	}
	
	public Cartography getCartography() throws ScopeException {
		return ProjectManager.INSTANCE.getCartography(getContext(), getProject().getId());
	}
	
	public List<Metric> getMetrics(Domain domain) {
		// T16
		try {
			DomainHierarchy hierarchy = getDomainHierarchy(domain, true);
			return hierarchy.getMetrics(getContext());
		} catch (ComputingException | InterruptedException e) {
			//
			return Collections.emptyList();
		}
	}

	/**
	 * return an equivalent universe associated with the RootUserContext
	 * @return
	 */
    public Universe asRootUserContext() {
    	//
    	// reload the project as the root context
    	AppContext rootctx = ServiceUtils.getInstance().getRootUserContext(getContext());
        Project projectRoot = ((ProjectDAO) DAOFactory.getDAOFactory().getDAO(Project.class)).read(rootctx, getProject().getId()).get();
        return new Universe(rootctx, projectRoot);
    }
	
	/**
	 * return the direct Space associated to a Domain
	 * @param domain
	 * @return
	 * @throws ScopeException
	 */
	public Space S(Domain domain) throws ScopeException {
		// check compatibility?
		if (getDomains().contains(domain)) {
			return new Space(this, domain);
		} else {
			throw new ScopeException("Domain '"+domain+"' does not belong to that Universe");
		}
	}

	public Space S(String domainName) throws ScopeException {
		for (Domain domain : getDomains()) {
			if (domain.getName().equals(domainName)) {
				return new Space(this, domain);
			}
		}
		// else
		throw new ScopeException("domain not found: "+domainName);
	}
	
	/**
	 * construct a space from a expression
	 * <li> the expression image domain must be a Domain Object
	 * @param expression
	 * @return
	 * @throws ScopeException 
	 */
	public Space S(ExpressionAST expression) throws ScopeException {
	    IDomain image = expression.getImageDomain();
	    if (image.isInstanceOf(IDomain.OBJECT)) {
            Object adapter = image.getAdapter(Domain.class);
            if (adapter!=null && adapter instanceof Domain) {
                Domain target = (Domain)adapter;
                if (expression instanceof Compose) {
                    Compose compose = (Compose)expression;
                    Space result = null;
                    for (ExpressionAST part : compose.getBody()) {
                        if (result==null) {
                            if (part instanceof DomainReference) {
                                DomainReference ref = (DomainReference)part;
                                result = S(ref.getDomain());
                            } else {
                                throw new ScopeException("Invalid expression");
                            }
                        } else {
                            if (part instanceof RelationReference) {
                                RelationReference ref = (RelationReference)part;
                                result = result.S(ref.getRelation());
                            } else {
                                throw new ScopeException("Invalid expression");
                            }
                        }
                    }
                    return result;
                } else {
                    return S(target);
                }
            }
	    } 
	    // else
	    throw new ScopeException("Invalid expression type, must be a Domain Object");
	}
	
	/**
	 * return all the available spaces
	 * @return
	 * @throws ScopeException 
	 */
	public List<Space> S() throws ScopeException {
		ArrayList<Space> spaces = new ArrayList<Space>();
		for (Domain domain : getDomains()) {
			spaces.add(S(domain));
		}
		return spaces;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this==obj) {
			return true;
		} else if (obj instanceof Universe) {
			if (this.getProject().equals(((Universe)obj).getProject())) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		return getProject().hashCode();
	}
	
    public DomainHierarchy getDomainHierarchy(Domain domain, boolean lazy) throws ComputingException, InterruptedException {
        return DomainHierarchyManager.INSTANCE.getHierarchy(getProject().getId(), domain, lazy);
    }
	
	/**
	 * parse a string into a Space using expressions
	 * @param string
	 * @return
	 * @throws ScopeException 
	 */
	public Space space(String expression) throws ScopeException {
		UniverseScope scope = new UniverseScope(this);
		ExpressionAST expr = scope.parseExpression(expression);
		if (expr instanceof SpaceExpression) {
			return ((SpaceExpression)expr).getSpace();
		} else {
			throw new ScopeException("the expression doesn't resolve to a space");
		}
	}

	/*
	public DimensionHierarchy getHierarchy(Domain domain) {
		return gb.getHierarchy(domain);
	}
	*/

	public Measure measure(String expression) throws ScopeException {
		UniverseScope scope = new UniverseScope(this);
		ExpressionAST expr = scope.parseExpression(expression);
		//
		IDomain source = expr.getSourceDomain();
		Space parent = null;
		if (source.isInstanceOf(DomainDomain.DOMAIN)) {
		    DomainDomain type = (DomainDomain)source;
		    Domain domain = type.getDomain();
		    parent = S(domain);// check if the universe can create this domain
		} else {
            throw new ScopeException("Invalid expression: source domain must be a Space, was " + source.getName());
        }
		if (!expr.getImageDomain().isInstanceOf(IDomain.AGGREGATE)) {
		    throw new ScopeException("Invalid expression: this is not a measure, must be an aggregation");
		}
		if (expr instanceof MeasureExpression) {
			// no need to wrap once again
			return ((MeasureExpression)expr).getMeasure();
		} else {
			Measure measure = new Measure(parent, expr);
			return measure;
		}
	}
	
	public Axis axis(String expression) throws ScopeException {
		UniverseScope scope = new UniverseScope(this);
		ExpressionAST expr = scope.parseExpression(expression);
		Axis axis = asAxis(expr);
		if (axis==null) {
			throw new ScopeException("the expression '" + expression + "' doesn't resolve to an axis");
		} else {
			return axis;
		}
	}

	/**
	 * create an axis based on a Dimension PK
	 * @param facetPK
	 * @return
	 * @throws ScopeException 
	 * @throws InterruptedException 
	 * @throws ComputingException 
	 */
	public Axis axis(DimensionPK dimensionPK) throws ScopeException, ComputingException, InterruptedException {
		Domain domain = ProjectManager.INSTANCE.getDomain(getContext(), dimensionPK.getParent());
		DomainHierarchy hierarchy = DomainHierarchyManager.INSTANCE.getHierarchy(getProject().getId(), domain, false);
		Dimension dimension = hierarchy.getDimension(getContext(), dimensionPK);
		return S(domain).A(dimension);
	}
	
	/**
     * try to convert the expression as an axis or return null if it is not possible.
     * 
     * @return
	 * @throws ScopeException 
     */
	public Axis asAxis(ExpressionAST expression) throws ScopeException {
		if (expression instanceof AxisExpression) {
			return ((AxisExpression)expression).getAxis();
		} else {
		    IDomain source = expression.getSourceDomain();
		    Object adapter = source.getAdapter(Domain.class);
		    if (adapter!=null && adapter instanceof Domain) {
		        Domain domain = (Domain)adapter;
		        IDomain image = expression.getImageDomain();
		        if (image.isInstanceOf(IDomain.INTRINSIC)) {
		            return new Axis(S(domain), expression);
		        }
		    }
		}
		// else
		return null;
	}
	
	/**
     * try to convert the expression as an measure or return null if it is not possible.
     * 
     * @return
	 * @throws ScopeException 
     */
	public Measure asMeasure(ExpressionAST expression) throws ScopeException {
		if (expression instanceof MeasureExpression) {
			return ((MeasureExpression)expression).getMeasure();
		} else {
		    IDomain source = expression.getSourceDomain();
		    Object adapter = source.getAdapter(Domain.class);
		    if (adapter!=null && adapter instanceof Domain) {
		        Domain domain = (Domain)adapter;
		        IDomain image = expression.getImageDomain();
		        if (image.isInstanceOf(IDomain.INTRINSIC)) {
		            return new Measure(S(domain), expression);
		        }
		    }
		}
		// else
		return null;
	}
	
	public ExpressionAST expression(String expression) throws ScopeException {
		UniverseScope scope = new UniverseScope(this);
		ExpressionAST expr = scope.parseExpression(expression);
		return expr;
	}

    public String getTableUUID(Table table) {
        if (table!=null) {
            return getProject().getId().toUUID()+"/DB/"+table.getSchema()+"."+table.getName();
        } else {
            return null;
        }
    }

}
