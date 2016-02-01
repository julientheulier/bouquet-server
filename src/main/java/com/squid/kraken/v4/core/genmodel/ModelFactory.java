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
package com.squid.kraken.v4.core.genmodel;

import java.util.ArrayList;
import java.util.Arrays;

import org.bson.types.ObjectId;

import com.squid.core.expression.reference.Cardinality;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.api.core.attribute.AttributeServiceBaseImpl;
import com.squid.kraken.v4.api.core.client.ClientServiceBaseImpl;
import com.squid.kraken.v4.api.core.customer.CustomerServiceBaseImpl;
import com.squid.kraken.v4.api.core.dimension.DimensionServiceBaseImpl;
import com.squid.kraken.v4.api.core.domain.DomainServiceBaseImpl;
import com.squid.kraken.v4.api.core.metric.MetricServiceBaseImpl;
import com.squid.kraken.v4.api.core.project.ProjectServiceBaseImpl;
import com.squid.kraken.v4.api.core.relation.RelationServiceBaseImpl;
import com.squid.kraken.v4.api.core.user.UserServiceBaseImpl;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Attribute;
import com.squid.kraken.v4.model.AttributePK;
import com.squid.kraken.v4.model.Client;
import com.squid.kraken.v4.model.ClientPK;
import com.squid.kraken.v4.model.Customer;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Dimension.Type;
import com.squid.kraken.v4.model.DimensionPK;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.Expression;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.MetricPK;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.Relation;
import com.squid.kraken.v4.model.RelationPK;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserPK;
import com.squid.kraken.v4.persistence.AppContext;

/**
 * The ModelFactory provides utility methods to create model instances.
 * The factory can also be used to initialize a AppContext with a new customer/user login
 * @author sfantino
 *
 */
public class ModelFactory {

    private CustomerServiceBaseImpl customerService = CustomerServiceBaseImpl.getInstance();
    private UserServiceBaseImpl userService = UserServiceBaseImpl.getInstance();

	private AppContext ctx;
	private Customer customer;
	
	/**
	 * Create a factory using an existing AppContext and Customer. The Customer will own the created objects
	 * @param ctx
	 * @param customer
	 */
	public ModelFactory(AppContext ctx, Customer customer) {
		this.ctx = ctx;
		this.customer = customer;
	}
	
	public ModelFactory(String customerName, String user, String login) {
		initContext(customerName, user, login);
	}
	
	protected void initContext(String customerName, String user, String login) {
        customer = new Customer(customerName);
        AppContext rootctx = ServiceUtils.getInstance().getRootUserContext(customer.getCustomerId());
        customer = customerService.store(rootctx, customer);
        String customerId = customer.getCustomerId();
        
        // add a owner
        User owner = new User(new UserPK(customer.getCustomerId()), "super", "squ1d4you");
        owner.setEmail("super@mail.com");
        userService.store(rootctx, owner);
        AccessRight ownerRight = new AccessRight(Role.OWNER, owner.getId().getUserId(), null);
        customer.getAccessRights().add(ownerRight);
        customerService.store(rootctx, customer);
        AppContext ownerCtx = new AppContext.Builder(customerId, owner).build();
        
        // add a write user which will be our project owner
        User writeUser = new User(new UserPK(customer.getCustomerId()), "user1", "user14you");
        writeUser.setEmail("user1@mail.com");
        userService.store(ownerCtx, writeUser);
        customer.getAccessRights().add(new AccessRight(Role.WRITE, writeUser.getId().getUserId(), null));
        
        // add a write user which will be allowed to create its own project (but read others)
        User writeUser2 = new User(new UserPK(customer.getCustomerId()), "user", "user4you");
        writeUser2.setEmail("userxxx@mail.com");
        userService.store(ownerCtx, writeUser2);
        customer.getAccessRights().add(new AccessRight(Role.READ, writeUser2.getId().getUserId(), null));
        
        // add a read user
        User readUser = new User(new UserPK(customer.getCustomerId()), "guest", "guest4you");
        readUser.setEmail("guest@mail.com");
        userService.store(ownerCtx, readUser);
        customer.getAccessRights().add(new AccessRight(Role.READ, readUser.getId().getUserId(), null));
        
        ClientPK clientPK = new ClientPK(customer.getCustomerId(), "admin_console");
        Client client = new Client(clientPK,"squid","secret",new ArrayList<String>());
        client = ClientServiceBaseImpl.getInstance().store(ownerCtx, client);
        
        customerService.store(ownerCtx, customer);
        
        System.out.println("customerId : " + customerId + " clientId : " + client.getId().getClientId());

        ctx = new AppContext.Builder(customerId, writeUser).build();
	}
	
	public Customer getCustomer() {
		return customer;
	}

	public AppContext getAppContext() {
		return ctx;
	}

	public ModelFactory(AppContext ctx) {
		this.ctx = ctx;
	}
	
	public String getNewID() {
		return new ObjectId().toString();
	}
	
	public String getNewID(String name) {
		if (ServiceUtils.getInstance().isValidId(name)) {
			return name.toLowerCase(); 
		} else {
			return new ObjectId().toString();
		}
	}
	
	public Project Project(String name, String dbUrl, String dbUser, String dbPassword, String[] dbSchemas) {
		ProjectPK pk = new ProjectPK(ctx.getCustomerId(), getNewID(name));
		Project project = new Project(pk, name);
		project.setDbUrl(dbUrl);
		project.setDbUser(dbUser);
		project.setDbPassword(dbPassword);
		project.setDbSchemas(Arrays.asList(dbSchemas));
		project = ProjectServiceBaseImpl.getInstance().store(ctx, project);
		return project;
	}

	/**
	 * @deprecated
	 * @param name
	 * @param expression
	 * @return
	 */
	public Project Project(String name, Expression expression) {
		ProjectPK pk = new ProjectPK(ctx.getCustomerId(), getNewID());
		Project project = new Project(pk, name);
		project = ProjectServiceBaseImpl.getInstance().store(ctx, project);
		return project;
	}

	public Domain Domain(Project project, String name, Expression subject) {
		Domain domain = new Domain(new DomainPK(project.getId(), getNewID(name)), name, subject);
		DomainServiceBaseImpl.getInstance().store(ctx, domain);
		return domain;
	}

	public Dimension Dimension(Domain parent, String name, Type type, Expression expression) {
		DomainPK pk = parent.getId();
		Dimension dimension = new Dimension(new DimensionPK(pk, getNewID(name)), name, type, expression);
		DimensionServiceBaseImpl.getInstance().store(ctx, dimension);
		return dimension;
	}

	public Dimension Dimension(Dimension parent, String name, Type type, Expression expression) {
		DimensionPK pk = parent.getId();
		Dimension dimension = new Dimension(new DimensionPK(pk.getCustomerId(), pk.getProjectId(), pk.getDomainId(), getNewID(name)), name, type, expression);
		dimension.setParentId(parent.getId());
		DimensionServiceBaseImpl.getInstance().store(ctx, dimension);
		return dimension;
	}

	public Attribute Attribute(Dimension parent, String name, Expression expression) {
		DimensionPK pk = parent.getId();
		Attribute attribute = new Attribute(new AttributePK(pk.getCustomerId(), pk.getProjectId(), pk.getDomainId(), pk.getDimensionId(), getNewID()), name, expression);
		return AttributeServiceBaseImpl.getInstance().store(ctx, attribute);
	}

	public Metric Metric(Domain parent, String name, Expression expression) {
		DomainPK pk = parent.getId();
		Metric metric = new Metric(new MetricPK(pk.getCustomerId(), pk.getProjectId(), pk.getDomainId(), getNewID(name)), name, expression);
		MetricServiceBaseImpl.getInstance().store(ctx, metric);
		return metric;
	}

	public void update(Metric metric) {
		MetricServiceBaseImpl.getInstance().store(ctx, metric);
	}

	public Relation Relation(Project project, String leftName, Domain left, Cardinality leftCardinality,
			String rightName, Domain right, Cardinality rightCardinality, Expression join) {
		RelationPK pk = new RelationPK(project.getId(), getNewID());
		Relation relation = new Relation(pk, left.getId(), leftCardinality, right.getId(), rightCardinality, leftName, rightName, join);
		RelationServiceBaseImpl.getInstance().store(ctx, relation);
		return relation;
	}
	
	/**
	 * create an user as owner of the givent project
	 * @param project
	 * @param string
	 * @param string2
	 * @return
	 */
	public User User(Project project, String name, String login) {
        // add a owner
        User owner = new User(new UserPK(ctx.getCustomerId()), name, login);
        UserServiceBaseImpl.getInstance().store(ctx, owner);
        AccessRight ownerRight = new AccessRight(Role.OWNER, owner.getId().getUserId(), null);
        customer.getAccessRights().add(ownerRight);
        AppContext ownerCtx = new AppContext.Builder(ctx.getCustomerId(), owner).build();
        CustomerServiceBaseImpl.getInstance().store(ownerCtx, customer);
        return owner;
	}

}
