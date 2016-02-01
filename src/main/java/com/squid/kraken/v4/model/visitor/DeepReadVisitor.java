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
package com.squid.kraken.v4.model.visitor;

import java.util.List;

import com.google.common.base.Optional;
import com.squid.kraken.v4.model.Attribute;
import com.squid.kraken.v4.model.Bookmark;
import com.squid.kraken.v4.model.Client;
import com.squid.kraken.v4.model.Customer;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.Relation;
import com.squid.kraken.v4.model.Shortcut;
import com.squid.kraken.v4.model.State;
import com.squid.kraken.v4.model.StatePK;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserGroup;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.AttributeDAO;
import com.squid.kraken.v4.persistence.dao.BookmarkDAO;
import com.squid.kraken.v4.persistence.dao.ClientDAO;
import com.squid.kraken.v4.persistence.dao.DimensionDAO;
import com.squid.kraken.v4.persistence.dao.DomainDAO;
import com.squid.kraken.v4.persistence.dao.MetricDAO;
import com.squid.kraken.v4.persistence.dao.ProjectDAO;
import com.squid.kraken.v4.persistence.dao.RelationDAO;
import com.squid.kraken.v4.persistence.dao.ShortcutDAO;
import com.squid.kraken.v4.persistence.dao.StateDAO;
import com.squid.kraken.v4.persistence.dao.UserDAO;
import com.squid.kraken.v4.persistence.dao.UserGroupDAO;

/**
 * A visitor to handle objects connections initialization.
 */
public class DeepReadVisitor extends AbstractModelVisitor {

	public DeepReadVisitor(AppContext ctx) {
		super(ctx);
	}

	@Override
	public void visit(Persistent<?> object) {
		// init the collections
		if (object instanceof Project) {
			visitInstance((Project) object);
		} else if (object instanceof Domain) {
			visitInstance((Domain) object);
		} else if (object instanceof Dimension) {
			visitInstance((Dimension) object);
		} else if (object instanceof Customer) {
			visitInstance((Customer) object);
		} else if (object instanceof Shortcut) {
			visitInstance((Shortcut) object);
		}
	}

	private void visitInstance(Project instance) {
		List<Domain> domains = ((DomainDAO) DAOFactory.getDAOFactory().getDAO(
				Domain.class)).findByProject(ctx, instance.getId());
		instance.setDomains(domains);

		List<Relation> relations = ((RelationDAO) DAOFactory.getDAOFactory()
				.getDAO(Relation.class)).findByProject(ctx, instance.getId());
		instance.setRelations(relations);
		
		List<Bookmark> bookmarks = ((BookmarkDAO) DAOFactory.getDAOFactory()
				.getDAO(Bookmark.class)).findByParent(ctx, instance.getId());
		instance.setBookmarks(bookmarks);
	}

	private void visitInstance(Domain object) {
		List<Dimension> dimensions = ((DimensionDAO) DAOFactory.getDAOFactory()
				.getDAO(Dimension.class)).findByDomain(ctx, object.getId());
		object.setDimensions(dimensions);
		List<Metric> metrics = ((MetricDAO) DAOFactory.getDAOFactory().getDAO(
				Metric.class)).findByDomain(ctx, object.getId());
		object.setMetrics(metrics);
	}

	private void visitInstance(Dimension object) {
		List<Attribute> attributes = ((AttributeDAO) DAOFactory.getDAOFactory()
				.getDAO(Attribute.class)).findByDimension(ctx, object.getId());
		object.setAttributes(attributes);
	}

	private void visitInstance(Customer object) {

		List<UserGroup> userGroups = ((UserGroupDAO) DAOFactory.getDAOFactory()
				.getDAO(UserGroup.class)).findByCustomer(ctx, object.getId());
		object.setUserGroups(userGroups);

		List<Client> clients = ((ClientDAO) DAOFactory.getDAOFactory().getDAO(
				Client.class)).findAll(ctx);
		object.setClients(clients);

		List<Project> projects = ((ProjectDAO) DAOFactory.getDAOFactory()
				.getDAO(Project.class)).findByCustomer(ctx, object.getId());
		object.setProjects(projects);

		List<User> users = ((UserDAO) DAOFactory.getDAOFactory().getDAO(
				User.class)).findByCustomer(ctx, object.getId());
		object.setUsers(users);
		
		List<Shortcut> shortcuts = ((ShortcutDAO) DAOFactory.getDAOFactory().getDAO(
				Shortcut.class)).findByParent(ctx, object.getId());
		object.setShortcuts(shortcuts);
		
		List<State> states = ((StateDAO) DAOFactory.getDAOFactory().getDAO(
				State.class)).findByParent(ctx, object.getId());
		object.setStates(states);
	}

	private void visitInstance(Shortcut object) {
		if (object.getStateId() != null) {
			Optional<State> read = ((StateDAO) DAOFactory.getDAOFactory()
					.getDAO(State.class)).read(ctx,
					new StatePK(object.getCustomerId(), object.getStateId()));
			// some shortcuts may be out of sync
			if (read.isPresent()) {
				object.setState(read.get());
			}
		}
	}
}
