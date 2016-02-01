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

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.persistence.AppContext;

public abstract class ReverseModelVisitor implements ModelVisitor {

	private static final Logger logger = LoggerFactory
			.getLogger(ReverseModelVisitor.class);

	protected AppContext ctx;

	private Deque<Persistent<?>> queue;

	public ReverseModelVisitor(AppContext ctx) {
		super();
		this.ctx = ctx;
		this.queue = new LinkedList<Persistent<?>>();
	}

	@Override
	public void visit(Persistent<?> object) {
		queue.add(object);
	}

	public abstract void visitElement(Persistent<?> object);

	@Override
	public AppContext getContext() {
		return ctx;
	}

	public void commit() {
		if(logger.isDebugEnabled()){logger.debug(("Queue size : " + queue.size()));}
		Iterator<Persistent<?>> descendingIterator = queue.descendingIterator();
		while (descendingIterator.hasNext()) {
			this.visitElement(descendingIterator.next());
		}
	}

}
