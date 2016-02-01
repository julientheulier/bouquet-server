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
package com.squid.kraken.v4.api.core.customer;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.GenericServiceImpl;
import com.squid.kraken.v4.api.core.ModelGC;
import com.squid.kraken.v4.model.State;
import com.squid.kraken.v4.model.StatePK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.StateDAO;

public class StateServiceBaseImpl extends GenericServiceImpl<State, StatePK> {

	private static final Logger logger = LoggerFactory
			.getLogger(StateServiceBaseImpl.class);

	private static StateServiceBaseImpl instance;

	private ScheduledExecutorService modelGC;

	private ScheduledFuture<?> modelGCThread;

	public static StateServiceBaseImpl getInstance() {
		if (instance == null) {
			instance = new StateServiceBaseImpl();
		}
		return instance;
	}

	private StateServiceBaseImpl() {
		// made private for singleton access
		super(State.class);
	}

	public void initGC(int maxAgeInSeconds) {
		modelGC = Executors.newSingleThreadScheduledExecutor();
		ModelGC<State, StatePK> gc = new ModelGC<State, StatePK>(
				maxAgeInSeconds, this, State.class);
		modelGCThread = modelGC
				.scheduleWithFixedDelay(gc, 0, 1, TimeUnit.HOURS);
	}

	public void stopGC() {
		try {
			logger.info("stopping GC scheduler for "
					+ this.getClass().getName());
			if (modelGCThread != null) {
				modelGCThread.cancel(true);
			}

			if (modelGC != null) {
				modelGC.shutdown();
				modelGC.awaitTermination(2, TimeUnit.SECONDS);
				modelGC.shutdownNow();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Prevent from deleting a state if associated to a shortcut
	 */
	@Override
	public boolean delete(AppContext ctx, StatePK objectId) {
		State state = read(ctx, objectId);
		if (state.getShortcutId() == null) {
			return super.delete(ctx, objectId);
		} else {
			throw new APIException(
					"Forbidden, this State is already associated to Shortcut : "
							+ state.getShortcutId(), ctx.isNoError());
		}
	}
	
	public List<State> readAll(AppContext ctx) {
		return ((StateDAO) DAOFactory.getDAOFactory().getDAO(State.class)).findByParent(ctx, ctx.getCustomerPk());
	}

}
