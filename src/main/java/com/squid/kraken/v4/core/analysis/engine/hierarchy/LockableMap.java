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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A map that can lock access by Key
 * @author sergefantino
 *
 */
public class LockableMap<KEY, VALUE>  extends ConcurrentHashMap<KEY, VALUE> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7681004128199344324L;

	private ConcurrentHashMap<KEY, ReentrantLock> domain_locks = new ConcurrentHashMap<>();

	public LockableMap() {
		super();
		// TODO Auto-generated constructor stub
	}

	public LockableMap(int initialCapacity, float loadFactor,
			int concurrencyLevel) {
		super(initialCapacity, loadFactor, concurrencyLevel);
		// TODO Auto-generated constructor stub
	}

	public LockableMap(int initialCapacity, float loadFactor) {
		super(initialCapacity, loadFactor);
		// TODO Auto-generated constructor stub
	}

	public LockableMap(int initialCapacity) {
		super(initialCapacity);
		// TODO Auto-generated constructor stub
	}

	public LockableMap(
			Map<? extends KEY, ? extends VALUE> m) {
		super(m);
		// TODO Auto-generated constructor stub
	}
	
	public ReentrantLock lock(KEY key) {
		try {
			return lock(key, 0);
		} catch (InterruptedException e) {
			// we never get there
			throw new CyclicDependencyException("Cyclic dependency detected", e);
		}
	}
	
	public ReentrantLock lock(KEY key, int timeoutMs) throws InterruptedException {
		ReentrantLock mylock = new ReentrantLock();
		mylock.lock();
		ReentrantLock lock = domain_locks.putIfAbsent(key, mylock);
		if (lock==null) {
			lock = mylock;// I already own the lock
		} else {
			if (lock.getHoldCount()>0) throw new InterruptedException("cyclic");
			if (timeoutMs > 0) {
				lock.tryLock(timeoutMs, TimeUnit.MILLISECONDS);
			} else {
				lock.lock();
			}
		}
		return lock;
	}
	
}
