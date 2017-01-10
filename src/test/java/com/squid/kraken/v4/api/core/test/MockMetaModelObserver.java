package com.squid.kraken.v4.api.core.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.squid.kraken.v4.persistence.DataStoreEvent;
import com.squid.kraken.v4.persistence.DataStoreEventObserver;

/**
 * Observes the Meta-Model and store received events. (used by unit tests)
 */
public class MockMetaModelObserver implements DataStoreEventObserver {

	private List<DataStoreEvent> events;

	public MockMetaModelObserver() {
		events = Collections.synchronizedList(new ArrayList<DataStoreEvent>());
	}

	@Override
	public void notifyEvent(DataStoreEvent event) {
		events.add(event);
	}

	public List<DataStoreEvent> getEvents() {
		return events;
	}

}
