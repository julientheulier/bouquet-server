package com.squid.kraken.v4.api.core.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.persistence.DataStoreEvent;
import com.squid.kraken.v4.persistence.DataStoreEventObserver;

/**
 * Observes the Meta-Model and logs received events.
 */
public class LogMetaModelObserver implements DataStoreEventObserver {

    private static final Logger logger = LoggerFactory.getLogger(LogMetaModelObserver.class);

    private static LogMetaModelObserver instance;

    public static synchronized LogMetaModelObserver getInstance() {
        if (instance == null) {
            instance = new LogMetaModelObserver();
        }
        return instance;
    }

    private LogMetaModelObserver() {
    }

    @Override
    public void notifyEvent(DataStoreEvent event) {
        logger.info(event.toString());
    }

}
