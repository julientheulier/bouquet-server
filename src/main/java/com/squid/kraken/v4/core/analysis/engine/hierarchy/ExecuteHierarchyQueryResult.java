package com.squid.kraken.v4.core.analysis.engine.hierarchy;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex.Status;
import com.squid.kraken.v4.model.DimensionPK;

public class ExecuteHierarchyQueryResult {

	static final Logger logger = LoggerFactory.getLogger(ExecuteHierarchyQueryResult.class);

	public HashMap<DimensionPK, String> lastIndexedDimension;
	public HashMap<DimensionPK, String> lastIndexedCorrelation;

	public ExecuteHierarchyQueryResult(HashMap<DimensionIndex, String> dimensions,
			HashMap<DimensionIndex, String> correlations) {
		this.lastIndexedDimension = new HashMap<DimensionPK, String>();
		for (DimensionIndex di : dimensions.keySet()) {
			this.lastIndexedDimension.put(di.getDimension().getId(), dimensions.get(di));
		}

		this.lastIndexedCorrelation = new HashMap<DimensionPK, String>();
		for (DimensionIndex di : correlations.keySet()) {
			this.lastIndexedCorrelation.put(di.getDimension().getId(), correlations.get(di));
		}
	}

	public void waitForIndexationCompletion(DimensionIndex di, int timeOutInSec) throws InterruptedException {
		logger.info("Checking indexation completion for index " + di.getDimensionName());

		synchronized (di) {
			if (di.getStatus() != Status.DONE) {
				Thread.sleep(timeOutInSec * 1000);

				if (!lastIndexedDimension.containsKey(di.getDimension().getId())
						&& !lastIndexedCorrelation.containsKey(di.getDimension().getId())) {
					// handle empty index
					di.setDone();
				} else {
					if (!di.isCorrelationIndexationDone(lastIndexedCorrelation.get(di.getDimension().getId()))) {
						logger.info("timeout during correlation indexing " + di.getDimensionName());
						di.setPermanentError("timeout during correlation indexing");
					}

					if (di.isDimensionIndexationDone(lastIndexedDimension.get(di.getDimension().getId()))) {
						logger.info("indexing  ok " + di.getDimensionName());
						di.setDone();
					} else {
						logger.info("timeout during  indexing " + di.getDimensionName());
						di.setPermanentError("timeout during indexing");
					}
				}

			}
		}
	}
}
