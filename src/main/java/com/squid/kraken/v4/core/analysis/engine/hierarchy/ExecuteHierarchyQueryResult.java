package com.squid.kraken.v4.core.analysis.engine.hierarchy;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.model.DimensionPK;

public class ExecuteHierarchyQueryResult {

	static final Logger logger = LoggerFactory.getLogger(ExecuteHierarchyQueryResult.class);

	public HashMap<DimensionPK, String> lastIndexedDimension;
	public HashMap<DimensionPK, String> lastIndexedCorrelation;

	public ExecuteHierarchyQueryResult() {
		this.lastIndexedDimension = new HashMap<DimensionPK, String>();
		this.lastIndexedCorrelation = new HashMap<DimensionPK, String>();
	}

	
	public void waitForIndexationCompletion(DimensionIndex di, int timeOutInSec) throws InterruptedException {
		logger.info ("wait for indexation");
		
		Thread.sleep(timeOutInSec*1000);
		
		if (!lastIndexedDimension.containsKey(di.getDimension().getId())
				&& !lastIndexedCorrelation.containsKey(di.getDimension().getId())) {
			// handle empty index
			di.setDone();
		}else{
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
