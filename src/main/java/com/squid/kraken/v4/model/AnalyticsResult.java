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
package com.squid.kraken.v4.model;

/**
 * @author sergefantino
 *
 */
public class AnalyticsResult {

	public static final class Info {
		
		private boolean fromCache = false;
		
		private boolean fromSmartCache = false;
		
		private String executionDate;
		
		private Integer startIndex = null;
		
		private Integer pageSize = null;
		
		private int totalSize = 0;
		
		private boolean complete = false;
		
		public Info() {
		}

		public boolean isFromCache() {
			return fromCache;
		}

		public void setFromCache(boolean fromCache) {
			this.fromCache = fromCache;
		}

		public boolean isFromSmartCache() {
			return fromSmartCache;
		}

		public void setFromSmartCache(boolean fromSmartCache) {
			this.fromSmartCache = fromSmartCache;
		}

		public String getExecutionDate() {
			return executionDate;
		}

		public void setExecutionDate(String executionDate) {
			this.executionDate = executionDate;
		}

		public Integer getStartIndex() {
			return startIndex;
		}

		public void setStartIndex(Integer startIndex) {
			this.startIndex = startIndex;
		}

		public Integer getPageSize() {
			return pageSize;
		}

		public void setPageSize(Integer pageSize) {
			this.pageSize = pageSize;
		}

		public int getTotalSize() {
			return totalSize;
		}

		public void setTotalSize(int totalSize) {
			this.totalSize = totalSize;
		}

		public boolean isComplete() {
			return complete;
		}

		public void setComplete(boolean complete) {
			this.complete = complete;
		}

	}
	
	private Object header = null;
	
	private Object data = null;
	
	private Info info = null;

	public Object getHeader() {
		return header;
	}

	public void setHeader(Object header) {
		this.header = header;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}

	public Info getInfo() {
		return info;
	}

	public void setInfo(Info info) {
		this.info = info;
	}

}
