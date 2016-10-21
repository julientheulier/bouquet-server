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

import java.util.ArrayList;
import java.util.List;

import com.squid.kraken.v4.model.ProjectAnalysisJob.OrderBy;

/**
 * pojo that describes a analysis result dataset
 * @author sergefantino
 *
 */
public class DataHeader {

    static public enum Role {
        GROUPBY, METRIC, OTHER
    };

    static public enum DataType {
        STRING, NUMBER, DATE
    };
	
	public static final class Column {
		
		private String name;
		
		private String description;
		
		/**
		 * this is the column definition in the original analysis scope
		 */
		private String definition;
		
		private DataType dataType;
		
		private String format;
		
		/**
		 * true if the format is already applied
		 */
		private boolean isFormatted;

		/**
		 * the column position in the header
		 */
        private int pos;
        
        /**
         * the column role: groupBy (coming from the groupBy spec), metric (coming from the metric spec), or other if this is derived from specs
         */
        private Role role;
        
        /**
         * if the column is ordered, orderByPos>=0 and define the column order in the sort
         */
        private int orderByPos;
        
        private OrderBy orderByDirection;
        
        /**
		 * 
		 */
		public Column() {
			// TODO Auto-generated constructor stub
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getDescription() {
			return description;
		}

		public void setDescription(String description) {
			this.description = description;
		}

		public String getDefinition() {
			return definition;
		}

		public void setDefinition(String definition) {
			this.definition = definition;
		}

		public DataType getDataType() {
			return dataType;
		}

		public void setDataType(DataType dataType) {
			this.dataType = dataType;
		}

		public String getFormat() {
			return format;
		}

		public void setFormat(String format) {
			this.format = format;
		}

		public boolean isFormatted() {
			return isFormatted;
		}

		public void setFormatted(boolean isFormatted) {
			this.isFormatted = isFormatted;
		}

		public int getPos() {
			return pos;
		}

		public void setPos(int pos) {
			this.pos = pos;
		}

		public Role getRole() {
			return role;
		}

		public void setRole(Role role) {
			this.role = role;
		}

		public int getOrderByPos() {
			return orderByPos;
		}

		public void setOrderByPos(int orderByPos) {
			this.orderByPos = orderByPos;
		}

		public OrderBy getOrderByDirection() {
			return orderByDirection;
		}

		public void setOrderByDirection(OrderBy orderByDirection) {
			this.orderByDirection = orderByDirection;
		}
		
	}
	
	private List<Column> columns = new ArrayList<>();
	
	/**
	 * 
	 */
	public DataHeader() {
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * @return the columns
	 */
	public List<Column> getColumns() {
		return columns;
	}
	
	/**
	 * @param columns the columns to set
	 */
	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

}
