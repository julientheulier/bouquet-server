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

// Inspired from google's DataTables (http://code.google.com/apis/chart/interactive/docs/roles.html))

import java.io.Serializable; 
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.squid.core.domain.operators.ExtendedType;
import com.squid.kraken.v4.api.core.JobResultBaseImpl;

/**
 * Result matrix.
 */
@SuppressWarnings("serial")
@XmlType(namespace = "http://model.v4.kraken.squid.com")
@XmlRootElement
public class DataTable extends JobResultBaseImpl {

    private List<Col> cols;

    private List<Row> rows;

    private long size;
    
    private String objectType = "DataTable";
    
    private boolean fromCache = false;// true if the data comes from the cache
    
    private Date executionDate = null;

    private boolean fullset = true; //true if the whole dataset fits in  REDIS cache  (ie : number of row < LIMIT)
    
    public DataTable() {
        super();
        cols = new ArrayList<Col>();
        rows = new ArrayList<Row>();
    }

    public DataTable(List<Col> cols, List<Row> rows) {
        super();
        this.cols = cols;
        this.rows = rows;
    }

    public List<Col> getCols() {
        return cols;
    }

    public void setCols(List<Col> cols) {
        this.cols = cols;
    }

    public List<Row> getRows() {
        return rows;
    }

    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    static public class Col implements Serializable {

        static public enum Role {
            DOMAIN, DATA
        };

        static public enum DataType {
            STRING, NUMBER, DATE
        };

        private GenericPK pk;
		private String name;
		private String definition;
        private ExtendedType extendedType;
        private Role role;

        public Col() {
        }

        @Deprecated
        public Col(GenericPK pk, String name, DataType dataType, Role role) {
            super();
            this.pk = pk;
            this.name = name;
            this.role = role;
        }

        @Deprecated
        public Col(GenericPK pk, String name, DataType dataType, ExtendedType extendedType, Role role) {
            super();
            this.pk = pk;
            this.name = name;
            this.extendedType = extendedType;
            this.role = role;
        }

        public Col(GenericPK pk, String name, ExtendedType extendedType, Role role) {
            super();
            this.pk = pk;
            this.name = name;
            this.extendedType = extendedType;
            this.role = role;
        }

        public String getId() {
            return pk!=null?pk.getObjectId():null;
        }
        
        public GenericPK getPk() {
			return pk;
		}

		public void setPk(GenericPK pk) {
			this.pk = pk;
		}

        /**
         * Column's data type (string, number, date)
         */
        public ExtendedType getExtendedType() {
            return extendedType;
        }

        /**
         * The column's role (domain, data, annotation ...)
         */
        public Role getRole() {
            return role;
        }

        public void setRole(Role role) {
            this.role = role;
        }

        /**
         * Column's name.
         */
        public String getName() {
            return name;
        }
        
        /**
         * Column's localized name.
         */
        @Deprecated
        public String getLname() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
        
        public void setDefinition(String definition) {
            this.definition = definition;
        }
        
        public String getDefinition() {
            return definition;
        }

        @Override
        public String toString() {
            return "Col [pk=" + pk + ", role=" + role + ", extendedType=" + extendedType +"]";
        }

    }

    /**
     * Row is a list of values.<br>
     * Note : Null values are stored as <code>null</code>.
     */
    static public class Row implements Serializable {

        private String[] v; // values

        public Row() {
        }

        public Row(String[] v) {
            super();
            this.v = v;
        }

        public String[] getV() {
            return v;
        }

        public void setV(String[] v) {
            this.v = v;
        }

        @Override
        public String toString() {
            return "Row [values=" + Arrays.toString(v) + "]";
        }

    }

    @Override
    public long getTotalSize() {
        return size;
    }

    public void setTotalSize(long size) {
        this.size = size;
    }
    
    public String getObjectType() {
        return objectType;
    }
    
    public void setObjectType(String objectType) {
        this.objectType = objectType;
    }
    
    public boolean isFromCache() {
        return fromCache;
    }
    
    public void setFromCache(boolean fromCache) {
        this.fromCache = fromCache;
    }
    
    public Date getExecutionDate() {
        return executionDate;
    }
    
    public void setExecutionDate(Date executionDate) {
        this.executionDate = executionDate;
    }

	public boolean getFullset() {
		return this.fullset;
	}

	public void setFullset(boolean fullset) {
		this.fullset = fullset;
	}

}
