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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Store the list of suggested definitions for expression editor.
 */
@SuppressWarnings("serial")
public class ExpressionSuggestion implements Serializable {
    
    /**
     * Definition list.
     */
    private List<ExpressionSuggestionItem> suggestions = Collections.emptyList();// default initialize with an empty list
    
    /**
     * Definition list.
     */
    private List<String> definitions = Collections.emptyList();// default initialize with an empty list
    
    /**
     * Validate message of expression editor.
     */
    private String validateMessage;
    
    /**
     * Position of filter key.
     */
    @Deprecated
    private int filterIndex;
    
    private int beginInsertPos;
    private int endInsertPos;
    
    /**
     * Filter key.
     */
    private String filter;
    
    /**
     * the value type of the expression
     */
    private ValueType valueType;
    
    /**
     * Constructor
     */
    public ExpressionSuggestion() {
    }
    
    /**
     * error constructor
     * @param e
     */
    public ExpressionSuggestion(Exception e) {
    	this.validateMessage = e.getLocalizedMessage();
    }
    
    /**
     * @return validated message
     */
    public String getValidateMessage() {
        return validateMessage;
    }
    
    /**
     * @param validateMessage
     */
    public void setValidateMessage(String validateMessage) {
        this.validateMessage = validateMessage;
    }
    
    public List<ExpressionSuggestionItem> getSuggestions() {
		return suggestions;
	}
    
    public void setSuggestions(List<ExpressionSuggestionItem> suggestions) {
		this.suggestions = suggestions;
	}
    
    /**
     * @return suggested definitions
     */
    public List<String> getDefinitions() {
        return definitions;
    }
    
    /**
     * @param definitions
     */
    @Deprecated
    public void setDefinitions(List<String> definitions) {
        this.definitions = definitions;
    }

    /**
     * @return
     */
    public String getFilter() {
        return filter;
    }

    /**
     * @param filterIndex
     */
    @Deprecated
    public void setFilterIndex(int filterIndex) {
        this.filterIndex = filterIndex;
    }

    /**
     * @return
     */
    @Deprecated
    public int getFilterIndex() {
        return filterIndex;
    }
    
    public void setInsertRange(int begin, int end) {
    	this.beginInsertPos = begin;
    	this.endInsertPos = end;
    }
    
    public int getBeginInsertPos() {
		return beginInsertPos;
	}
    
    public int getEndInsertPos() {
		return endInsertPos;
	}

    /**
     * @param filter
     */
    public void setFilter(String filter) {
        this.filter = filter;
    }
    
    /**
	 * @return the valueType
	 */
	public ValueType getValueType() {
		return valueType;
	}

	/**
	 * @param valueType the valueType to set
	 */
	public void setValueType(ValueType valueType) {
		this.valueType = valueType;
	}
    
}
