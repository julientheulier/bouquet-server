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
 * define a piece of suggestion
 * @author sergefantino
 *
 */
public class ExpressionSuggestionItem {

	private String display = null;// this is the display value for the suggestion, if different from suggestion

	private String description = null;

	private String caption = null;

	private int ranking = 1;

	private String suggestion = null;// this is the actual value for the suggestion, that will be used in the expression
    
    private ObjectType objectType = null;
    
    private ValueType valueType = null;
    
    public ExpressionSuggestionItem() {
	}

	public ExpressionSuggestionItem(String display, String description, String caption, String suggestion,
									ObjectType objectType, ValueType valueType) {
		this.display = display;
		this.description = description;
		this.caption = caption;
		this.suggestion = suggestion;
		this.objectType = objectType;
		this.valueType = valueType;
	}

	public ExpressionSuggestionItem(String display, String description, String caption, String suggestion,
									ObjectType objectType, ValueType valueType, int ranking) {
		this.display = display;
		this.description = description;
		this.caption = caption;
		this.suggestion = suggestion;
		this.objectType = objectType;
		this.valueType = valueType;
		this.ranking = ranking;
	}

    public ExpressionSuggestionItem(String display, String description, String suggestion,
			ObjectType objectType, ValueType valueType) {
    	this.display = display;
		this.description = description;
    	this.suggestion = suggestion;
    	this.objectType = objectType;
    	this.valueType = valueType;
	}

	public ExpressionSuggestionItem(String display, String suggestion,
									ObjectType objectType, ValueType valueType) {
		this.display = display;
		this.suggestion = suggestion;
		this.objectType = objectType;
		this.valueType = valueType;
	}

    public ExpressionSuggestionItem(String suggestion,
			ObjectType objectType, ValueType valueType) {
    	this.suggestion = suggestion;
    	this.objectType = objectType;
    	this.valueType = valueType;
	}


    
    public String getDisplay() {
		return display!=null?display:suggestion;
	}
    
    public void setDisplay(String display) {
		this.display = display;
	}

	public String getSuggestion() {
		return suggestion;
	}
    
    public void setSuggestion(String suggestion) {
		this.suggestion = suggestion;
	}
    
    public ObjectType getObjectType() {
		return objectType;
	}
    
    public void setObjectType(ObjectType objectType) {
		this.objectType = objectType;
	}
    
    public ValueType getValueType() {
		return valueType;
	}
    
    public void setValueType(ValueType valueType) {
		this.valueType = valueType;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getCaption() {
		return caption;
	}

	public void setCaption(String caption) {
		this.caption = caption;
	}

	public int getRanking() {
		return ranking;
	}

	public void setRanking(int ranking) {
		this.ranking = ranking;
	}


}
