package com.squid.kraken.v4.model;

/**
 * extend the Expression to allow adding a name (label)
 * note: this is done that way to be backward compatible with the ProjectAnalysisJob API
 * @author sergefantino
 *
 */
public class FacetExpression extends Expression {

	/**
	 * 
	 */
	private static final long serialVersionUID = -91190929258335779L;
	
	private String name = null;// allow to name the facet - this will be use in the analysis output to reference the facet

	public FacetExpression() {
		super();
	}

	public FacetExpression(String value) {
		super(value);
	}

	public FacetExpression(String value, String name) {
		super(value);
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
}
