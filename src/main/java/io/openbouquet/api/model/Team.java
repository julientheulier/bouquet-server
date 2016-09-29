package io.openbouquet.api.model;

import java.io.Serializable;

public interface Team extends Serializable{

	public String getId();

	public String getName();

	public String getServerUrl();
	
}