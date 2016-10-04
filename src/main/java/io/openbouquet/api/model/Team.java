package io.openbouquet.api.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.openbouquet.api.impl.TeamImpl;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(as = TeamImpl.class)
public interface Team extends Serializable{

	public String getId();

	public String getName();

	public String getServerUrl();
	
}