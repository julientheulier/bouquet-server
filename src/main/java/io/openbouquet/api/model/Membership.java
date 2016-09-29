package io.openbouquet.api.model;

import java.io.Serializable;

public interface Membership extends Serializable {
	
	public User getUser();

	public Team getTeam();

	public String getRole();

}