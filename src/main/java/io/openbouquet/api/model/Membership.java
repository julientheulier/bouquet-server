package io.openbouquet.api.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.openbouquet.api.impl.MembershipImpl;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(as = MembershipImpl.class)
public interface Membership extends Serializable {

	public User getUser();

	public Team getTeam();

	public String getRole();

}