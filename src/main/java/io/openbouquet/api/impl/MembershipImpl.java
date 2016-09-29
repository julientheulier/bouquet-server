package io.openbouquet.api.impl;

import io.openbouquet.api.model.Membership;
import io.openbouquet.api.model.Team;
import io.openbouquet.api.model.User;

@SuppressWarnings("serial")
public class MembershipImpl implements Membership {
	private User user;
	private Team team;
	private String role;

	public MembershipImpl() {
	}

	public User getUser() {
		return user;
	}

	public void setUser(User user) {
		this.user = user;
	}

	public Team getTeam() {
		return team;
	}

	public void setTeam(Team team) {
		this.team = team;
	}

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	@Override
	public String toString() {
		return "Membership [user=" + user + ", team=" + team + "]";
	}

}