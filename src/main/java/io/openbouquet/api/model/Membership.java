package io.openbouquet.api.model;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Membership implements Serializable {
	private User user;
	private Team team;
	private String role;

	public Membership() {
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