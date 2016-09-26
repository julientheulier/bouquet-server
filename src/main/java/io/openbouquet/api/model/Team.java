package io.openbouquet.api.model;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Team implements Serializable {
	
	private String id;
	private String name;
	private String serverUrl;
	
	public Team() {
	}

	public String getId() {
		return id;
	}

	public void setTeamId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getServerUrl() {
		return serverUrl;
	}

	public void setServerUrl(String serverUrl) {
		this.serverUrl = serverUrl;
	}

	@Override
	public String toString() {
		return "Team [id=" + id + ", name=" + name + "]";
	}
	
}