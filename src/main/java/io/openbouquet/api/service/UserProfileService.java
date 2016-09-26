package io.openbouquet.api.service;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import io.openbouquet.api.model.UserProfile;

@Path("/userprofile")
@Produces(MediaType.APPLICATION_JSON)
public interface UserProfileService {

	@GET
	public UserProfile get(@QueryParam("access_token") String token);
}
