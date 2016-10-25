package io.openbouquet.api.service;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import io.openbouquet.api.model.UserProfile;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface UserProfileService {

    @POST
    @Path("/users")
    public UserProfile createUser(@HeaderParam("Authorization") String authorization, UserProfile user);

    @GET
    @Path("/users")
    public UserProfile getUser(@HeaderParam("Authorization") String authorization);
}
