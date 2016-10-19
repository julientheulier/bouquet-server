package io.openbouquet.api.service;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import io.openbouquet.api.model.Team;

@Produces(MediaType.APPLICATION_JSON)
public interface TeamService {

    @POST
    @Path("/teams")
    public Team createUser(@HeaderParam("Authorization") String authorization, Team team);

    @GET
    @Path("/teams/{teamId}")
    public Team getTeam(@HeaderParam("Authorization") String authorization, @PathParam("teamId") Long teamId);

    @PUT
    @Path("/teams/{teamId}")
    public Team updateTeam(@HeaderParam("Authorization") String authorization, @PathParam("teamId") Long teamId,
            Team team);

    @DELETE
    @Path("/teams/{teamId}")
    public Team deleteTeam(@HeaderParam("Authorization") String authorization, @PathParam("teamId") Long teamId);
}
