package io.openbouquet.api.service;

import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import io.openbouquet.api.model.Membership;
import io.openbouquet.api.model.User;

@Produces(MediaType.APPLICATION_JSON)
public interface MembershipService {

    @GET
    @Path("/teamMembership")
    public Membership get(@HeaderParam("Authorization") String authorization, @QueryParam("teamId") Long teamId);

    @GET
    @Path("/users/memberships")
    public List<Membership> getUserMemberships(@HeaderParam("Authorization") String authorization);

    @GET
    @Path("/team/{teamid}/members")
    public List<User> getTeamMembers(@HeaderParam("Authorization") String authorization,
            @PathParam("teamId") Long teamId);

    @POST
    @Path("/team/{teamid}/invite")
    public Membership inviteMember(@HeaderParam("Authorization") String authorization, @PathParam("teamId") Long teamId,
            @QueryParam("email") String email, String data);

    @POST
    @Path("/team/{teamid}/invite/{userid}")
    public Membership inviteMember(@HeaderParam("Authorization") String authorization, @PathParam("teamId") Long teamId,
            @PathParam("userId") Long userId, String data);

    @DELETE
    @Path("/memberships/{teamid}/member/{userId}")
    public String deleteMembership(@HeaderParam("Authorization") String authorization, @PathParam("teamId") Long teamid,
            @PathParam("userId") Long userId);

}
