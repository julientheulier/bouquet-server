package io.openbouquet.api.service;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.squid.enterprise.model.Invitation;

import io.openbouquet.api.model.Membership;
import io.openbouquet.api.model.User;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface MembershipService {

    @GET
    @Path("/teamMembership/{teamid}")
    public Membership get(@HeaderParam("Authorization") String authorization, @PathParam("teamid") String teamid);

    @GET
    @Path("/users/memberships")
    public List<Membership> getUserMemberships(@HeaderParam("Authorization") String authorization);

    @GET
    @Path("/team/{teamid}/members")
    public List<User> getTeamMembers(@HeaderParam("Authorization") String authorization,
            @PathParam("teamid") Long teamid);

    @POST
    @Path("/team/{teamid}/invite")
    public String inviteMember(@HeaderParam("Authorization") String authorization, @PathParam("teamid") String teamid,
            @QueryParam("email") String email, Invitation invitation);

    @DELETE
    @Path("/memberships/{teamid}/member/{userid}")
    public String deleteMembership(@HeaderParam("Authorization") String authorization,
            @PathParam("teamid") String teamid, @PathParam("userid") Long userid);

}
