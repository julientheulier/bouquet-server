package com.squid.kraken.v4.api.core;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.squid.kraken.v4.api.core.test.BaseTest;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserPK;
import com.squid.kraken.v4.persistence.AppContext;

public class DomainServiceTest extends BaseTest {
    
    @Test
    public void testReadList0() {
    	// given
    	given("a Project with a Domain D with no metric");
        AppContext ctx = CustomerServiceTest.createTestUserContext();
        String customerId = ctx.getCustomerId();
        String projectId = ps.store(ctx, new Project(new ProjectPK(customerId, "test"), "test"))
                .getOid();
        Domain domain = ds.store(ctx, new Domain(new DomainPK(customerId, projectId, "A"),
                "test A", null));
    	
        // when
    	when("a User without WRITE right on the Project lists the domains");
    	User user = new User(new UserPK(customerId), "login", "password0");
    	List<String> groups = user.getGroups();
    	groups.add(CoreConstants.PRJ_DEFAULT_GROUP_GUEST + projectId);
    	user.setGroups(groups);
    	us.store(ctx, user);
    	AppContext userCtx = new AppContext.Builder(customerId, user).build();
    	List<Domain> readAll = ds.readAll(userCtx, projectId);
    	
    	// then
    	then("then D is not returned");
    	assertTrue(!readAll.contains(domain));
    	
        // when
    	when("a User with WRITE right on the Project lists the domains");
    	List<Domain> readAll2 = ds.readAll(ctx, projectId);
    	
    	// then
    	then("then D is returned");
    	assertTrue(readAll2.contains(domain));
    }
    
}
