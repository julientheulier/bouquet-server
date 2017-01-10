package com.squid.kraken.v4.api.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.squid.kraken.v4.api.core.test.BaseTest;
import com.squid.kraken.v4.api.core.test.LogMetaModelObserver;
import com.squid.kraken.v4.api.core.test.MockMetaModelObserver;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DataStoreEventBus;

public class ProjectServiceTest extends BaseTest {
	@Test
	public void testCreateFormJson() {
		given("a Customer");
		AppContext ctx = CustomerServiceTest.createTestUserContext();
		String customerId = ctx.getCustomerId();

		when("adding a new Project to the Customer from a Json File");
		DataStoreEventBus.getInstance().subscribe(
				LogMetaModelObserver.getInstance());
		MockMetaModelObserver observer = DataStoreEventBus.getInstance().subscribe(new MockMetaModelObserver());
		ObjectMapper mapper = new ObjectMapper();
		Project project = null;
		try {
			byte[] projectJSON = ByteStreams.toByteArray(this.getClass()
					.getClassLoader().getResourceAsStream("musicbrainz.json"));
			project = mapper.readValue(projectJSON, Project.class);
		} catch (Exception e) {
			e.printStackTrace();
		}

		project = ps.store(ctx, project);
		DataStoreEventBus.getInstance().unSubscribe(
				LogMetaModelObserver.getInstance());
		DataStoreEventBus.getInstance().unSubscribe(observer);
		then("number of events should be 51 : ");
		assertTrue(observer.getEvents().size() == 51);
		
		ProjectPK projectId = project.getId();

		then("the project should be created");
		Project read = ps.read(ctx, projectId);
		assertEquals(projectId, read.getId());

		then("the project should be added to the Customer");
		assertEquals(projectId, ps.readAll(ctx).iterator().next().getId());

		then("the project userRole should be OWNER");
		assertTrue(read.getUserRole() == Role.OWNER);
		
		then("Then the domains should be created");
		List<Domain> readDomains = ds.readAll(ctx, projectId.getProjectId());
		assertTrue(readDomains.size()>0);
	}
}
