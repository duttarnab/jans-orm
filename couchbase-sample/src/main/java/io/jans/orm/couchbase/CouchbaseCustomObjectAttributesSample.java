/*
 * Janssen Project software is available under the Apache License (2004). See http://www.apache.org/licenses/ for full text.
 *
 * Copyright (c) 2020, Janssen Project
 */

package io.jans.orm.couchbase;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import io.jans.orm.couchbase.impl.CouchbaseEntryManager;
import io.jans.orm.couchbase.model.SimpleUser;
import io.jans.orm.couchbase.model.UserRole;
import io.jans.orm.couchbase.operation.impl.CouchbaseConnectionProvider;
import io.jans.orm.model.base.CustomObjectAttribute;
import io.jans.orm.search.filter.Filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yuriy Movchan Date: 09/24/2019
 */
public final class CouchbaseCustomObjectAttributesSample {

	private static final Logger LOG = LoggerFactory.getLogger(CouchbaseConnectionProvider.class);

	private CouchbaseCustomObjectAttributesSample() {
	}

	public static void main(String[] args) {
		// Prepare sample connection details
		CouchbaseEntryManagerSample couchbaseEntryManagerSample = new CouchbaseEntryManagerSample();

		// Create Couchbase entry manager
		CouchbaseEntryManager couchbaseEntryManager = couchbaseEntryManagerSample.createCouchbaseEntryManager();

		// Add dummy user
		SimpleUser newUser = new SimpleUser();
		newUser.setDn(String.format("inum=%s,ou=people,o=jans", System.currentTimeMillis()));
		newUser.setUserId("sample_user_" + System.currentTimeMillis());
		newUser.setUserPassword("test");
		newUser.getCustomAttributes().add(new CustomObjectAttribute("streetAddress", Arrays.asList("London", "Texas", "Kiev")));
		newUser.getCustomAttributes().add(new CustomObjectAttribute("test", "test_value"));
		newUser.getCustomAttributes().add(new CustomObjectAttribute("birthdate", new Date()));
		newUser.getCustomAttributes().add(new CustomObjectAttribute("enabled", false));
		newUser.getCustomAttributes().add(new CustomObjectAttribute("age", 18));

		newUser.setUserRole(UserRole.ADMIN);
		newUser.setNotes(Arrays.asList("note 1", "note 2", "note 3"));

		couchbaseEntryManager.persist(newUser);

		LOG.info("Added User '{}' with uid '{}' and key '{}'", newUser, newUser.getUserId(), newUser.getDn());

		// Find added dummy user
		SimpleUser foundUser = couchbaseEntryManager.find(SimpleUser.class, newUser.getDn());
		LOG.info("Found User '{}' with uid '{}' and key '{}'", foundUser, foundUser.getUserId(), foundUser.getDn());

		LOG.info("Custom attributes '{}'", foundUser.getCustomAttributes());
		for (CustomObjectAttribute customAttribute : foundUser.getCustomAttributes()) {
			if (customAttribute.getValue() instanceof Date) {
				LOG.info("Found date custom attribute '{}' with value '{}'", customAttribute.getName(), customAttribute.getValue());
			} else if (customAttribute.getValue() instanceof Integer) {
				LOG.info("Found integer custom attribute '{}' with value '{}'", customAttribute.getName(), customAttribute.getValue());
			} else if (customAttribute.getValue() instanceof Boolean) {
				LOG.info("Found boolean custom attribute '{}' with value '{}'", customAttribute.getName(), customAttribute.getValue());
			}

		}

		// Find added dummy user by numeric attribute
		Filter filter = Filter.createGreaterOrEqualFilter("age", 16);
		List<SimpleUser> foundUsers = couchbaseEntryManager.findEntries("ou=people,o=jans", SimpleUser.class, filter);
		if (foundUsers.size() > 0) {
			foundUser = foundUsers.get(0);
			LOG.info("Found User '{}' by filter '{}' with uid '{}' and key '{}'", foundUser, filter, foundUser, foundUser);
		} else {
			LOG.error("Can't find User by filter '{}'", filter);
		}
	}

}
