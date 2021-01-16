/*
 * Janssen Project software is available under the MIT License (2008). See http://opensource.org/licenses/MIT for full text.
 *
 * Copyright (c) 2020, Janssen Project
 */

package io.jans.orm.sql;

import java.util.Properties;

import org.apache.log4j.Logger;
import io.jans.orm.sql.impl.SqlEntryManager;
import io.jans.orm.sql.impl.SqlEntryManagerFactory;

/**
 * @author Yuriy Movchan
 * Date: 01/13/2017
 */
public class SqlSampleEntryManager {

    private static final Logger LOG = Logger.getLogger(SqlSampleEntryManager.class);

    private Properties getSampleConnectionProperties() {
        Properties connectionProperties = new Properties();

        connectionProperties.put("couchbase.servers", "test.jans.info");
        connectionProperties.put("couchbase.auth.userName", "admin");
        connectionProperties.put("couchbase.auth.userPassword", "secret");
//        connectionProperties.put("couchbase.buckets", "jans");
        connectionProperties.put("couchbase.buckets", "jans, jans_user, jans_token");

        connectionProperties.put("couchbase.bucket.default", "jans");
        connectionProperties.put("couchbase.bucket.jans_user.mapping", "people, groups");
        connectionProperties.put("couchbase.bucket.jans_token.mapping", "sessions");

        connectionProperties.put("couchbase.password.encryption.method", "CRYPT-SHA-256");

        return connectionProperties;
    }

    public SqlEntryManager createSqlEntryManager() {
        SqlEntryManagerFactory couchbaseEntryManagerFactory = new SqlEntryManagerFactory();
        couchbaseEntryManagerFactory.create();
        Properties connectionProperties = getSampleConnectionProperties();

        SqlEntryManager couchbaseEntryManager = couchbaseEntryManagerFactory.createEntryManager(connectionProperties);
        LOG.debug("Created SqlEntryManager: " + couchbaseEntryManager);

        return couchbaseEntryManager;
    }

}